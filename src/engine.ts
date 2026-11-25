import { randomUUID } from 'node:crypto';
import type pg from 'pg';
import { withTenantLock, type Tx } from './db.ts';

/*
 * The routing engine: every state transition of a call or an agent, and the routing rule.
 *
 * Agent states            Call states
 *   offline                 waiting     in the queue
 *   away                    offered     ringing at one agent, with a deadline
 *   available               connected   answered, in progress
 *   offered  (ringing)      completed   answered, then ended      (final)
 *   busy     (on a call)    abandoned   caller hung up unanswered (final)
 *
 * Every write runs inside `change`, which holds the tenant's lock. So inside a transition the
 * code can read a row, decide, and write, without another request changing the row in between.
 */

export type AgentState = 'offline' | 'away' | 'available' | 'offered' | 'busy';
export type CallState = 'waiting' | 'offered' | 'connected' | 'completed' | 'abandoned';
export type DeclineReason = 'rejected' | 'timeout' | 'agent_away' | 'agent_logout';

export interface CallView {
  id: string;
  tenantId: string;
  skill: string;
  priority: number;
  state: CallState;
  /** How the call ended up: 'answered' once an agent accepted, 'abandoned' if the caller left first. */
  outcome: 'answered' | 'abandoned' | null;
  agentId: string | null;
  enqueuedAt: Date;
  offerExpiresAt: Date | null;
  answeredAt: Date | null;
  endedAt: Date | null;
}

export interface AgentView {
  id: string;
  tenantId: string;
  skills: string[];
  state: AgentState;
  idleSince: Date | null;
  /** The call ringing at or connected to this agent. A list so a client can check it never has two. */
  calls: { id: string; state: CallState; skill: string; priority: number; offerExpiresAt: Date | null }[];
}

/** The request was understood but is not possible: unknown id (404) or wrong state (409). */
export class RoutingError extends Error {
  readonly status: 404 | 409;
  readonly code: string;
  constructor(status: 404 | 409, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

const notFound = (what: string, id: string) => new RoutingError(404, 'not_found', `${what} ${id} not found`);
const conflict = (code: string, message: string) => new RoutingError(409, code, message);

interface AgentRow {
  id: string;
  state: AgentState;
}

interface CallRow {
  id: string;
  state: CallState;
  agent_id: string | null;
  /** True when the offer deadline has passed, according to the database clock. */
  offer_expired: boolean | null;
}

type Queryable = pg.Pool | Tx;

export interface EngineOptions {
  offerTimeoutMs: number;
  /** Name of this service instance, stamped on every event. */
  instance: string;
}

export class Engine {
  readonly pool: pg.Pool;
  readonly offerTimeoutMs: number;
  readonly instance: string;

  constructor(pool: pg.Pool, options: EngineOptions) {
    this.pool = pool;
    this.offerTimeoutMs = options.offerTimeoutMs;
    this.instance = options.instance;
  }

  // ───────────────────────────── Agent API ─────────────────────────────

  /** Log in. The first login creates the agent; a repeated login only updates the skills. */
  login(tenant: string, agentId: string, skills: string[]): Promise<AgentView> {
    return this.change(
      tenant,
      async (tx) => {
        await tx.query(
          `INSERT INTO agents (tenant_id, id, skills, state) VALUES ($1, $2, $3, 'away')
           ON CONFLICT (tenant_id, id) DO UPDATE
             SET skills = EXCLUDED.skills,
                 state  = CASE WHEN agents.state = 'offline' THEN 'away' ELSE agents.state END`,
          [tenant, agentId, skills],
        );
      },
      (tx) => this.agentView(tx, tenant, agentId),
    );
  }

  logout(tenant: string, agentId: string): Promise<AgentView> {
    return this.change(
      tenant,
      async (tx) => {
        const agent = await this.loadAgent(tx, tenant, agentId);
        if (agent.state === 'offline') return;
        if (agent.state === 'busy') throw conflict('on_call', 'End the call before logging out');
        if (agent.state === 'offered') await this.declineOfferOf(tx, tenant, agentId, 'agent_logout');
        await this.setAgentState(tx, tenant, agentId, 'offline');
      },
      (tx) => this.agentView(tx, tenant, agentId),
    );
  }

  setAvailable(tenant: string, agentId: string): Promise<AgentView> {
    return this.change(
      tenant,
      async (tx) => {
        const agent = await this.loadAgent(tx, tenant, agentId);
        if (agent.state === 'available') return; // already available: the idle clock keeps running
        if (agent.state === 'offline') throw conflict('not_logged_in', 'Log in first');
        if (agent.state !== 'away') throw conflict('has_call', `Agent is ${agent.state}`);
        await this.setAgentState(tx, tenant, agentId, 'available');
      },
      (tx) => this.agentView(tx, tenant, agentId),
    );
  }

  setAway(tenant: string, agentId: string): Promise<AgentView> {
    return this.change(
      tenant,
      async (tx) => {
        const agent = await this.loadAgent(tx, tenant, agentId);
        if (agent.state === 'away') return;
        if (agent.state === 'offline') throw conflict('not_logged_in', 'Log in first');
        if (agent.state === 'busy') throw conflict('on_call', 'End the call before going away');
        // Going away while a call is ringing gives the call back to the queue.
        if (agent.state === 'offered') await this.declineOfferOf(tx, tenant, agentId, 'agent_away');
        await this.setAgentState(tx, tenant, agentId, 'away');
      },
      (tx) => this.agentView(tx, tenant, agentId),
    );
  }

  /**
   * Accept an offered call. The request names the call, so a late or repeated request can
   * never accept a different call that was offered to the agent in the meantime.
   */
  accept(tenant: string, agentId: string, callId: string): Promise<AgentView> {
    return this.change(
      tenant,
      async (tx) => {
        const call = await this.loadCall(tx, tenant, callId);
        const mine = call.agent_id === agentId;
        if (mine && call.state === 'connected') return; // repeated accept
        if (!mine || call.state !== 'offered') {
          throw conflict('not_offered', 'This call is not offered to this agent');
        }
        // Accept against timeout: the database clock decides, not the order in which the
        // sweeper happens to run. After the deadline the offer is dead even if still unswept.
        if (call.offer_expired) throw conflict('offer_expired', 'The offer has expired');

        await tx.query(
          `UPDATE calls SET state = 'connected', offer_expires_at = NULL, answered_at = clock_timestamp()
           WHERE tenant_id = $1 AND id = $2`,
          [tenant, callId],
        );
        await this.setAgentState(tx, tenant, agentId, 'busy');
        await this.log(tx, tenant, 'accepted', callId, agentId);
      },
      (tx) => this.agentView(tx, tenant, agentId),
    );
  }

  /** Reject an offered call. Treated like a missed call: the agent goes away, the call is re-offered. */
  reject(tenant: string, agentId: string, callId: string): Promise<AgentView> {
    return this.change(
      tenant,
      async (tx) => {
        const call = await this.loadCall(tx, tenant, callId);
        if (call.agent_id !== agentId || call.state !== 'offered') {
          throw conflict('not_offered', 'This call is not offered to this agent');
        }
        await this.declineOffer(tx, tenant, callId, agentId, 'rejected');
        await this.setAgentState(tx, tenant, agentId, 'away');
      },
      (tx) => this.agentView(tx, tenant, agentId),
    );
  }

  /** The agent ends a connected call and becomes available again. */
  endCall(tenant: string, agentId: string, callId: string): Promise<AgentView> {
    return this.change(
      tenant,
      async (tx) => {
        const call = await this.loadCall(tx, tenant, callId);
        const mine = call.agent_id === agentId;
        if (mine && call.state === 'completed') return; // repeated end, or the caller hung up first
        if (!mine || call.state !== 'connected') {
          throw conflict('not_connected', 'This call is not connected to this agent');
        }
        await this.complete(tx, tenant, callId, agentId);
      },
      (tx) => this.agentView(tx, tenant, agentId),
    );
  }

  getAgent(tenant: string, agentId: string): Promise<AgentView> {
    return this.agentView(this.pool, tenant, agentId);
  }

  // ───────────────────────────── Call API ──────────────────────────────

  /**
   * A new call joins the queue. The caller may supply the id; sending the same id again
   * returns the existing call, so the upstream system can retry safely.
   */
  async newCall(
    tenant: string,
    input: { id?: string; skill: string; priority?: number },
  ): Promise<{ created: boolean; call: CallView }> {
    const callId = input.id ?? randomUUID();
    let created = false;
    const call = await this.change(
      tenant,
      async (tx) => {
        const inserted = await tx.query(
          `INSERT INTO calls (tenant_id, id, skill, priority) VALUES ($1, $2, $3, $4)
           ON CONFLICT (tenant_id, id) DO NOTHING`,
          [tenant, callId, input.skill, input.priority ?? 0],
        );
        created = inserted.rowCount === 1;
        if (created) await this.log(tx, tenant, 'queued', callId, null);
      },
      (tx) => this.callView(tx, tenant, callId),
    );
    return { created, call };
  }

  /** The caller hangs up. Allowed at any moment; what it means depends on the call's state. */
  hangup(tenant: string, callId: string): Promise<CallView> {
    return this.change(
      tenant,
      async (tx) => {
        const call = await this.loadCall(tx, tenant, callId);
        switch (call.state) {
          case 'waiting':
          case 'offered':
            await tx.query(
              `UPDATE calls SET state = 'abandoned', agent_id = NULL, offer_expires_at = NULL,
                                ended_at = clock_timestamp()
               WHERE tenant_id = $1 AND id = $2`,
              [tenant, callId],
            );
            // The agent did nothing wrong: back to available, keeping their place in the idle order.
            if (call.agent_id) {
              await this.setAgentState(tx, tenant, call.agent_id, 'available', { restartIdleClock: false });
            }
            await this.log(tx, tenant, 'abandoned', callId, call.agent_id);
            return;
          case 'connected':
            await this.complete(tx, tenant, callId, call.agent_id!);
            return;
          case 'completed':
          case 'abandoned':
            return; // already over: a repeated hangup changes nothing
        }
      },
      (tx) => this.callView(tx, tenant, callId),
    );
  }

  getCall(tenant: string, callId: string): Promise<CallView> {
    return this.callView(this.pool, tenant, callId);
  }

  // ───────────────────────────── Timeouts ──────────────────────────────

  /**
   * Time out every offer whose deadline has passed: the agent goes away and the call is
   * offered to the next agent. Called by the sweeper on every instance. If both instances
   * pick up the same offer, the second one finds it already handled and does nothing.
   */
  async expireOffers(): Promise<number> {
    const due = await this.pool.query<{ tenant_id: string; id: string }>(
      `SELECT tenant_id, id FROM calls
       WHERE state = 'offered' AND offer_expires_at <= clock_timestamp()
       ORDER BY offer_expires_at LIMIT 200`,
    );
    let expired = 0;
    for (const { tenant_id: tenant, id: callId } of due.rows) {
      await this.change(
        tenant,
        async (tx) => {
          // Check again under the lock: the list above was read without it.
          const call = await this.loadCall(tx, tenant, callId);
          if (call.state !== 'offered' || !call.offer_expired) return;
          await this.declineOffer(tx, tenant, callId, call.agent_id!, 'timeout');
          await this.setAgentState(tx, tenant, call.agent_id!, 'away');
          expired++;
        },
        async () => {},
      );
    }
    return expired;
  }

  // ───────────────────────────── Routing ───────────────────────────────

  /**
   * The routing rule. Take the waiting call with the highest priority (longest wait among
   * equals) that some available agent can answer, and offer it to the agent with the right
   * skill who has been idle the longest. Repeat until no call can be matched.
   *
   * A call with no skilled agent available is skipped, so it does not block the calls behind it.
   */
  private async route(tx: Tx, tenant: string): Promise<void> {
    for (;;) {
      const match = await tx.query<{ call_id: string; agent_id: string }>(
        `SELECT c.id AS call_id, a.id AS agent_id
         FROM calls c
         CROSS JOIN LATERAL (
             SELECT id FROM agents
             WHERE tenant_id = c.tenant_id AND state = 'available' AND c.skill = ANY (skills)
             ORDER BY idle_since, id                    -- idle the longest
             LIMIT 1
         ) a
         WHERE c.tenant_id = $1 AND c.state = 'waiting'
         ORDER BY c.priority DESC, c.enqueued_at, c.id  -- highest priority, then longest wait
         LIMIT 1`,
        [tenant],
      );
      if (match.rows.length === 0) return;
      const { call_id: callId, agent_id: agentId } = match.rows[0]!;

      await tx.query(
        `UPDATE calls SET state = 'offered', agent_id = $3,
                          offer_expires_at = clock_timestamp() + $4::int * interval '1 millisecond'
         WHERE tenant_id = $1 AND id = $2`,
        [tenant, callId, agentId, this.offerTimeoutMs],
      );
      await this.setAgentState(tx, tenant, agentId, 'offered');
      await this.log(tx, tenant, 'offered', callId, agentId);
    }
  }

  // ───────────────────────────── Internals ─────────────────────────────

  /**
   * The only way to write. Locks the tenant, applies one transition, routes, then builds the
   * response. Routing is part of the same transaction as the event that made it possible, so
   * no committed state ever has a waiting call next to an idle agent who could take it, and
   * there is no separate "wake the router" step that could get lost.
   */
  private change<T>(
    tenant: string,
    apply: (tx: Tx) => Promise<void>,
    respond: (tx: Tx) => Promise<T>,
  ): Promise<T> {
    return withTenantLock(this.pool, tenant, async (tx) => {
      await apply(tx);
      await this.route(tx, tenant);
      return respond(tx);
    });
  }

  /** offered → waiting. The call keeps its enqueued_at, so it is back at the front of its priority. */
  private async declineOffer(
    tx: Tx,
    tenant: string,
    callId: string,
    agentId: string,
    reason: DeclineReason,
  ): Promise<void> {
    await tx.query(
      `UPDATE calls SET state = 'waiting', agent_id = NULL, offer_expires_at = NULL
       WHERE tenant_id = $1 AND id = $2`,
      [tenant, callId],
    );
    await this.log(tx, tenant, 'declined', callId, agentId, reason);
  }

  /** Decline whatever call is currently ringing at this agent. */
  private async declineOfferOf(tx: Tx, tenant: string, agentId: string, reason: DeclineReason): Promise<void> {
    const offered = await tx.query<{ id: string }>(
      `SELECT id FROM calls WHERE tenant_id = $1 AND agent_id = $2 AND state = 'offered'`,
      [tenant, agentId],
    );
    for (const { id } of offered.rows) await this.declineOffer(tx, tenant, id, agentId, reason);
  }

  /** connected → completed. The agent becomes available and their idle clock restarts. */
  private async complete(tx: Tx, tenant: string, callId: string, agentId: string): Promise<void> {
    await tx.query(
      `UPDATE calls SET state = 'completed', ended_at = clock_timestamp() WHERE tenant_id = $1 AND id = $2`,
      [tenant, callId],
    );
    await this.setAgentState(tx, tenant, agentId, 'available');
    await this.log(tx, tenant, 'completed', callId, agentId);
  }

  /** Set an agent's state. Becoming available restarts the idle clock unless told otherwise. */
  private async setAgentState(
    tx: Tx,
    tenant: string,
    agentId: string,
    state: AgentState,
    { restartIdleClock = state === 'available' } = {},
  ): Promise<void> {
    await tx.query(
      `UPDATE agents SET state = $3, idle_since = CASE WHEN $4 THEN clock_timestamp() ELSE idle_since END
       WHERE tenant_id = $1 AND id = $2`,
      [tenant, agentId, state, restartIdleClock],
    );
  }

  private async log(
    tx: Tx,
    tenant: string,
    type: 'queued' | 'offered' | 'accepted' | 'declined' | 'abandoned' | 'completed',
    callId: string,
    agentId: string | null,
    reason: DeclineReason | null = null,
  ): Promise<void> {
    await tx.query(
      `INSERT INTO events (tenant_id, type, call_id, agent_id, reason, instance) VALUES ($1, $2, $3, $4, $5, $6)`,
      [tenant, type, callId, agentId, reason, this.instance],
    );
  }

  private async loadAgent(tx: Tx, tenant: string, agentId: string): Promise<AgentRow> {
    const { rows } = await tx.query<AgentRow>(`SELECT id, state FROM agents WHERE tenant_id = $1 AND id = $2`, [
      tenant,
      agentId,
    ]);
    if (!rows[0]) throw notFound('agent', agentId);
    return rows[0];
  }

  private async loadCall(tx: Tx, tenant: string, callId: string): Promise<CallRow> {
    const { rows } = await tx.query<CallRow>(
      `SELECT id, state, agent_id, offer_expires_at <= clock_timestamp() AS offer_expired
       FROM calls WHERE tenant_id = $1 AND id = $2`,
      [tenant, callId],
    );
    if (!rows[0]) throw notFound('call', callId);
    return rows[0];
  }

  private async callView(db: Queryable, tenant: string, callId: string): Promise<CallView> {
    const { rows } = await db.query(`SELECT * FROM calls WHERE tenant_id = $1 AND id = $2`, [tenant, callId]);
    const c = rows[0];
    if (!c) throw notFound('call', callId);
    return {
      id: c.id,
      tenantId: c.tenant_id,
      skill: c.skill,
      priority: c.priority,
      state: c.state,
      outcome: c.state === 'abandoned' ? 'abandoned' : c.answered_at ? 'answered' : null,
      agentId: c.agent_id,
      enqueuedAt: c.enqueued_at,
      offerExpiresAt: c.offer_expires_at,
      answeredAt: c.answered_at,
      endedAt: c.ended_at,
    };
  }

  /** One statement, so the agent and their live call come from the same snapshot. */
  private async agentView(db: Queryable, tenant: string, agentId: string): Promise<AgentView> {
    const { rows } = await db.query(
      `SELECT a.id, a.skills, a.state, a.idle_since,
              c.id AS call_id, c.state AS call_state, c.skill AS call_skill,
              c.priority AS call_priority, c.offer_expires_at
       FROM agents a
       LEFT JOIN calls c ON c.tenant_id = a.tenant_id AND c.agent_id = a.id
                        AND c.state IN ('offered', 'connected')
       WHERE a.tenant_id = $1 AND a.id = $2`,
      [tenant, agentId],
    );
    const a = rows[0];
    if (!a) throw notFound('agent', agentId);
    return {
      id: a.id,
      tenantId: tenant,
      skills: a.skills,
      state: a.state,
      idleSince: a.idle_since,
      calls: rows
        .filter((r) => r.call_id !== null)
        .map((r) => ({
          id: r.call_id,
          state: r.call_state,
          skill: r.call_skill,
          priority: r.call_priority,
          offerExpiresAt: r.offer_expires_at,
        })),
    };
  }
}
