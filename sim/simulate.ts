import { setTimeout as sleep } from 'node:timers/promises';
import pg from 'pg';

/*
 * Simulation: 50 agents in two tenants and a burst of calls, sent to both service instances.
 * Some agents do not answer, some reject, some callers hang up.
 *
 * At the end it proves three things, each from more than one source:
 *   1. no agent had two calls at once
 *   2. no call was offered to two agents at once
 *   3. every call ended answered or abandoned
 *
 * Sources: a replay of the event log, snapshots of the live tables taken during the run,
 * what the simulated agents saw through the API, and the final state of the tables.
 */

const URLS = (process.env.SIM_URLS ?? 'http://localhost:8081,http://localhost:8082').split(',');
const DATABASE_URL = process.env.DATABASE_URL ?? 'postgres://routing:routing@localhost:55432/routing';
const CALLS = Number(process.env.SIM_CALLS ?? 300);
const SEED = Number(process.env.SIM_SEED ?? Date.now() % 1_000_000);
const DEADLINE_MS = Number(process.env.SIM_DEADLINE_MS ?? 240_000);

// A seeded random generator, so a run can be repeated with SIM_SEED.
function mulberry32(seed: number): () => number {
  return () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const random = mulberry32(SEED);
const between = (min: number, max: number) => min + random() * (max - min);
const pick = <T>(items: T[]): T => items[Math.floor(random() * items.length)]!;

// Tenant names are unique per run, so repeated runs on the same database do not mix.
const run = Date.now().toString(36);
const TENANTS = [
  { id: `acme-${run}`, agents: 30, skills: ['arabic', 'english', 'billing', 'support'] },
  { id: `globex-${run}`, agents: 20, skills: ['arabic', 'english', 'sales'] },
];
const tenantIds = TENANTS.map((t) => t.id);

// ───────────────────────────── HTTP ─────────────────────────────

let serverErrors = 0;

/**
 * Send a request to a random instance. If that instance is unreachable, try again, most
 * likely on the other one. Safe because every request can be repeated without harm.
 */
async function http(method: 'GET' | 'POST', path: string, body?: unknown): Promise<{ status: number; body: any }> {
  for (let attempt = 1; ; attempt++) {
    const base = URLS[Math.floor(Math.random() * URLS.length)]!;
    try {
      const res = await fetch(base + path, {
        method,
        headers: body === undefined ? undefined : { 'content-type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      if (res.status >= 500) serverErrors++;
      return { status: res.status, body: await res.json() };
    } catch (err) {
      if (attempt >= 50) throw err;
      await sleep(200);
    }
  }
}

// ───────────────────────────── What the clients saw ─────────────────────────────

/** For each call, the agents whose accept succeeded. More than one agent means a double connect. */
const acceptedBy = new Map<string, Set<string>>();
/** Times an agent's own status showed more than one live call. */
let agentSawTwoCalls = 0;

// ───────────────────────────── Simulated agent ─────────────────────────────

let agentsShouldStop = false;

async function runAgent(tenant: string, id: string, skills: string[], flaky: boolean): Promise<void> {
  const base = `/v1/tenants/${tenant}/agents/${id}`;
  await http('POST', `${base}/login`, { skills });
  await http('POST', `${base}/available`);

  // The agent acts only on what the server reports, never on its own memory of past requests.
  // So a lost response cannot leave it stuck: the next poll shows the real state.
  const plans = new Map<string, { action: 'accept' | 'reject' | 'ignore'; at: number }>();
  const talkUntil = new Map<string, number>();
  let backAt = 0;

  while (!agentsShouldStop) {
    const { status, body: me } = await http('GET', base);
    const now = Date.now();
    if (status === 200) {
      if (me.calls.length > 1) agentSawTwoCalls++;
      const call = me.calls[0];

      if (me.state === 'offered' && call) {
        // One plan per offer: answer after some thinking time, reject, or let it ring out.
        const offer = `${call.id}@${call.offerExpiresAt}`;
        if (!plans.has(offer)) {
          const roll = random();
          const [ignore, reject] = flaky ? [0.5, 0.6] : [0.04, 0.1];
          const action = roll < ignore ? 'ignore' : roll < reject ? 'reject' : 'accept';
          plans.set(offer, { action, at: now + between(100, 1200) });
        }
        const plan = plans.get(offer)!;
        if (plan.action !== 'ignore' && now >= plan.at) {
          const res = await http('POST', `${base}/${plan.action}`, { callId: call.id });
          if (plan.action === 'accept' && res.status === 200) {
            const key = `${tenant}/${call.id}`;
            acceptedBy.set(key, (acceptedBy.get(key) ?? new Set()).add(id));
          }
        }
      } else if (me.state === 'busy' && call) {
        if (!talkUntil.has(call.id)) talkUntil.set(call.id, now + between(500, 2500));
        else if (now >= talkUntil.get(call.id)!) await http('POST', `${base}/end`, { callId: call.id });
      } else if (me.state === 'away') {
        // Set away after a missed or rejected call. Come back after a short break.
        if (backAt === 0) backAt = now + between(1500, 4000);
        else if (now >= backAt) {
          backAt = 0;
          await http('POST', `${base}/available`);
        }
      }
    }
    await sleep(between(150, 250));
  }
}

// ───────────────────────────── Simulated caller ─────────────────────────────

async function runCaller(tenant: string, id: string, skill: string, priority: number): Promise<void> {
  await sleep(between(0, 3000)); // the burst: all calls arrive within three seconds
  const base = `/v1/tenants/${tenant}/calls`;
  await http('POST', base, { id, skill, priority });

  // A third of the callers are impatient. Some of them hang up while the call is ringing at an agent.
  const patience = random() < 0.33 ? between(1000, 14_000) : 120_000;
  const start = Date.now();
  for (;;) {
    const { status, body: call } = await http('GET', `${base}/${id}`);
    if (status === 200) {
      if (call.state === 'completed' || call.state === 'abandoned') return;
      if (call.state !== 'connected' && Date.now() - start >= patience) {
        await http('POST', `${base}/${id}/hangup`);
      }
    }
    await sleep(between(300, 500));
  }
}

// ───────────────────────────── Snapshots of the live tables ─────────────────────────────

/**
 * Three things that must never be true, counted in one statement, so in one snapshot:
 *   double_booked  an agent with more than one live call
 *   disagree       an agent whose state does not match their call (ringing or busy with no
 *                  call behind it, or the reverse). This is how "one call, two agents" would show.
 *   starved        a waiting call while an available agent with the skill exists
 */
const SNAPSHOT_SQL = `
  SELECT
    (SELECT count(*) FROM (SELECT 1 FROM calls
                           WHERE tenant_id = ANY ($1) AND state IN ('offered', 'connected')
                           GROUP BY tenant_id, agent_id HAVING count(*) > 1) x)::int AS double_booked,
    (SELECT count(*) FROM agents a
       LEFT JOIN calls c ON c.tenant_id = a.tenant_id AND c.agent_id = a.id
                        AND c.state IN ('offered', 'connected')
      WHERE a.tenant_id = ANY ($1)
        AND ((a.state = 'offered') <> (c.state IS NOT DISTINCT FROM 'offered')
          OR (a.state = 'busy')    <> (c.state IS NOT DISTINCT FROM 'connected')))::int AS disagree,
    (SELECT count(*) FROM calls c
      WHERE c.tenant_id = ANY ($1) AND c.state = 'waiting'
        AND EXISTS (SELECT 1 FROM agents a WHERE a.tenant_id = c.tenant_id
                      AND a.state = 'available' AND c.skill = ANY (a.skills)))::int AS starved,
    (SELECT count(*) FROM calls WHERE tenant_id = ANY ($1) AND state = 'waiting')::int   AS waiting,
    (SELECT count(*) FROM calls WHERE tenant_id = ANY ($1) AND state = 'offered')::int   AS offered,
    (SELECT count(*) FROM calls WHERE tenant_id = ANY ($1) AND state = 'connected')::int AS connected,
    (SELECT count(*) FROM calls WHERE tenant_id = ANY ($1)
                                  AND state IN ('completed', 'abandoned'))::int          AS finished`;

const snapshots = { taken: 0, double_booked: 0, disagree: 0, starved: 0 };
let snapshotsShouldStop = false;

async function takeSnapshots(db: pg.Pool, startedAt: number): Promise<void> {
  let lastPrinted = 0;
  while (!snapshotsShouldStop) {
    const { rows } = await db.query(SNAPSHOT_SQL, [tenantIds]);
    const s = rows[0];
    snapshots.taken++;
    if (s.double_booked > 0) snapshots.double_booked++;
    if (s.disagree > 0) snapshots.disagree++;
    if (s.starved > 0) snapshots.starved++;
    if (Date.now() - lastPrinted >= 5000) {
      lastPrinted = Date.now();
      const t = ((Date.now() - startedAt) / 1000).toFixed(0).padStart(3);
      console.log(`  t=${t}s  waiting ${s.waiting}  ringing ${s.offered}  connected ${s.connected}  finished ${s.finished}`);
    }
    await sleep(200);
  }
}

// ───────────────────────────── Replay of the event log ─────────────────────────────

interface EventRow {
  id: string;
  tenant_id: string;
  type: 'queued' | 'offered' | 'accepted' | 'declined' | 'abandoned' | 'completed';
  call_id: string;
  agent_id: string | null;
  reason: string | null;
  instance: string;
}

/**
 * Walk the log in order and rebuild who held which call. The log was written inside the same
 * transactions as the decisions, so within a tenant its order is the order things happened.
 */
function replay(events: EventRow[]) {
  const callState = new Map<string, string>();
  const callHolder = new Map<string, string>(); // call  → agent it is ringing at or connected to
  const agentHolds = new Map<string, string>(); // agent → call they hold
  const problems = { agentWithTwoCalls: [] as string[], callWithTwoAgents: [] as string[], illegal: [] as string[] };
  const count: Record<string, number> = {};
  const offersBy: Record<string, number> = {};

  const release = (call: string) => {
    const agent = callHolder.get(call);
    if (agent) agentHolds.delete(agent);
    callHolder.delete(call);
  };

  for (const e of events) {
    const call = `${e.tenant_id}/${e.call_id}`;
    const agent = e.agent_id ? `${e.tenant_id}/${e.agent_id}` : null;
    const state = callState.get(call);
    const label = e.type === 'declined' ? `declined:${e.reason}` : e.type;
    count[label] = (count[label] ?? 0) + 1;
    const illegal = () => problems.illegal.push(`#${e.id} ${e.type} on ${call} while ${state}`);

    switch (e.type) {
      case 'queued':
        if (state !== undefined) illegal();
        callState.set(call, 'waiting');
        break;
      case 'offered':
        offersBy[e.instance] = (offersBy[e.instance] ?? 0) + 1;
        if (state !== 'waiting') illegal();
        if (callHolder.has(call)) problems.callWithTwoAgents.push(`#${e.id} ${call} to ${agent}, already at ${callHolder.get(call)}`);
        if (agentHolds.has(agent!)) problems.agentWithTwoCalls.push(`#${e.id} ${agent} got ${call}, already holds ${agentHolds.get(agent!)}`);
        callState.set(call, 'offered');
        callHolder.set(call, agent!);
        agentHolds.set(agent!, call);
        break;
      case 'accepted':
        if (state !== 'offered' || callHolder.get(call) !== agent) illegal();
        callState.set(call, 'connected');
        break;
      case 'declined':
        if (state !== 'offered' || callHolder.get(call) !== agent) illegal();
        release(call);
        callState.set(call, 'waiting');
        break;
      case 'abandoned':
        if (state !== 'waiting' && state !== 'offered') illegal();
        if (state === 'offered') count['abandoned while ringing'] = (count['abandoned while ringing'] ?? 0) + 1;
        release(call);
        callState.set(call, 'abandoned');
        break;
      case 'completed':
        if (state !== 'connected' || callHolder.get(call) !== agent) illegal();
        release(call);
        callState.set(call, 'completed');
        break;
    }
  }

  const unfinished = [...callState].filter(([, s]) => s !== 'completed' && s !== 'abandoned').map(([c]) => c);
  return { problems, count, offersBy, unfinished, calls: callState.size };
}

// ───────────────────────────── Main ─────────────────────────────

async function waitForService(): Promise<void> {
  for (const url of URLS) {
    for (let attempt = 1; ; attempt++) {
      try {
        if ((await fetch(`${url}/healthz`)).ok) break;
      } catch {}
      if (attempt >= 120) throw new Error(`${url} did not become healthy`);
      await sleep(500);
    }
  }
}

async function main(): Promise<number> {
  await waitForService();
  const db = new pg.Pool({ connectionString: DATABASE_URL, max: 2 });
  const startedAt = Date.now();
  console.log(`Simulation ${run}: 50 agents, ${CALLS} calls, instances ${URLS.join(' ')}, seed ${SEED}\n`);

  // Agents: one in five is flaky and often lets a call ring out.
  const agents: Promise<void>[] = [];
  for (const tenant of TENANTS) {
    for (let i = 0; i < tenant.agents; i++) {
      const skills = [...new Set([tenant.skills[i % tenant.skills.length]!, pick(tenant.skills)])];
      agents.push(runAgent(tenant.id, `agent-${i}`, skills, i % 5 === 4));
    }
  }

  // Calls: split over the tenants by their share of agents, random skill and priority.
  const callers: Promise<void>[] = [];
  for (let i = 0; i < CALLS; i++) {
    const tenant = i % 5 < 3 ? TENANTS[0]! : TENANTS[1]!;
    callers.push(runCaller(tenant.id, `call-${i}`, pick(tenant.skills), Math.floor(between(0, 3))));
  }

  const snapshotLoop = takeSnapshots(db, startedAt);
  const deadline = sleep(DEADLINE_MS, 'deadline' as const, { ref: false });
  const finished = await Promise.race([Promise.all(callers).then(() => 'done' as const), deadline]);

  agentsShouldStop = true;
  snapshotsShouldStop = true;
  await Promise.all([...agents, snapshotLoop]);
  for (const tenant of TENANTS) {
    for (let i = 0; i < tenant.agents; i++) await http('POST', `/v1/tenants/${tenant.id}/agents/agent-${i}/logout`);
  }
  const seconds = ((Date.now() - startedAt) / 1000).toFixed(1);

  // ── Evidence ──
  const events = await db.query<EventRow>(`SELECT * FROM events WHERE tenant_id = ANY ($1) ORDER BY id`, [tenantIds]);
  const log = replay(events.rows);
  const finalCalls = await db.query<{ state: string; n: number }>(
    `SELECT state, count(*)::int AS n FROM calls WHERE tenant_id = ANY ($1) GROUP BY state`,
    [tenantIds],
  );
  const callsIn = Object.fromEntries(finalCalls.rows.map((r) => [r.state, r.n])) as Record<string, number>;
  const completed = callsIn.completed ?? 0;
  const abandoned = callsIn.abandoned ?? 0;
  const leftovers = await db.query<{ n: number }>(
    `SELECT ((SELECT count(*) FROM agents WHERE tenant_id = ANY ($1) AND state <> 'offline')
           + (SELECT count(*) FROM calls WHERE tenant_id = ANY ($1) AND state IN ('waiting', 'offered', 'connected')))::int AS n`,
    [tenantIds],
  );
  const acceptedTwice = [...acceptedBy].filter(([, who]) => who.size > 1).map(([call]) => call);
  await db.end();

  // ── Report ──
  const n = (label: string) => log.count[label] ?? 0;
  console.log(`\nFinished in ${seconds}s${finished === 'deadline' ? '  (DEADLINE REACHED)' : ''}\n`);
  console.log(`Calls      ${log.calls} queued: ${completed} answered and completed, ${abandoned} abandoned` +
    ` (${n('abandoned while ringing')} of them while ringing at an agent)`);
  console.log(`Offers     ${n('offered')} made: ${n('accepted')} accepted, ${n('declined:timeout')} timed out,` +
    ` ${n('declined:rejected')} rejected, ${n('abandoned while ringing')} ended by the caller`);
  console.log(`Instances  offers decided by ${Object.entries(log.offersBy).map(([i, c]) => `${i}: ${c}`).join(', ')}`);
  console.log(`Evidence   ${events.rows.length} events replayed, ${snapshots.taken} table snapshots, ${acceptedBy.size} accepts seen by agents\n`);

  const checks: [string, boolean, string][] = [
    [
      'No agent had two calls at once',
      log.problems.agentWithTwoCalls.length === 0 && snapshots.double_booked === 0 && agentSawTwoCalls === 0,
      `event log: ${log.problems.agentWithTwoCalls.length} violations, snapshots: ${snapshots.double_booked}, agents' own view: ${agentSawTwoCalls}`,
    ],
    [
      'No call was offered to two agents at once',
      log.problems.callWithTwoAgents.length === 0 && snapshots.disagree === 0 && acceptedTwice.length === 0,
      `event log: ${log.problems.callWithTwoAgents.length} violations, agent/call mismatch snapshots: ${snapshots.disagree}, calls accepted by two agents: ${acceptedTwice.length}`,
    ],
    [
      'Every call ended answered or abandoned',
      completed + abandoned === CALLS && log.unfinished.length === 0 && leftovers.rows[0]!.n === 0,
      `${completed} answered + ${abandoned} abandoned = ${completed + abandoned} of ${CALLS}; nothing left ringing or connected: ${leftovers.rows[0]!.n === 0}`,
    ],
    [
      'Every transition in the event log was a legal one',
      log.problems.illegal.length === 0,
      `${log.problems.illegal.length} illegal transitions`,
    ],
    [
      'No call waited while a skilled agent was idle',
      snapshots.starved === 0,
      `${snapshots.starved} of ${snapshots.taken} snapshots`,
    ],
    [
      'The run exercised the hard cases',
      n('declined:timeout') > 0 && n('abandoned while ringing') > 0 && Object.keys(log.offersBy).length >= URLS.length && serverErrors === 0,
      `timeouts: ${n('declined:timeout')}, hang-ups while ringing: ${n('abandoned while ringing')}, instances making offers: ${Object.keys(log.offersBy).length}, server errors: ${serverErrors}`,
    ],
  ];
  for (const [name, ok, detail] of checks) console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}\n      ${detail}`);
  for (const problem of [...log.problems.agentWithTwoCalls, ...log.problems.callWithTwoAgents, ...log.problems.illegal].slice(0, 20)) {
    console.log(`      ! ${problem}`);
  }

  const passed = checks.every(([, ok]) => ok);
  console.log(`\n${passed ? 'ALL CHECKS PASSED' : 'SOME CHECKS FAILED'}`);
  return passed ? 0 : 1;
}

process.exit(await main());
