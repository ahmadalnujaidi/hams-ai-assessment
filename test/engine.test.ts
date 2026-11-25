import { randomUUID } from 'node:crypto';
import pg from 'pg';
import { afterAll, beforeAll, expect, test } from 'vitest';
import { createPool, migrate } from '../src/db.ts';
import { Engine, RoutingError } from '../src/engine.ts';

/*
 * Tests of the routing rules and the races, against a real Postgres (the one from docker compose).
 * They use their own database, so the running service instances do not interfere.
 * Two engines with separate connection pools stand in for two service instances.
 */

const baseUrl = process.env.DATABASE_URL ?? 'postgres://routing:routing@localhost:55432/routing';
const testUrl = new URL(baseUrl);
testUrl.pathname = '/routing_test';

let poolA: pg.Pool;
let poolB: pg.Pool;
let a: Engine;
let b: Engine;

beforeAll(async () => {
  const admin = new pg.Client({ connectionString: baseUrl });
  await admin.connect();
  await admin.query('CREATE DATABASE routing_test').catch((err) => {
    if (err.code !== '42P04') throw err; // 42P04: database already exists
  });
  await admin.end();

  poolA = createPool(testUrl.href);
  poolB = createPool(testUrl.href);
  await migrate(poolA);
  a = new Engine(poolA, { offerTimeoutMs: 15_000, instance: 'test-a' });
  b = new Engine(poolB, { offerTimeoutMs: 15_000, instance: 'test-b' });
});

afterAll(async () => {
  await poolA.end();
  await poolB.end();
});

/** Every test gets its own tenant, so tests do not see each other's calls and agents. */
const newTenant = () => `t-${randomUUID()}`;

async function availableAgent(tenant: string, id: string, skills: string[]) {
  await a.login(tenant, id, skills);
  return a.setAvailable(tenant, id);
}

/** Move an offer's deadline into the past, as if 15 seconds had gone by. */
async function passDeadline(tenant: string, callId: string) {
  await poolA.query(
    `UPDATE calls SET offer_expires_at = clock_timestamp() - interval '1 millisecond'
     WHERE tenant_id = $1 AND id = $2 AND state = 'offered'`,
    [tenant, callId],
  );
}

async function errorCode(promise: Promise<unknown>): Promise<string> {
  const err = await promise.then(
    () => null,
    (e) => e,
  );
  expect(err).toBeInstanceOf(RoutingError);
  return (err as RoutingError).code;
}

/** The three things that must never be true, counted in one snapshot. */
async function brokenInvariants(tenant: string) {
  const { rows } = await poolA.query(
    `SELECT
       (SELECT count(*) FROM (SELECT 1 FROM calls WHERE tenant_id = $1 AND state IN ('offered','connected')
                              GROUP BY agent_id HAVING count(*) > 1) x)::int AS "doubleBooked",
       (SELECT count(*) FROM agents ag
          LEFT JOIN calls c ON c.tenant_id = ag.tenant_id AND c.agent_id = ag.id AND c.state IN ('offered','connected')
         WHERE ag.tenant_id = $1
           AND ((ag.state = 'offered') <> (c.state IS NOT DISTINCT FROM 'offered')
             OR (ag.state = 'busy')    <> (c.state IS NOT DISTINCT FROM 'connected')))::int AS "disagree",
       (SELECT count(*) FROM calls c WHERE c.tenant_id = $1 AND c.state = 'waiting'
           AND EXISTS (SELECT 1 FROM agents ag WHERE ag.tenant_id = c.tenant_id
                         AND ag.state = 'available' AND c.skill = ANY (ag.skills)))::int AS "starved"`,
    [tenant],
  );
  return rows[0];
}

// ───────────────────────────── Routing order ─────────────────────────────

test('highest priority first, then longest wait', async () => {
  const t = newTenant();
  await a.login(t, 'alice', ['billing']); // logged in, still away
  await a.newCall(t, { id: 'low-1', skill: 'billing', priority: 0 });
  await a.newCall(t, { id: 'high', skill: 'billing', priority: 5 });
  await a.newCall(t, { id: 'low-2', skill: 'billing', priority: 0 });

  const order: string[] = [];
  let alice = await a.setAvailable(t, 'alice');
  while (alice.calls.length > 0) {
    const callId = alice.calls[0]!.id;
    order.push(callId);
    await a.accept(t, 'alice', callId);
    alice = await a.endCall(t, 'alice', callId); // ending a call routes the next one in the same step
  }
  expect(order).toEqual(['high', 'low-1', 'low-2']);
});

test('the call goes to the skilled agent who has been idle the longest', async () => {
  const t = newTenant();
  await availableAgent(t, 'arabic-only', ['arabic']); // idle longest, wrong skill
  await availableAgent(t, 'first', ['billing']);
  await availableAgent(t, 'second', ['billing', 'arabic']);

  expect((await a.newCall(t, { skill: 'billing' })).call.agentId).toBe('first');
  expect((await a.newCall(t, { skill: 'billing' })).call.agentId).toBe('second');
  expect((await a.newCall(t, { skill: 'billing' })).call.state).toBe('waiting');
});

test('a call nobody can answer does not block the calls behind it', async () => {
  const t = newTenant();
  await a.login(t, 'bob', ['billing']);
  await a.newCall(t, { id: 'urgent-arabic', skill: 'arabic', priority: 9 });
  await a.newCall(t, { id: 'billing', skill: 'billing', priority: 0 });

  const bob = await a.setAvailable(t, 'bob');
  expect(bob.calls[0]?.id).toBe('billing');
  expect((await a.getCall(t, 'urgent-arabic')).state).toBe('waiting');
});

// ───────────────────────────── Offers and timeouts ───────────────────────

test('a missed offer sets the agent away and the call keeps its place in the queue', async () => {
  const t = newTenant();
  await availableAgent(t, 'alice', ['billing']);
  await a.newCall(t, { id: 'c1', skill: 'billing' }); // offered to alice
  await a.newCall(t, { id: 'c2', skill: 'billing' }); // waits behind c1

  await passDeadline(t, 'c1');
  await b.expireOffers(); // the other instance notices the timeout

  expect((await a.getAgent(t, 'alice')).state).toBe('away');
  expect((await a.getCall(t, 'c1')).state).toBe('waiting');

  const bob = await availableAgent(t, 'bob', ['billing']);
  expect(bob.calls[0]?.id).toBe('c1'); // c1 arrived first, so it is still ahead of c2
});

test('an accept after the deadline is refused, even before the sweeper has run', async () => {
  const t = newTenant();
  await availableAgent(t, 'alice', ['billing']);
  await a.newCall(t, { id: 'c1', skill: 'billing' });
  await passDeadline(t, 'c1');

  expect(await errorCode(a.accept(t, 'alice', 'c1'))).toBe('offer_expired');
});

test('accept against timeout: exactly one of them wins, and the state agrees', async () => {
  for (let i = 0; i < 25; i++) {
    const t = newTenant();
    await availableAgent(t, 'alice', ['billing']);
    await a.newCall(t, { id: 'c1', skill: 'billing' });
    // Put the deadline a few milliseconds ahead, then let accept and the sweeper collide.
    await poolA.query(
      `UPDATE calls SET offer_expires_at = clock_timestamp() + interval '3 milliseconds' WHERE tenant_id = $1`,
      [t],
    );
    const [accepted] = await Promise.all([
      a.accept(t, 'alice', 'c1').then(
        () => true,
        () => false,
      ),
      b.expireOffers(),
      a.expireOffers(),
    ]);
    await b.expireOffers();

    const call = await a.getCall(t, 'c1');
    const alice = await a.getAgent(t, 'alice');
    if (accepted) {
      expect([call.state, call.agentId, alice.state]).toEqual(['connected', 'alice', 'busy']);
    } else {
      expect([call.state, call.agentId, alice.state]).toEqual(['waiting', null, 'away']);
    }
    expect(await brokenInvariants(t)).toEqual({ doubleBooked: 0, disagree: 0, starved: 0 });
  }
});

test('rejecting a call sets the agent away and offers the call to the next agent', async () => {
  const t = newTenant();
  await availableAgent(t, 'alice', ['billing']);
  await availableAgent(t, 'bob', ['billing']);
  await a.newCall(t, { id: 'c1', skill: 'billing' });

  const alice = await a.reject(t, 'alice', 'c1');
  expect(alice.state).toBe('away');
  expect((await a.getCall(t, 'c1')).agentId).toBe('bob');
});

// ───────────────────────────── Caller hangs up ───────────────────────────

test('caller hangs up during an offer: the agent is free again and keeps their idle position', async () => {
  const t = newTenant();
  const before = await availableAgent(t, 'alice', ['billing']);
  await availableAgent(t, 'bob', ['billing']);
  await a.newCall(t, { id: 'c1', skill: 'billing' }); // offered to alice

  const call = await a.hangup(t, 'c1');
  expect([call.state, call.outcome]).toEqual(['abandoned', 'abandoned']);

  const alice = await a.getAgent(t, 'alice');
  expect(alice.state).toBe('available');
  expect(alice.idleSince).toEqual(before.idleSince);
  expect((await a.newCall(t, { id: 'c2', skill: 'billing' })).call.agentId).toBe('alice'); // still ahead of bob
});

test('a late request about an old call cannot touch the call the agent has now', async () => {
  const t = newTenant();
  await availableAgent(t, 'alice', ['billing']);
  await a.newCall(t, { id: 'old', skill: 'billing' });
  await a.hangup(t, 'old');
  await a.newCall(t, { id: 'current', skill: 'billing' }); // now ringing at alice

  expect(await errorCode(a.accept(t, 'alice', 'old'))).toBe('not_offered');
  expect(await errorCode(a.endCall(t, 'alice', 'old'))).toBe('not_connected');
  expect((await a.getAgent(t, 'alice')).calls[0]).toMatchObject({ id: 'current', state: 'offered' });
});

test('repeating a request is safe', async () => {
  const t = newTenant();
  await availableAgent(t, 'alice', ['billing']);
  expect((await a.newCall(t, { id: 'c1', skill: 'billing' })).created).toBe(true);
  expect((await b.newCall(t, { id: 'c1', skill: 'billing' })).created).toBe(false);

  await a.accept(t, 'alice', 'c1');
  expect((await b.accept(t, 'alice', 'c1')).state).toBe('busy');
  await a.endCall(t, 'alice', 'c1');
  expect((await b.endCall(t, 'alice', 'c1')).state).toBe('available');
  expect((await a.hangup(t, 'c1')).state).toBe('completed');

  const events = await poolA.query(`SELECT type FROM events WHERE tenant_id = $1 ORDER BY id`, [t]);
  expect(events.rows.map((e) => e.type)).toEqual(['queued', 'offered', 'accepted', 'completed']);
});

// ───────────────────────────── Tenants and instances ─────────────────────

test('tenants are isolated', async () => {
  const [t1, t2] = [newTenant(), newTenant()];
  await availableAgent(t2, 'alice', ['billing']);
  const { call } = await a.newCall(t1, { id: 'c1', skill: 'billing' });

  expect(call.state).toBe('waiting'); // the only billing agent belongs to the other tenant
  expect(await errorCode(a.getCall(t2, 'c1'))).toBe('not_found');
});

test('two instances under a concurrent burst never double-book an agent', async () => {
  const t = newTenant();
  const engines = [a, b];
  const agents = Array.from({ length: 10 }, (_, i) => `agent-${i}`);
  const calls = Array.from({ length: 40 }, (_, i) => `call-${i}`);

  // Agents becoming available and calls arriving, all at once, split over both instances.
  await Promise.all([
    ...agents.map((id, i) => engines[i % 2]!.login(t, id, ['billing']).then(() => engines[(i + 1) % 2]!.setAvailable(t, id))),
    ...calls.map((id, i) => engines[i % 2]!.newCall(t, { id, skill: 'billing' })),
  ]);

  // Work the queue down. Every agent asks both instances to accept the same call at the same time.
  for (let round = 0; round < 10; round++) {
    expect(await brokenInvariants(t)).toEqual({ doubleBooked: 0, disagree: 0, starved: 0 });
    const views = await Promise.all(agents.map((id) => a.getAgent(t, id)));
    const ringing = views.filter((v) => v.state === 'offered');
    if (ringing.length === 0) break;
    await Promise.all(ringing.flatMap((v) => engines.map((e) => e.accept(t, v.id, v.calls[0]!.id))));
    await Promise.all(ringing.map((v, i) => engines[i % 2]!.endCall(t, v.id, v.calls[0]!.id)));
  }

  const states = await poolA.query(`SELECT state, count(*)::int AS n FROM calls WHERE tenant_id = $1 GROUP BY state`, [t]);
  expect(states.rows).toEqual([{ state: 'completed', n: 40 }]);
});
