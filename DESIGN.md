# Design: Call Routing Engine

## Approach

Postgres holds all state. The service instances hold none: no queue, no timer and no cache lives in memory. Every change to a tenant's calls or agents runs in one transaction that first takes that tenant's lock (`pg_advisory_xact_lock`), and routing runs at the end of that same transaction. So within a tenant, events are applied one at a time, in a definite order, whichever instance receives them.

## States and transitions

| Call: from | Event | To |
|---|---|---|
| (none) | new call | `waiting` |
| `waiting` | engine offers it to an agent | `offered`, deadline = now + 15 s |
| `offered` | agent accepts before the deadline | `connected` |
| `offered` | agent rejects, deadline passes, agent goes away or logs out | `waiting`, same queue position |
| `waiting`, `offered` | caller hangs up | `abandoned` (final) |
| `connected` | agent ends the call, or caller hangs up | `completed` (final, counts as answered) |

| Agent: from | Event | To |
|---|---|---|
| `offline` | log in | `away` |
| `away` | set available | `available`, idle clock starts |
| `available` | set away | `away` |
| `available` | engine offers a call | `offered` |
| `offered` | accept | `busy` |
| `offered` | reject, or deadline passes | `away` |
| `offered` | caller hangs up | `available`, idle clock **not** reset |
| `busy` | call ends | `available`, idle clock restarts |
| `away`, `available`, `offered` | log out | `offline` |

Anything else is refused with `409`, for example logging out while on a call. A repeated request whose effect already happened returns `200` and changes nothing.

**Routing rule** (`route` in `src/engine.ts`): take the waiting call with the highest priority, oldest first among equals, and offer it to the available agent with that skill who has been idle the longest. A call that no available agent can answer is skipped, so it does not block the calls behind it.

## Events at the same moment

Two requests for the same tenant never run together. The second waits for the lock, then reads the state the first one left. "At the same moment" becomes "one after the other", and every transition checks the current state before it writes.

**Accept against the 15 seconds.** The deadline is a timestamp on the call row, written and compared with the database clock, so the clocks of the instances do not matter. Accept succeeds only if the call is still offered to that agent *and* the deadline has not passed. Timeout applies only if the call is still offered *and* the deadline has passed. Both cannot be true, and the lock puts them in order, so exactly one happens: the call is connected, or the agent is set away, the call goes to the next agent and the late accept gets `409`. The sweeper that applies timeouts runs every 500 ms. Its delay can only delay the re-offer; it can never let a late accept through.

**Hang-up against accept.** Whichever takes the lock first wins. Hang-up first: the call is abandoned and the accept gets `409`. Accept first: the call connects, then the hang-up completes it.

**Late requests.** Accept, reject and end carry the call id, so a delayed request about an old call cannot act on the call the agent has now.

## Two instances

1. **The lock lives in Postgres**, so both instances share it. Different tenants never wait for each other.
2. **The schema refuses the bad states.** A call has a single `agent_id` column, so it cannot be with two agents. A partial unique index allows one live call per agent. A composite foreign key keeps a call with agents of its own tenant. If the lock were ever bypassed, the write would fail instead of corrupting state.
3. **Both instances run the timeout sweeper.** If both pick up the same overdue offer, the second finds it already handled and does nothing. No leader election is needed.

I checked that the proof can fail: with the lock line removed, the simulation reports violations.

## Restart

There is nothing to recover, because nothing lives in the process.

- **Calls** are rows. Waiting calls stay queued in the same position.
- **Timers** are the deadline column. After a restart the sweeper finds overdue offers and times them out, late by at most the downtime. While one instance is still up there is no delay.
- **A request in flight** commits fully or not at all. Postgres releases the tenant lock when the connection drops. The client can retry, because requests are safe to repeat.
- **No routing work can be lost**, because routing commits in the same transaction as the event that caused it.

Tested: the simulation passes while one instance is restarted and the other is killed with `SIGKILL`.

## Assumptions

- A reject is treated like a missed call: the agent is set away. If the agent stayed available, the same call would bounce straight back to them; doing that properly needs a per-call exclusion list.
- Login leaves the agent `away`; they set themselves available. There is no wrap-up time after a call.
- Agents learn about offers by polling their own status. Skills are flat: an agent has a skill or not.
- The tenant comes from the URL. Authentication is out of scope.

## What I chose not to do, and what comes next

**Not done, on purpose**

- **No Redis or message broker.** One store gives locking, ordering and durability. Each extra system adds a failure mode between "decided" and "saved".
- **No in-memory timers and no leader election**, for the reasons above.
- **No `SKIP LOCKED` worker pool.** It scales better, but two workers can then match out of order, which breaks "highest priority, longest wait".

**The cost I accepted:** one lock per tenant caps a tenant at roughly 200 transitions per second on my laptop, and `route` scans the queue. Tenants scale in parallel (about 3,600 per second across 20 tenants). That is enough for a contact center tenant, and it is the first thing to change at larger scale.

**Next, in order**

1. Push offers to agents (SSE or WebSocket fed by `LISTEN/NOTIFY`) instead of polling. The `events` table is already an outbox.
2. Scale inside a tenant: lock per skill group, and targeted matching (a new call looks only for an agent, a freed agent looks only for a call).
3. Agent heartbeats, so an agent whose client disappeared goes offline before a caller waits 15 seconds on them.
4. Wrap-up state, reject without going away, priority aging so low-priority calls cannot starve, wait-time and abandon-rate metrics, retention for `events`.
