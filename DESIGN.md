# Design: Call Routing Engine

## Approach

Postgres holds all state. The service instances hold none: no queue, timer or cache lives in memory. Every change to a tenant's calls or agents runs in one transaction that first takes that tenant's lock (`pg_advisory_xact_lock`), and routing runs at the end of that same transaction. So within a tenant, events are applied one at a time, in a definite order, whichever instance receives them.

## States and transitions

| Call: from | Event | To |
|---|---|---|
| (none) | new call | `waiting` |
| `waiting` | engine offers it to an agent | `offered`, deadline = now + 15 s |
| `offered` | agent accepts before the deadline | `connected` |
| `offered` | agent rejects, deadline passes, agent goes away or logs out | `waiting`, same queue position |
| `waiting`, `offered` | caller hangs up | `abandoned` (final) |
| `connected` | agent ends the call, or caller hangs up | `completed` (final, answered) |

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

Anything else is refused with `409`. A repeated request whose effect already happened returns `200` and changes nothing.

**Routing rule:** take the waiting call with the highest priority, oldest first among equals, and offer it to the available agent with that skill who has been idle the longest. A call that no available agent can answer is skipped, so it does not block the calls behind it.

## Events at the same moment

Two requests for the same tenant never run together. The second waits for the lock, then reads the state the first one left. "At the same moment" becomes "one after the other", and every transition checks the current state before it writes.

**Accept against the 15 seconds.** The deadline is a timestamp on the call row, written and compared with the database clock, so the instances' own clocks do not matter. Accept succeeds only if the call is still offered to that agent *and* the deadline has not passed. Timeout applies only if the deadline *has* passed. Both cannot be true, so exactly one happens: the call connects, or the agent is set away, the call goes to the next agent and the late accept gets `409`. The sweeper applies timeouts every 500 ms. Its delay can only delay the re-offer; it can never let a late accept through.

**Hang-up against accept.** Whichever takes the lock first wins. The other sees the new state: a late accept gets `409`, a late hang-up ends the connected call.

**Late requests.** Accept, reject and end carry the call id, so a delayed request about an old call cannot act on the call the agent has now.

## Two instances

1. **The lock lives in Postgres**, so both instances share it. Tenants never wait for each other.
2. **The schema refuses the bad states.** A call has one `agent_id` column, so it cannot be with two agents. A partial unique index allows one live call per agent. If the lock were bypassed, the write would fail instead of corrupting state.
3. **Both instances run the timeout sweeper.** If both pick up the same overdue offer, the second finds it already handled and does nothing. No leader election is needed.

I checked that the proof can fail: with the lock line removed, the simulation reports violations.

## Restart

Nothing lives in the process, so there is nothing to recover.

- **Calls** are rows. Waiting calls keep their queue position.
- **Timers** are the deadline column. After a restart the sweeper times out overdue offers, late by at most the downtime. While one instance stays up there is no delay.
- **A request in flight** commits fully or not at all, and Postgres releases the lock when the connection drops. Clients can retry, because requests are safe to repeat.
- **No routing work is lost**, because routing commits with the event that caused it.

Tested: the simulation passes while one instance is restarted and the other is killed with `SIGKILL`.

## Not done, and next

Assumptions are in the README. The main one: a reject is treated like a missed call, so the agent is set away. Keeping them available needs a per-call exclusion list, or the same call bounces straight back.

**Chose not to do**

- **Redis or a message broker.** One store gives locking, ordering and durability. Each extra system adds a failure mode between "decided" and "saved".
- **In-memory timers and leader election**, for the reasons above.
- **A `SKIP LOCKED` worker pool.** It scales better, but workers can match out of order, which breaks "highest priority, longest wait".

**Cost accepted:** one lock per tenant caps a tenant at roughly 200 transitions per second on my laptop, and routing scans the queue. Tenants run in parallel (about 3,600 per second across 20). Enough for one contact center, and the first thing to change at larger scale.

**Next, in order**

1. Push offers to agents (SSE or WebSocket fed by `LISTEN/NOTIFY`) instead of polling. The `events` table is already an outbox.
2. Scale inside a tenant: a lock per skill group, and targeted matching.
3. Agent heartbeats, so a vanished agent goes offline before a caller waits 15 seconds on them.
4. Wrap-up state, priority aging against starvation, wait-time and abandon-rate metrics, authentication.
