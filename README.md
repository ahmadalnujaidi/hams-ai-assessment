# Call Routing Engine

A routing engine for a multi-tenant contact center. Calls wait in a queue, agents answer them, and the engine decides which agent gets which call. There is no audio: calls and agents are API events.

The design and its reasons are in [DESIGN.md](DESIGN.md).

## Run it

```bash
docker compose up --build        # Postgres + two instances: localhost:8081 and localhost:8082
docker compose run --rm sim      # in a second terminal: the simulation, about one minute
```

The simulation runs 50 agents in two tenants and a burst of 300 calls against both instances. Some agents let calls ring out or reject them, and some callers hang up. It ends with a report and exits non-zero if any check fails:

```
Calls      300 queued: 246 answered and completed, 54 abandoned (7 of them while ringing at an agent)
Offers     305 made: 246 accepted, 29 timed out, 23 rejected, 7 ended by the caller
Instances  offers decided by app2: 149, app1: 156
Evidence   1203 events replayed, 277 table snapshots, 246 accepts seen by agents

PASS  No agent had two calls at once
PASS  No call was offered to two agents at once
PASS  Every call ended answered or abandoned
PASS  Every transition in the event log was a legal one
PASS  No call waited while a skilled agent was idle
PASS  The run exercised the hard cases
```

Each of the three required results is checked from more than one source: a replay of the event log, snapshots of the live tables taken five times a second during the run, what the simulated agents saw through the API, and the final state of the tables.

To see the restart behaviour, run this while the simulation is running. It still passes:

```bash
docker compose restart app1
docker compose kill -s SIGKILL app2 && docker compose start app2
```

### Tests

The tests cover routing order, timeouts, and the races, with two engines standing in for two instances. They need Node 24 and the database from `docker compose up`:

```bash
npm install
npm test
```

## API

Every path starts with `/v1/tenants/:tenant`.

| Method and path | Body | What it does |
|---|---|---|
| `POST /agents/:agent/login` | `{ "skills": ["arabic", "billing"] }` | Log in. Creates the agent on first use. State becomes `away`. |
| `POST /agents/:agent/logout` | | Log out. |
| `POST /agents/:agent/available` | | Ready for calls. |
| `POST /agents/:agent/away` | | Not ready. |
| `POST /agents/:agent/accept` | `{ "callId": "..." }` | Accept the offered call. |
| `POST /agents/:agent/reject` | `{ "callId": "..." }` | Reject the offered call. |
| `POST /agents/:agent/end` | `{ "callId": "..." }` | End the connected call. |
| `GET  /agents/:agent` | | The agent's state and the call ringing at or connected to them. |
| `POST /calls` | `{ "skill": "billing", "priority": 1, "id": "optional" }` | New call. A higher priority is more urgent. |
| `POST /calls/:call/hangup` | | The caller hangs up. |
| `GET  /calls/:call` | | The call's status. |

Agents learn about offers by polling `GET /agents/:agent`. Errors are `404` for an unknown id and `409` when the request is not allowed in the current state, with a body like `{ "error": "offer_expired", "message": "..." }`.

```bash
T=http://localhost:8081/v1/tenants/acme
curl -X POST $T/agents/alice/login -H 'content-type: application/json' -d '{"skills":["billing"]}'
curl -X POST $T/agents/alice/available
curl -X POST $T/calls -H 'content-type: application/json' -d '{"id":"c1","skill":"billing","priority":1}'
curl http://localhost:8082/v1/tenants/acme/agents/alice        # the other instance sees the offer
curl -X POST $T/agents/alice/accept -H 'content-type: application/json' -d '{"callId":"c1"}'
curl -X POST $T/agents/alice/end -H 'content-type: application/json' -d '{"callId":"c1"}'
```

## Code map

| File | What is in it |
|---|---|
| `src/engine.ts` | The routing engine: every state transition and the routing rule. Start here. |
| `src/db.ts` | The per-tenant transaction lock and the schema bootstrap. |
| `src/schema.sql` | Tables, and the constraints that refuse invalid states. |
| `src/sweeper.ts` | The loop that times out overdue offers. |
| `src/api.ts`, `src/server.ts` | HTTP routes and process start-up. |
| `sim/simulate.ts` | The simulation and its checks. |
| `test/engine.test.ts` | Tests for routing order, timeouts and races. |

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `OFFER_TIMEOUT_MS` | `15000` | How long an agent has to accept. |
| `SWEEP_INTERVAL_MS` | `500` | How often each instance looks for overdue offers. |
| `DATABASE_URL` | compose database | Postgres connection string. |
| `INSTANCE_ID` | host name | Name stamped on every event this instance writes. |
| `SIM_CALLS`, `SIM_SEED` | `300`, random | Size of the burst, and a seed to repeat a run. |

The database has no volume, so `docker compose down` gives a clean start. It is published on port 55432 to stay clear of a local Postgres.

## Assumptions

The brief leaves these open. Each is a decision I made, and most are a few lines to change.

- **Reject** is treated like a missed call: the agent is set away and the call goes to the next agent.
- **Accept after the deadline** is refused, judged by the database clock, even if the timeout has not been processed yet.
- **A call that returns to the queue** keeps its original position.
- **A caller who hangs up while the call is ringing** does not cost the agent their place in the idle order.
- **A call nobody can answer** (no available agent has the skill) waits without blocking other calls.
- **Login** leaves the agent away; they set themselves available. Logging in again updates the skills.
- **Going away or logging out while a call is ringing** returns the call to the queue. Both are refused during a connected call.
- **After a call ends** the agent is available at once. There is no wrap-up time.
- **A caller hanging up on a connected call** ends it as completed. Hanging up a finished call does nothing.
- **Sending a new call with an id that exists** returns the existing call, so the sender can retry.
- **The tenant** comes from the URL. Authentication and tenant provisioning are out of scope.
- **AI tools** were used, as the brief allows. The commit history shows where.
