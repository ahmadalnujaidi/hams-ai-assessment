-- Postgres is the only place where state lives. Both service instances read and write these tables.
-- Every statement is idempotent, so the file is applied on every boot.

CREATE TABLE IF NOT EXISTS agents (
    tenant_id   text        NOT NULL,
    id          text        NOT NULL,
    skills      text[]      NOT NULL DEFAULT '{}',
    state       text        NOT NULL DEFAULT 'offline'
                CHECK (state IN ('offline', 'away', 'available', 'offered', 'busy')),
    -- When the agent last became available. "Idle the longest" means the smallest idle_since.
    idle_since  timestamptz,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS calls (
    tenant_id        text        NOT NULL,
    id               text        NOT NULL,
    skill            text        NOT NULL,
    priority         int         NOT NULL DEFAULT 0,   -- higher is more urgent
    state            text        NOT NULL DEFAULT 'waiting'
                     CHECK (state IN ('waiting', 'offered', 'connected', 'completed', 'abandoned')),
    -- The agent this call is offered to or connected with. One column, so a call can never be
    -- with two agents at the same moment.
    agent_id         text,
    -- Queue position. Never changes, so a call that returns to the queue keeps its place.
    enqueued_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    -- The 15-second timer. It is data, not an in-memory timer, so it survives a restart.
    offer_expires_at timestamptz,
    answered_at      timestamptz,
    ended_at         timestamptz,
    PRIMARY KEY (tenant_id, id),
    -- A call can only point at an agent of its own tenant.
    FOREIGN KEY (tenant_id, agent_id) REFERENCES agents (tenant_id, id),
    CHECK (state NOT IN ('offered', 'connected') OR agent_id IS NOT NULL),
    CHECK (state <> 'waiting' OR agent_id IS NULL),
    CHECK ((state = 'offered') = (offer_expires_at IS NOT NULL))
);

-- Safety net below the application: an agent can hold at most one live call.
-- If a bug or a second instance ever tried to give an agent two calls, the write fails.
CREATE UNIQUE INDEX IF NOT EXISTS one_live_call_per_agent
    ON calls (tenant_id, agent_id) WHERE state IN ('offered', 'connected');

-- The queue in routing order: highest priority first, then longest wait.
CREATE INDEX IF NOT EXISTS calls_queue
    ON calls (tenant_id, priority DESC, enqueued_at) WHERE state = 'waiting';

-- Offers by deadline, for the timeout sweeper.
CREATE INDEX IF NOT EXISTS calls_offer_deadline
    ON calls (offer_expires_at) WHERE state = 'offered';

-- Available agents, longest idle first.
CREATE INDEX IF NOT EXISTS agents_available
    ON agents (tenant_id, idle_since) WHERE state = 'available';

-- Append-only log of every routing decision, written in the same transaction as the decision.
-- The simulation replays it to check the invariants. Within one tenant, id order is the real order.
CREATE TABLE IF NOT EXISTS events (
    id         bigserial   PRIMARY KEY,
    at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    tenant_id  text        NOT NULL,
    type       text        NOT NULL,   -- queued | offered | accepted | declined | abandoned | completed
    call_id    text        NOT NULL,
    agent_id   text,
    reason     text,                   -- declined: rejected | timeout | agent_away | agent_logout
    instance   text        NOT NULL    -- which service instance made the decision
);

CREATE INDEX IF NOT EXISTS events_tenant ON events (tenant_id, id);
