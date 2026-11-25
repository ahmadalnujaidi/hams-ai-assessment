import Fastify, { type FastifyInstance } from 'fastify';
import { RoutingError, type Engine } from './engine.ts';

/*
 * HTTP API. A thin layer: validate the request, call the engine, return its view.
 * The tenant is part of every path. In production it would come from the caller's credentials.
 *
 *   POST /v1/tenants/:tenant/agents/:agent/login       { skills: string[] }
 *   POST /v1/tenants/:tenant/agents/:agent/logout
 *   POST /v1/tenants/:tenant/agents/:agent/available
 *   POST /v1/tenants/:tenant/agents/:agent/away
 *   POST /v1/tenants/:tenant/agents/:agent/accept      { callId }
 *   POST /v1/tenants/:tenant/agents/:agent/reject      { callId }
 *   POST /v1/tenants/:tenant/agents/:agent/end         { callId }
 *   GET  /v1/tenants/:tenant/agents/:agent
 *
 *   POST /v1/tenants/:tenant/calls                     { skill, priority?, id? }
 *   POST /v1/tenants/:tenant/calls/:call/hangup
 *   GET  /v1/tenants/:tenant/calls/:call
 */

type AgentParams = { tenant: string; agent: string };
type CallParams = { tenant: string; call: string };

const callIdBody = {
  type: 'object',
  required: ['callId'],
  properties: { callId: { type: 'string', minLength: 1 } },
} as const;

export function buildApi(engine: Engine): FastifyInstance {
  const app = Fastify();

  app.setErrorHandler((err: Error & { statusCode?: number }, _req, reply) => {
    if (err instanceof RoutingError) {
      return reply.code(err.status).send({ error: err.code, message: err.message });
    }
    // The framework's own client errors: a body that fails validation, malformed JSON, and so on.
    if (err.statusCode && err.statusCode >= 400 && err.statusCode < 500) {
      return reply.code(err.statusCode).send({ error: 'bad_request', message: err.message });
    }
    console.error(err);
    return reply.code(500).send({ error: 'internal', message: 'Internal error' });
  });

  app.get('/healthz', async () => ({ ok: true, instance: engine.instance }));

  // ── Agents ──

  app.post<{ Params: AgentParams; Body: { skills: string[] } }>(
    '/v1/tenants/:tenant/agents/:agent/login',
    {
      schema: {
        body: {
          type: 'object',
          required: ['skills'],
          properties: { skills: { type: 'array', items: { type: 'string', minLength: 1 } } },
        },
      },
    },
    (req) => engine.login(req.params.tenant, req.params.agent, req.body.skills),
  );

  app.post<{ Params: AgentParams }>('/v1/tenants/:tenant/agents/:agent/logout', (req) =>
    engine.logout(req.params.tenant, req.params.agent),
  );

  app.post<{ Params: AgentParams }>('/v1/tenants/:tenant/agents/:agent/available', (req) =>
    engine.setAvailable(req.params.tenant, req.params.agent),
  );

  app.post<{ Params: AgentParams }>('/v1/tenants/:tenant/agents/:agent/away', (req) =>
    engine.setAway(req.params.tenant, req.params.agent),
  );

  app.post<{ Params: AgentParams; Body: { callId: string } }>(
    '/v1/tenants/:tenant/agents/:agent/accept',
    { schema: { body: callIdBody } },
    (req) => engine.accept(req.params.tenant, req.params.agent, req.body.callId),
  );

  app.post<{ Params: AgentParams; Body: { callId: string } }>(
    '/v1/tenants/:tenant/agents/:agent/reject',
    { schema: { body: callIdBody } },
    (req) => engine.reject(req.params.tenant, req.params.agent, req.body.callId),
  );

  app.post<{ Params: AgentParams; Body: { callId: string } }>(
    '/v1/tenants/:tenant/agents/:agent/end',
    { schema: { body: callIdBody } },
    (req) => engine.endCall(req.params.tenant, req.params.agent, req.body.callId),
  );

  app.get<{ Params: AgentParams }>('/v1/tenants/:tenant/agents/:agent', (req) =>
    engine.getAgent(req.params.tenant, req.params.agent),
  );

  // ── Calls ──

  app.post<{ Params: { tenant: string }; Body: { skill: string; priority?: number; id?: string } }>(
    '/v1/tenants/:tenant/calls',
    {
      schema: {
        body: {
          type: 'object',
          required: ['skill'],
          properties: {
            skill: { type: 'string', minLength: 1 },
            priority: { type: 'integer' },
            id: { type: 'string', minLength: 1 },
          },
        },
      },
    },
    async (req, reply) => {
      const { created, call } = await engine.newCall(req.params.tenant, req.body);
      return reply.code(created ? 201 : 200).send(call);
    },
  );

  app.post<{ Params: CallParams }>('/v1/tenants/:tenant/calls/:call/hangup', (req) =>
    engine.hangup(req.params.tenant, req.params.call),
  );

  app.get<{ Params: CallParams }>('/v1/tenants/:tenant/calls/:call', (req) =>
    engine.getCall(req.params.tenant, req.params.call),
  );

  return app;
}
