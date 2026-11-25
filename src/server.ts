import { hostname } from 'node:os';
import { buildApi } from './api.ts';
import { createPool, migrate } from './db.ts';
import { Engine } from './engine.ts';
import { startSweeper } from './sweeper.ts';

const config = {
  port: Number(process.env.PORT ?? 8080),
  databaseUrl: process.env.DATABASE_URL ?? 'postgres://routing:routing@localhost:55432/routing',
  offerTimeoutMs: Number(process.env.OFFER_TIMEOUT_MS ?? 15_000),
  sweepIntervalMs: Number(process.env.SWEEP_INTERVAL_MS ?? 500),
  instance: process.env.INSTANCE_ID ?? hostname(),
};

const pool = createPool(config.databaseUrl);
pool.on('error', (err) => console.error('idle database connection failed', err.message));

await migrate(pool);

const engine = new Engine(pool, { offerTimeoutMs: config.offerTimeoutMs, instance: config.instance });
const api = buildApi(engine);
await api.listen({ host: '0.0.0.0', port: config.port });
const stopSweeper = startSweeper(engine, config.sweepIntervalMs);

console.log(`[${config.instance}] listening on :${config.port}, offer timeout ${config.offerTimeoutMs} ms`);

// The process holds no state of its own, so shutting down only means finishing what is in flight.
async function shutdown(signal: string): Promise<void> {
  console.log(`[${config.instance}] ${signal}, shutting down`);
  await api.close();
  await stopSweeper();
  await pool.end();
  process.exit(0);
}
process.once('SIGTERM', () => void shutdown('SIGTERM'));
process.once('SIGINT', () => void shutdown('SIGINT'));
