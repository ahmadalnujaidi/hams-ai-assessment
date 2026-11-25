import { readFileSync } from 'node:fs';
import pg from 'pg';

/** A connection that is inside an open transaction. */
export type Tx = pg.PoolClient;

export function createPool(connectionString: string): pg.Pool {
  return new pg.Pool({ connectionString, max: 10 });
}

/**
 * Apply schema.sql. Both instances boot at the same time, so a session advisory lock
 * makes sure only one of them runs the DDL at a time.
 */
export async function migrate(pool: pg.Pool): Promise<void> {
  const schema = readFileSync(new URL('./schema.sql', import.meta.url), 'utf8');
  const client = await pool.connect();
  try {
    await client.query('SELECT pg_advisory_lock(7, 1)');
    await client.query(schema);
  } finally {
    await client.query('SELECT pg_advisory_unlock(7, 1)').catch(() => {});
    client.release();
  }
}

/**
 * Run `fn` in a transaction that holds the tenant's lock.
 *
 * This is the whole concurrency model: every change to a tenant's calls or agents happens
 * inside this function, so changes to one tenant run one at a time, no matter how many
 * service instances there are. Different tenants do not block each other.
 *
 * The lock is a transaction-level advisory lock. Postgres releases it at COMMIT or ROLLBACK,
 * and also if the connection dies, so a crashed instance cannot leave a tenant locked.
 *
 * The isolation level must stay READ COMMITTED (the default): each statement then sees
 * everything the previous lock holder committed. Under REPEATABLE READ the snapshot would be
 * taken before waiting for the lock, and the transaction would act on stale state.
 */
export async function withTenantLock<T>(
  pool: pg.Pool,
  tenantId: string,
  fn: (tx: Tx) => Promise<T>,
): Promise<T> {
  const tx = await pool.connect();
  let broken = false;
  try {
    await tx.query('BEGIN');
    await tx.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [tenantId]);
    const result = await fn(tx);
    await tx.query('COMMIT');
    return result;
  } catch (err) {
    await tx.query('ROLLBACK').catch(() => {
      broken = true;
    });
    throw err;
  } finally {
    tx.release(broken); // a connection that could not roll back is destroyed, not reused
  }
}
