import { setTimeout as sleep } from 'node:timers/promises';
import type { Engine } from './engine.ts';

/**
 * The 15-second timer. There is no in-memory timer per offer: the deadline is a column, and
 * this loop asks the database which offers are overdue. Every instance runs it. After a
 * restart, or if one instance dies, overdue offers are still found, only a little late.
 *
 * Returns a function that stops the loop and waits for the current pass to finish.
 */
export function startSweeper(engine: Engine, intervalMs: number): () => Promise<void> {
  const stop = new AbortController();

  const loop = (async () => {
    while (!stop.signal.aborted) {
      try {
        await engine.expireOffers();
      } catch (err) {
        console.error('sweep failed, will retry', err);
      }
      await sleep(intervalMs, undefined, { signal: stop.signal }).catch(() => {});
    }
  })();

  return async () => {
    stop.abort();
    await loop;
  };
}
