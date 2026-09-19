/** Exponential backoff: base, 2x, 4x... capped at max. `attempt` is 1-based. */
export function retryDelayMs(
  attempt: number,
  baseMs: number,
  maxMs: number,
): number {
  return Math.min(baseMs * 2 ** Math.max(0, attempt - 1), maxMs);
}
