import { retryDelayMs } from './backoff';

describe('retryDelayMs', () => {
  it('doubles from the base and caps at the max', () => {
    expect(
      [1, 2, 3, 4, 5, 6, 7].map((n) => retryDelayMs(n, 1000, 60_000)),
    ).toEqual([1000, 2000, 4000, 8000, 16_000, 32_000, 60_000]);
  });

  it('treats a nonsensical attempt number as the first', () => {
    expect(retryDelayMs(0, 1000, 60_000)).toBe(1000);
  });
});
