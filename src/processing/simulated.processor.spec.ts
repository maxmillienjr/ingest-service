import type { ConfigService } from '@nestjs/config';
import type { Env } from '../config/env.schema';
import type { EventDoc } from '../events/event.types';
import { SimulatedProcessor } from './simulated.processor';

const config = (values: Partial<Env>): ConfigService<Env, true> =>
  ({ get: (key: keyof Env) => values[key] }) as unknown as ConfigService<
    Env,
    true
  >;

const event = { _id: 'e1', data: { secret: 'phi' } } as unknown as EventDoc;

describe('SimulatedProcessor', () => {
  it('waits the configured delay and returns a result without the payload', async () => {
    const processor = new SimulatedProcessor(
      config({ PROCESSING_DELAY_MS: 30, FAILURE_RATE: 0 }),
    );
    const started = Date.now();
    const result = await processor.process(event);

    expect(Date.now() - started).toBeGreaterThanOrEqual(25);
    expect(result).toEqual({ eventId: 'e1', simulatedDelayMs: 30 });
    expect(JSON.stringify(result)).not.toContain('phi');
  });

  it('throws when the failure rate says so', async () => {
    const processor = new SimulatedProcessor(
      config({ PROCESSING_DELAY_MS: 0, FAILURE_RATE: 1 }),
    );
    await expect(processor.process(event)).rejects.toThrow(/simulated/);
  });
});
