import { validateEnv } from './env.schema';

describe('validateEnv', () => {
  it('applies defaults when nothing is set', () => {
    const env = validateEnv({});
    expect(env).toMatchObject({
      MONGO_URI: 'mongodb://localhost:27017/ingest',
      PORT: 3000,
      ROLE: 'both',
      WORKER_CONCURRENCY: 100,
      PROCESSING_DELAY_MS: 5_000,
      FAILURE_RATE: 0,
    });
  });

  it('coerces string values from the environment into numbers', () => {
    expect(validateEnv({ PORT: '8080', FAILURE_RATE: '0.25' })).toMatchObject({
      PORT: 8080,
      FAILURE_RATE: 0.25,
    });
  });

  it('rejects a lease that a processing call could outlive', () => {
    expect(() =>
      validateEnv({ PROCESSING_DELAY_MS: '20000', LEASE_TTL_MS: '30000' }),
    ).toThrow(/LEASE_TTL_MS must be at least twice PROCESSING_DELAY_MS/);
    expect(
      validateEnv({ PROCESSING_DELAY_MS: '15000', LEASE_TTL_MS: '30000' }),
    ).toMatchObject({ LEASE_TTL_MS: 30_000 });
  });

  it('rejects an unknown role and names the variable', () => {
    expect(() => validateEnv({ ROLE: 'batch' })).toThrow(/ROLE/);
  });

  it('rejects a failure rate outside 0..1', () => {
    expect(() => validateEnv({ FAILURE_RATE: '1.5' })).toThrow(/FAILURE_RATE/);
  });

  it('rejects a non-mongodb URI', () => {
    expect(() => validateEnv({ MONGO_URI: 'postgres://db' })).toThrow(
      /MONGO_URI/,
    );
  });

  it('drops variables it does not know about', () => {
    expect(validateEnv({ HOME: '/home/someone' })).not.toHaveProperty('HOME');
  });
});
