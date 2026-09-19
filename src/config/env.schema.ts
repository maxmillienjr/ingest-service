import type { ConfigService } from '@nestjs/config';
import { z } from 'zod';

const positiveInt = (fallback: number): z.ZodDefault<z.ZodCoercedNumber> =>
  z.coerce.number().int().min(1).default(fallback);
const nonNegativeInt = (fallback: number): z.ZodDefault<z.ZodCoercedNumber> =>
  z.coerce.number().int().min(0).default(fallback);

/**
 * Every environment variable the service reads, with its default.
 * Validated once at boot; nothing below main.ts touches process.env.
 */
export const envSchema = z
  .object({
    MONGO_URI: z
      .string()
      .regex(
        /^mongodb(\+srv)?:\/\//,
        'must be a mongodb:// or mongodb+srv:// URI',
      )
      .default('mongodb://localhost:27017/ingest'),
    PORT: positiveInt(3000),
    /** api: HTTP only. worker: processing only. both: one process does everything. */
    ROLE: z.enum(['api', 'worker', 'both']).default('both'),

    /** Patients processed concurrently per worker instance. */
    WORKER_CONCURRENCY: positiveInt(100),
    /** Simulated duration of the external call. */
    PROCESSING_DELAY_MS: nonNegativeInt(5_000),
    /** Probability (0..1) that the simulated external call fails. */
    FAILURE_RATE: z.coerce.number().min(0).max(1).default(0),
    /** Settling window after receipt before an event may be claimed. */
    ORDERING_GRACE_MS: nonNegativeInt(2_000),
    /** Patient lease and event lock lifetime. Well above one processing call. */
    LEASE_TTL_MS: positiveInt(30_000),
    MAX_ATTEMPTS: positiveInt(5),
    RETRY_BASE_MS: positiveInt(1_000),
    RETRY_MAX_MS: positiveInt(60_000),
    /** How long shutdown waits for in-flight events before giving up. */
    SHUTDOWN_GRACE_MS: positiveInt(15_000),
    /** Poll interval floor when the worker finds nothing to claim. */
    POLL_IDLE_MS: positiveInt(250),
  })
  .refine((env) => env.LEASE_TTL_MS >= 2 * env.PROCESSING_DELAY_MS, {
    // A lock is not renewed during the call. If the call can outlive the lock,
    // every event is reclaimed mid-flight and processed twice.
    message: 'LEASE_TTL_MS must be at least twice PROCESSING_DELAY_MS',
    path: ['LEASE_TTL_MS'],
  });

export type Env = z.infer<typeof envSchema>;

/** Typed, validated ConfigService. Inject this instead of the raw one. */
export type AppConfig = ConfigService<Env, true>;

/** Passed to ConfigModule.forRoot({ validate }). Throws a readable error at boot. */
export function validateEnv(raw: Record<string, unknown>): Env {
  const result = envSchema.safeParse(raw);
  if (!result.success) {
    throw new Error(`Invalid environment:\n${z.prettifyError(result.error)}`);
  }
  return result.data;
}
