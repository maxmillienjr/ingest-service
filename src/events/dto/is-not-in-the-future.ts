import { buildMessage, ValidateBy } from 'class-validator';

/**
 * How far ahead of our clock a sender's `ts` may be. Covers clock skew
 * between systems; rejects the mistake that matters. One far-future
 * timestamp would become its patient's ordering watermark and flag every
 * later event as out of order, permanently.
 */
export const MAX_TS_SKEW_MS = 5 * 60_000;

export function IsNotInTheFuture(skewMs = MAX_TS_SKEW_MS): PropertyDecorator {
  return ValidateBy({
    name: 'isNotInTheFuture',
    validator: {
      validate: (value: unknown): boolean =>
        typeof value === 'string' && Date.parse(value) <= Date.now() + skewMs,
      defaultMessage: buildMessage(
        (prefix) =>
          `${prefix}$property must not be more than ${skewMs}ms in the future`,
      ),
    },
  });
}
