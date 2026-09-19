import { createHash } from 'node:crypto';

/**
 * JSON with object keys sorted at every depth, so two payloads that differ
 * only in key order serialize identically. Arrays keep their order: for a
 * sender, [a, b] and [b, a] are different data.
 */
export function stableStringify(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value) ?? 'null';
  }
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(',')}]`;
  }
  const record = value as Record<string, unknown>;
  const fields = Object.keys(record)
    .sort()
    .filter((key) => record[key] !== undefined)
    .map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`);
  return `{${fields.join(',')}}`;
}

export interface EventIdentity {
  patientId: string;
  type: string;
  ts: Date;
  data: Record<string, unknown>;
}

/**
 * The event's identity is its content. Senders we do not control retry
 * without an id, so the id is derived from what they sent: same patient,
 * type, instant and data means the same event. `ts` is normalized to epoch
 * milliseconds so "…Z" and "…+00:00" spellings of one instant agree.
 */
export function canonicalEventId(event: EventIdentity): string {
  const canonical = stableStringify({
    patientId: event.patientId,
    type: event.type,
    ts: event.ts.getTime(),
    data: event.data,
  });
  return createHash('sha256').update(canonical).digest('hex');
}
