export const EVENT_STATUSES = [
  'pending',
  'processing',
  'done',
  'failed',
] as const;
export type EventStatus = (typeof EVENT_STATUSES)[number];

/**
 * One document per event. It is both the work item and the record of
 * what happened to it, so there is never a second store to reconcile.
 */
export interface EventDoc {
  /** SHA-256 of the canonical payload. Identical payloads collide on purpose. */
  _id: string;
  patientId: string;
  type: string;
  data: Record<string, unknown>;
  /** Sender's event time. Drives per-patient ordering. */
  ts: Date;
  /** Our clock at receipt. Tie-break and audit. */
  receivedAt: Date;
  status: EventStatus;
  /** Earliest time a worker may claim this event: receipt + grace, or the next retry. */
  notBefore: Date;
  /** Incremented on every claim, including re-claims after a crash. */
  attempts: number;

  /** Fencing token for the current attempt. Every outcome write must present it. */
  lockToken?: string;
  lockedBy?: string;
  lockedUntil?: Date;
  outOfOrder?: boolean;
  processedAt?: Date;
  result?: unknown;
  error?: string;
}

/** An event a worker has just claimed: the lock fields are guaranteed. */
export type ClaimedEvent = EventDoc & {
  lockToken: string;
  lockedBy: string;
  lockedUntil: Date;
};

/** What a sender gets back, whether the event is new or a duplicate. */
export interface EventReceipt {
  id: string;
  status: EventStatus;
}
