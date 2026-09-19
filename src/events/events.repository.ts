import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { Collection, Db, Filter } from 'mongodb';
import { randomUUID } from 'node:crypto';
import { isDuplicateKey } from '../mongo/mongo-errors';
import { MONGO_DB } from '../mongo/mongo.tokens';
import {
  EVENT_STATUSES,
  type ClaimedEvent,
  type EventDoc,
  type EventStatus,
  type StatusCounts,
} from './event.types';

export interface Outcome {
  result: unknown;
  outOfOrder: boolean;
}

/** What claiming a patient's head event can come back with. */
export type HeadClaim =
  | { kind: 'claimed'; event: ClaimedEvent }
  /** The next event exists but is not claimable until `until` (grace, retry backoff, or a live lock). */
  | { kind: 'blocked'; until: Date }
  | { kind: 'empty' };

/**
 * Work a worker may take right now: never claimed, or claimed by an
 * attempt whose lock has expired. That second arm is the whole crash
 * recovery story; there is no sweeper.
 */
const claimable = (now: Date): Filter<EventDoc> => ({
  $or: [
    { status: 'pending', notBefore: { $lte: now } },
    { status: 'processing', lockedUntil: { $lte: now } },
  ],
});

/** Every query against the events collection lives here. */
@Injectable()
export class EventsRepository implements OnModuleInit {
  private readonly events: Collection<EventDoc>;

  constructor(@Inject(MONGO_DB) db: Db) {
    this.events = db.collection<EventDoc>('events');
  }

  async onModuleInit(): Promise<void> {
    await this.events.createIndexes([
      // Candidate scan, both arms of `claimable`.
      { key: { status: 1, notBefore: 1 } },
      { key: { status: 1, lockedUntil: 1 } },
      // Per-patient head: the next event to apply for one patient.
      { key: { patientId: 1, status: 1, ts: 1, receivedAt: 1 } },
    ]);
  }

  /**
   * The insert is the dedupe: `_id` is the content hash, so a retry of the
   * same payload hits the unique index and we hand back what is already there.
   */
  async insertIfAbsent(
    doc: EventDoc,
  ): Promise<{ event: EventDoc; created: boolean }> {
    try {
      await this.events.insertOne(doc);
      return { event: doc, created: true };
    } catch (err) {
      if (!isDuplicateKey(err)) throw err;
      const existing = await this.events.findOne({ _id: doc._id });
      if (!existing) throw err;
      return { event: existing, created: false };
    }
  }

  findById(id: string): Promise<EventDoc | null> {
    return this.events.findOne({ _id: id });
  }

  /** How many events sit in each status. Every status is present, zero or not. */
  async countByStatus(): Promise<StatusCounts> {
    const rows = await this.events
      .aggregate<{ _id: EventStatus; n: number }>([
        { $group: { _id: '$status', n: { $sum: 1 } } },
      ])
      .toArray();
    const counts = Object.fromEntries(
      EVENT_STATUSES.map((status) => [status, 0]),
    ) as StatusCounts;
    for (const row of rows) counts[row._id] = row.n;
    return counts;
  }

  /**
   * Patients that have something claimable, oldest work first. Callers
   * lease a patient before touching its events, so this is only a hint.
   */
  async claimablePatients(limit: number, now = new Date()): Promise<string[]> {
    const docs = await this.events
      .find(claimable(now), {
        projection: { patientId: 1 },
        sort: { notBefore: 1 },
        limit,
      })
      .toArray();
    return [...new Set(docs.map((doc) => doc.patientId))];
  }

  /**
   * Takes one patient's next event in `ts` order (receipt order breaks
   * ties). The head is the smallest-ts event that is not finished, ready or
   * not: an event waiting out its grace window, its retry backoff, or a
   * still-live lock holds the line, so a hiccup cannot reorder a patient.
   *
   * Only the patient's lease holder calls this, which is what makes the
   * read-then-update safe. The update is still conditional, so a stale
   * attempt cannot take a document out from under a live one.
   */
  async claimHead(
    patientId: string,
    workerId: string,
    lockTtlMs: number,
    now = new Date(),
  ): Promise<HeadClaim> {
    const head = await this.events.findOne(
      { patientId, status: { $in: ['pending', 'processing'] } },
      {
        sort: { ts: 1, receivedAt: 1 },
        projection: { _id: 1, status: 1, notBefore: 1, lockedUntil: 1 },
      },
    );
    if (!head) return { kind: 'empty' };

    const readyAt =
      head.status === 'pending' ? head.notBefore : head.lockedUntil;
    if (readyAt && readyAt > now) return { kind: 'blocked', until: readyAt };

    const claimed = await this.events.findOneAndUpdate(
      { _id: head._id, ...claimable(now) },
      {
        $set: {
          status: 'processing',
          lockToken: randomUUID(),
          lockedBy: workerId,
          lockedUntil: new Date(now.getTime() + lockTtlMs),
        },
        $inc: { attempts: 1 },
      },
      { returnDocument: 'after' },
    );
    return claimed
      ? { kind: 'claimed', event: claimed as ClaimedEvent }
      : { kind: 'empty' };
  }

  /** Records success, but only for the attempt that still holds the lock. */
  async complete(
    id: string,
    lockToken: string,
    outcome: Outcome,
    now = new Date(),
  ): Promise<boolean> {
    const res = await this.events.updateOne(
      { _id: id, lockToken },
      {
        $set: {
          status: 'done',
          result: outcome.result,
          outOfOrder: outcome.outOfOrder,
          processedAt: now,
        },
        $unset: { lockToken: '', lockedBy: '', lockedUntil: '' },
      },
    );
    return res.matchedCount === 1;
  }

  /** Puts a failed attempt back in line for a later try. Fenced. */
  async retryLater(
    id: string,
    lockToken: string,
    error: string,
    notBefore: Date,
  ): Promise<boolean> {
    const res = await this.events.updateOne(
      { _id: id, lockToken },
      {
        $set: { status: 'pending', notBefore, error },
        $unset: { lockToken: '', lockedBy: '', lockedUntil: '' },
      },
    );
    return res.matchedCount === 1;
  }

  /** Records a terminal failure. Fenced. */
  async fail(
    id: string,
    lockToken: string,
    error: string,
    now = new Date(),
  ): Promise<boolean> {
    const res = await this.events.updateOne(
      { _id: id, lockToken },
      {
        $set: { status: 'failed', error, processedAt: now },
        $unset: { lockToken: '', lockedBy: '', lockedUntil: '' },
      },
    );
    return res.matchedCount === 1;
  }
}
