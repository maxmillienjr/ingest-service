import { Inject, Injectable } from '@nestjs/common';
import { Collection, Db } from 'mongodb';
import { isDuplicateKey } from '../mongo/mongo-errors';
import { MONGO_DB } from '../mongo/mongo.tokens';
import type { PatientDoc } from './patient.types';

/**
 * The lease is what makes "one patient, one worker at a time" hold across
 * any number of instances. Its correctness rests on a single document per
 * patient that every competing worker must write through.
 */
@Injectable()
export class PatientsRepository {
  private readonly patients: Collection<PatientDoc>;

  constructor(@Inject(MONGO_DB) db: Db) {
    this.patients = db.collection<PatientDoc>('patients');
  }

  /**
   * Atomic acquire. If the patient has no live lease the upsert takes it;
   * if it has one, the filter does not match, the upsert tries to insert a
   * second document with the same _id, and the unique index says no. That
   * duplicate-key error is the "held by someone else" signal.
   */
  async tryLease(
    patientId: string,
    workerId: string,
    ttlMs: number,
    now = new Date(),
  ): Promise<boolean> {
    try {
      await this.patients.updateOne(
        {
          _id: patientId,
          $or: [
            { leaseUntil: { $exists: false } },
            { leaseUntil: { $lte: now } },
          ],
        },
        {
          $set: {
            leaseOwner: workerId,
            leaseUntil: new Date(now.getTime() + ttlMs),
          },
        },
        { upsert: true },
      );
      return true;
    } catch (err) {
      if (isDuplicateKey(err)) return false;
      throw err;
    }
  }

  /** Extends a lease this worker still holds. False means it was lost. */
  async renewLease(
    patientId: string,
    workerId: string,
    ttlMs: number,
    now = new Date(),
  ): Promise<boolean> {
    const res = await this.patients.updateOne(
      { _id: patientId, leaseOwner: workerId },
      { $set: { leaseUntil: new Date(now.getTime() + ttlMs) } },
    );
    return res.matchedCount === 1;
  }

  /** Lets the lease go, but only if this worker is still the owner. */
  async releaseLease(patientId: string, workerId: string): Promise<void> {
    await this.patients.updateOne(
      { _id: patientId, leaseOwner: workerId },
      { $unset: { leaseOwner: '', leaseUntil: '' } },
    );
  }

  /**
   * Moves the watermark forward to `ts` if it is newer, and returns what
   * it was before, so the caller can tell whether `ts` fell behind it.
   */
  async advanceWatermark(patientId: string, ts: Date): Promise<Date | null> {
    const before = await this.patients.findOneAndUpdate(
      { _id: patientId },
      { $max: { lastAppliedTs: ts } },
      { upsert: true, returnDocument: 'before' },
    );
    return before?.lastAppliedTs ?? null;
  }
}
