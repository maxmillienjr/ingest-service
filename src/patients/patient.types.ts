/**
 * Per-patient coordination, never an outcome. One document per patient
 * that has ever had an event, holding the worker lease that serializes
 * that patient's processing and the ordering watermark.
 */
export interface PatientDoc {
  _id: string;
  leaseOwner?: string;
  leaseUntil?: Date;
  /** The newest `ts` applied so far. An event below it is out of order. */
  lastAppliedTs?: Date;
}
