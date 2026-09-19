import { MongoServerError } from 'mongodb';

const DUPLICATE_KEY = 11000;

/** A unique-index collision. Several writes in this service rely on it on purpose. */
export function isDuplicateKey(err: unknown): boolean {
  return err instanceof MongoServerError && err.code === DUPLICATE_KEY;
}
