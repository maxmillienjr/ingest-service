import { MongoBinary } from 'mongodb-memory-server';
import { MONGO_MEMORY_VERSION } from './mongo-memory';

/**
 * Runs once before any suite. Downloads the mongod binary if it is not
 * cached yet, so a first run on a fresh clone or in CI does not spend a
 * suite's timeout on the download.
 */
export default async function globalSetup(): Promise<void> {
  await MongoBinary.getPath({ version: MONGO_MEMORY_VERSION });
}
