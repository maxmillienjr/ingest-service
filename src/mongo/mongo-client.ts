import { MongoClient } from 'mongodb';
import * as os from 'node:os';

/**
 * The one place a MongoClient is built, shared by the app and the tests.
 *
 * `runtimeAdapters.os` is passed explicitly: driver 7 otherwise loads it
 * through a dynamic `import()`, which Jest's CommonJS sandbox cannot run.
 * The driver swallows that failure into empty handshake metadata and the
 * server then rejects every connection. Supplying the adapter keeps a
 * single, deterministic code path in both environments.
 */
export function createMongoClient(uri: string): MongoClient {
  return new MongoClient(uri, {
    appName: 'ingest-service',
    serverSelectionTimeoutMS: 5_000,
    runtimeAdapters: { os },
  });
}
