import { MongoMemoryServer } from 'mongodb-memory-server';

/**
 * Pinned to 8.2: the 8.0 line refuses to start on Linux kernels >= 6.19.
 * Override with MONGOMS_VERSION if your platform needs another build.
 */
export const MONGO_MEMORY_VERSION = process.env.MONGOMS_VERSION ?? '8.2.12';

export interface MongoMemory {
  uri: string;
  stop(): Promise<void>;
}

/** A throwaway mongod for one test suite, so `npm test` needs nothing running. */
export async function startMongoMemory(
  dbName = 'ingest-test',
): Promise<MongoMemory> {
  const server = await MongoMemoryServer.create({
    binary: { version: MONGO_MEMORY_VERSION },
  });
  return {
    uri: server.getUri(dbName),
    stop: async (): Promise<void> => {
      await server.stop();
    },
  };
}
