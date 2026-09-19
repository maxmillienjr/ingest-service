import { Inject, Logger, Module, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Db, MongoClient } from 'mongodb';
import type { AppConfig } from '../config/env.schema';
import { createMongoClient } from './mongo-client';
import { MONGO_CLIENT, MONGO_DB } from './mongo.tokens';

/**
 * One MongoClient for the process, connected before the app starts serving
 * and closed on shutdown. Repositories inject MONGO_DB; nothing else in the
 * codebase should touch the driver directly.
 */
@Module({
  providers: [
    {
      provide: MONGO_CLIENT,
      inject: [ConfigService],
      useFactory: async (config: AppConfig): Promise<MongoClient> => {
        const client = createMongoClient(
          config.get('MONGO_URI', { infer: true }),
        );
        // Connect eagerly: a bad URI fails the boot, not the first request.
        await client.connect();
        return client;
      },
    },
    {
      provide: MONGO_DB,
      inject: [MONGO_CLIENT],
      useFactory: (client: MongoClient): Db => client.db(),
    },
  ],
  exports: [MONGO_CLIENT, MONGO_DB],
})
export class MongoModule implements OnApplicationShutdown {
  private readonly logger = new Logger(MongoModule.name);

  constructor(@Inject(MONGO_CLIENT) private readonly client: MongoClient) {}

  async onApplicationShutdown(): Promise<void> {
    await this.client.close();
    this.logger.log('Mongo client closed');
  }
}
