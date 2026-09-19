# Ingest Service

Accepts patient events over HTTP, processes each one asynchronously, and persists the
outcome in MongoDB. NestJS + TypeScript.

Work in progress. The full architecture, decisions, and trade-offs land with the final commit.

## Run

```
docker compose up --build
```

## Develop

```
docker compose up -d mongo
npm ci
npm run start:dev
npm test
```
