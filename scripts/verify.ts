/**
 * Invariant checker. Reads the manifest `load` wrote and the events
 * collection, prints one PASS/FAIL line per invariant, exits 1 on any FAIL.
 *
 *   npm run verify                 after a plain load run
 *   npm run verify -- --chaos      after a run where a worker was killed
 */
import { readFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { MongoClient } from 'mongodb';
import { DEFAULT_MANIFEST, type Manifest } from './manifest';

const { values } = parseArgs({
  options: {
    manifest: { type: 'string', default: DEFAULT_MANIFEST },
    mongo: {
      type: 'string',
      default: process.env.MONGO_URI ?? 'mongodb://localhost:27017/ingest',
    },
    /** Must match the worker's PROCESSING_DELAY_MS for the exclusivity check. */
    'delay-ms': { type: 'string', default: '5000' },
    /** A worker was killed or stopped mid-run, so reclaims are expected. */
    chaos: { type: 'boolean', default: false },
    /** Highest acceptable pending sample; default is 15 seconds of traffic. */
    'backlog-cap': { type: 'string' },
  },
});

interface Stored {
  _id: string;
  patientId: string;
  ts: Date;
  status: string;
  processedAt?: Date;
  attempts: number;
  outOfOrder?: boolean;
}

interface Check {
  name: string;
  pass: boolean;
  detail: string;
}

/** Node timers can fire a hair early against the wall clock. */
const TIMER_TOLERANCE_MS = 5;

function byPatient(docs: Stored[]): Map<string, Stored[]> {
  const groups = new Map<string, Stored[]>();
  for (const doc of docs) {
    const group = groups.get(doc.patientId) ?? [];
    group.push(doc);
    groups.set(doc.patientId, group);
  }
  return groups;
}

function checks(manifest: Manifest, stored: Stored[]): Check[] {
  const sentIds = new Set(manifest.sent.map((s) => s.id));
  const storedIds = new Set(stored.map((d) => d._id));
  const missing = [...sentIds].filter((id) => !storedIds.has(id)).length;
  const extra = [...storedIds].filter((id) => !sentIds.has(id)).length;

  const count = (status: string): number =>
    stored.filter((d) => d.status === status).length;
  const done = stored.filter((d) => d.status === 'done');
  const patients = byPatient(done);

  let orderViolations = 0;
  let late = 0;
  for (const events of patients.values()) {
    late += events.filter((e) => e.outOfOrder).length;
    const inOrder = events
      .filter((e) => !e.outOfOrder)
      .sort((a, b) => a.ts.getTime() - b.ts.getTime());
    for (let i = 1; i < inOrder.length; i += 1) {
      if (inOrder[i]!.processedAt! < inOrder[i - 1]!.processedAt!) {
        orderViolations += 1;
      }
    }
  }

  const delayMs = Number(values['delay-ms']);
  let overlaps = 0;
  for (const events of patients.values()) {
    const byEnd = [...events].sort(
      (a, b) => a.processedAt!.getTime() - b.processedAt!.getTime(),
    );
    for (let i = 1; i < byEnd.length; i += 1) {
      const gap =
        byEnd[i]!.processedAt!.getTime() - byEnd[i - 1]!.processedAt!.getTime();
      if (gap < delayMs - TIMER_TOLERANCE_MS) overlaps += 1;
    }
  }

  const reclaimed = stored.filter((d) => d.attempts > 1);
  const histogram = [...reclaimed].reduce(
    (m, d) => m.set(d.attempts, (m.get(d.attempts) ?? 0) + 1),
    new Map<number, number>(),
  );
  const histogramText =
    [...histogram.entries()]
      .sort(([a], [b]) => a - b)
      .map(([attempts, n]) => `${n} x ${attempts} attempts`)
      .join(', ') || 'none';

  const cap = values['backlog-cap']
    ? Number(values['backlog-cap'])
    : Math.round(manifest.args.rate / 4);
  const maxPending = Math.max(0, ...manifest.backlog.map((s) => s.pending));

  return [
    {
      name: 'no loss, no duplicates',
      pass: missing === 0 && extra === 0 && sentIds.size === storedIds.size,
      detail: `${sentIds.size} distinct sent (${manifest.duplicates.sent} duplicates, ${manifest.duplicates.mismatched} mismatched), ${storedIds.size} stored, ${missing} missing, ${extra} extra`,
    },
    {
      name: 'every event reached a terminal status',
      pass: count('pending') + count('processing') === 0,
      detail: `done ${done.length}, failed ${count('failed')}, pending ${count('pending')}, processing ${count('processing')}`,
    },
    {
      name: 'no terminal failures',
      pass: count('failed') === 0,
      detail: `${count('failed')} failed (expected only with FAILURE_RATE set)`,
    },
    {
      name: 'per-patient completion follows ts order',
      pass: orderViolations === 0,
      detail: `${orderViolations} violations across ${patients.size} patients; ${late} late arrivals flagged outOfOrder`,
    },
    {
      name: 'no patient processed concurrently',
      pass: overlaps === 0,
      detail: `${overlaps} overlapping processing windows (delay ${delayMs}ms, tolerance ${TIMER_TOLERANCE_MS}ms)`,
    },
    {
      name: values.chaos
        ? 'reclaims happened after the chaos step'
        : 'no reclaims without a chaos step',
      pass: values.chaos ? reclaimed.length > 0 : reclaimed.length === 0,
      detail: `${reclaimed.length} events with attempts > 1: ${histogramText}`,
    },
    {
      name: 'backlog stayed bounded',
      pass: manifest.backlog.length > 0 && maxPending <= cap,
      detail: `pending max ${maxPending}, cap ${cap}, ${manifest.backlog.length} samples`,
    },
  ];
}

async function main(): Promise<void> {
  const manifest = JSON.parse(
    await readFile(values.manifest, 'utf8'),
  ) as Manifest;
  const client = new MongoClient(values.mongo);
  let stored: Stored[];
  try {
    stored = await client
      .db()
      .collection<Stored>('events')
      .find(
        {},
        {
          projection: {
            patientId: 1,
            ts: 1,
            status: 1,
            processedAt: 1,
            attempts: 1,
            outOfOrder: 1,
          },
        },
      )
      .toArray();
  } finally {
    await client.close();
  }

  console.log(
    `verify: ${values.manifest} (${manifest.startedAt} to ${manifest.finishedAt}, ${manifest.args.rate}/min for ${manifest.args.minutes} min, ${manifest.args.patients} patients)`,
  );
  const results = checks(manifest, stored);
  const width = Math.max(...results.map((c) => c.name.length));
  for (const c of results) {
    console.log(
      `${c.pass ? 'PASS' : 'FAIL'}  ${c.name.padEnd(width)}  ${c.detail}`,
    );
  }
  const failed = results.filter((c) => !c.pass).length;
  console.log(`${results.length - failed} passed, ${failed} failed`);
  process.exitCode = failed === 0 ? 0 : 1;
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
