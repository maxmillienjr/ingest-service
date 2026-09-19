/** What `load` sent, written for `verify` to check against the database. */
export interface Manifest {
  startedAt: string;
  finishedAt: string;
  args: {
    url: string;
    rate: number;
    minutes: number;
    patients: number;
    dup: number;
    shuffle: number;
  };
  /** One entry per distinct payload, with the id the API handed back. */
  sent: { id: string; patientId: string; ts: string }[];
  /** Exact re-sends of earlier payloads, and how many came back with a different id (should be 0). */
  duplicates: { sent: number; mismatched: number };
  /** `/health` backlog samples taken during the run and the drain that follows it. */
  backlog: { at: string; pending: number; processing: number }[];
}

export const DEFAULT_MANIFEST = 'load-manifest.json';
