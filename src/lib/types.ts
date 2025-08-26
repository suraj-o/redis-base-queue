export type JobPayload = Record<string, unknown>;

export interface Job {
  id: string;
  type: string;
  payload: JobPayload;
  enqueuedAt: number;
  attempt: number;
  maxAttempts: number;
  visibilityTimeoutMs: number;
  backoffMs?: number;
  idempotencyKey?: string;
}
