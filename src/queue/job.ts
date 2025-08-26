// src/queue/job.ts
import { v4 as uuidv4 } from "uuid";
import { Job } from "../lib/types";

export function makeJob(props: Partial<Job> & { type: string; payload?: object }): Job {
  const now = Date.now();
  return {
    id: props.id ?? uuidv4(),
    type: props.type,
    payload: props.payload ?? {},
    enqueuedAt: props.enqueuedAt ?? now,
    attempt: props.attempt ?? 1,
    maxAttempts: props.maxAttempts ?? 5,
    visibilityTimeoutMs: props.visibilityTimeoutMs ?? 30000,
    backoffMs: props.backoffMs ?? 1000,
    idempotencyKey: props.idempotencyKey
  } as Job;
}
