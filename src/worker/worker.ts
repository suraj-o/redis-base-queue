import { StreamQueue } from "../queue/streamQueue";
import { Job } from "../lib/types";

type Handler = (job: Job) => Promise<void>;

export class Worker {
  queue: StreamQueue;
  consumerId: string;
  handlers: Map<string, Handler>;
  maxBatch: number;
  visibilityMs: number;

  constructor(queue: StreamQueue, consumerId: string, maxBatch = 1) {
    this.queue = queue;
    this.consumerId = consumerId;
    this.handlers = new Map();
    this.maxBatch = maxBatch;
    this.visibilityMs = 30000;
  }

  register(jobType: string, handler: Handler) {
    this.handlers.set(jobType, handler);
  }

  async runLoop() {
    await this.queue.ensureGroup();
    while (true) {
      try {
        const messages = await this.queue.readGroup(this.consumerId, this.maxBatch, 5000);
        if (messages.length === 0) {
          continue;
        }
        for (const msg of messages) {
          const { id, job } = msg;
          const h = this.handlers.get(job.type);
          if (!h) {
            console.warn(`no handler for ${job.type}. acking and moving to DLQ`);
            await this.queue.toDLQ(id, job);
            await this.queue.ack(id);
            continue;
          }
          try {
            await h(job);
            await this.queue.ack(id);
          } catch (err: any) {
            console.error(`handler error for job ${job.id}:`, err?.message ?? err);
            job.attempt = (job.attempt ?? 1) + 1;
            if (job.attempt > job.maxAttempts) {
              console.log(`max attempts exceeded for ${job.id}, moving to DLQ`);
              await this.queue.toDLQ(id, job);
              await this.queue.ack(id);
            } else {
              const backoffMs = job.backoffMs ?? 1000;
              const delay = backoffMs * Math.pow(2, job.attempt - 1);
              setTimeout(async () => {
                await this.queue.retry(job);
              }, Math.min(delay, 60_000));
              await this.queue.ack(id);
            }
          }
        }
      } catch (err) {
        console.error("worker read/processing error", err);
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  }
}
