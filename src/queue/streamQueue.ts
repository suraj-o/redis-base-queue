import { redis } from "../lib/redisClient";
import { Job } from "../lib/types";

const STREAM_PREFIX = "djq";
const DEFAULT_GROUP = "workers";

function streamName(queueName: string) { return `${STREAM_PREFIX}:${queueName}`; }
function dlqName(queueName: string) { return `${STREAM_PREFIX}:${queueName}:dlq`; }

export class StreamQueue {
  queueName: string;
  stream: string;
  group: string;

  constructor(queueName: string, group = DEFAULT_GROUP) {
    this.queueName = queueName;
    this.stream = streamName(queueName);
    this.group = group;
  }

  async ensureGroup() {
    try {
      await redis.xGroupCreate(this.stream, this.group, "0", { MKSTREAM: true });
    } catch (err: any) {
      if (!String(err).includes("BUSYGROUP")) throw err;
    }
  }

  async enqueue(job: Job) {
    const fields = {
      data: JSON.stringify(job),
      type: job.type,
      enqueuedAt: String(job.enqueuedAt)
    };
    await redis.xAdd(this.stream, "*", fields); 
  }

  async toDLQ(queueEntryId: string, job: Job) {
    await redis.xAdd(dlqName(this.queueName), "*", {
      originalId: queueEntryId,
      job: JSON.stringify(job),
      movedAt: String(Date.now())
    });
  }

  async readGroup(consumer: string, count = 1, block = 5000) {
    const res = await redis.xReadGroup(this.group, consumer, [{ key: this.stream, id: ">" }], { COUNT: count, BLOCK: block });
    if (!res) return [];
    const out: { id: string; job: Job }[] = [];
    for (const stream of res) {
      for (const msg of stream.messages) {
        const raw = msg.message.data as string;
        const job: Job = JSON.parse(raw);
        out.push({ id: msg.id, job });
      }
    }
    return out;
  }

  async ack(id: string) {
    await redis.xAck(this.stream, this.group, id);
  }

  async retry(job: Job) {
    await this.enqueue(job);
  }

  async claimStuckEntries(consumer: string, minIdleMs: number, count = 10) {
  const pending = await redis.xPendingRange(
    this.stream,
    this.group,
    "-",   // start
    "+",   // end
    count
  );
    const claimed: { id: string; job?: Job }[] = [];
    for (const p of pending) {
      const [id, entryConsumer, idleMsStr] = p as any;
      const idleMs = Number(p.millisecondsSinceLastDelivery);
      if (idleMs >= minIdleMs) {
        const msgs = await redis.xClaim(this.stream, this.group, consumer, minIdleMs, [id], { IDLE: idleMs });
        if (msgs && msgs.length > 0) {
          const raw = (msgs as any)[0].message.data as string;
          const job: Job = JSON.parse(raw);
          claimed.push({ id, job });
        } else {
          claimed.push({ id });
        }
      }
    }
    return claimed;
  }

  async pendingSummary() {
    return redis.xPending(this.stream, this.group);
  }
}
