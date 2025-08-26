import { StreamQueue } from "../queue/streamQueue";

export class Reclaimer {
  queue: StreamQueue;
  consumerId: string;
  minIdleMs: number;
  intervalMs: number;

  constructor(queue: StreamQueue, consumerId: string, minIdleMs = 30000, intervalMs = 15000) {
    this.queue = queue;
    this.consumerId = consumerId;
    this.minIdleMs = minIdleMs;
    this.intervalMs = intervalMs;
  }

  start() {
    setInterval(async () => {
      try {
        const claimed = await this.queue.claimStuckEntries(this.consumerId, this.minIdleMs, 50);
        for (const c of claimed) {
          if (c.job) {

            console.log(`Reclaimed ${c.id} -> job ${c.job.id} (attempt ${c.job.attempt})`);
          } else {
            console.log(`Reclaimed ${c.id} but no message body returned`);
          }
        }
      } catch (err) {
        console.error("reclaimer err", err);
      }
    }, this.intervalMs);
  }
}
