import { connectRedis } from "../lib/redisClient";
import { StreamQueue } from "../queue/streamQueue";
import { Worker } from "../worker/worker";
import { Reclaimer } from "../worker/reclaimer";

async function main() {
  await connectRedis();
  const q = new StreamQueue("emails", "email-group");
  const worker = new Worker(q, "consumer-1", 2);

  worker.register("send_email", async (job) => {
    console.log("processing job", job.id, "attempt", job.attempt, "payload:", job.payload);
    if (Math.random() < 0.4) {
      throw new Error("simulated transient failure");
    }
    await new Promise((r) => setTimeout(r, 500));
    console.log("sent", job.payload);
  });

  const reclaimer = new Reclaimer(q, "consumer-1", 30_000, 15_000);
  reclaimer.start();

  await worker.runLoop();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
