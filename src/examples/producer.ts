import { connectRedis, redis } from "../lib/redisClient";
import { StreamQueue } from "../queue/streamQueue";
import { makeJob } from "../queue/job";

async function main() {
  await connectRedis();
  const q = new StreamQueue("emails");
  await q.ensureGroup();

  for (let i = 0; i < 10; i++) {
    const job = makeJob({
      type: "send_email",
      payload: { to: `user+${i}@example.com`, subject: "Welcome", body: `hello ${i}` },
      maxAttempts: 4,
      backoffMs: 1000
    });
    await q.enqueue(job);
    console.log("enqueued", job.id);
  }

  await redis.quit();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
