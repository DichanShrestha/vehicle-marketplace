import kafka from "../utils/kafka.js";

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "notifications-dlq" });

async function main() {
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({
    topics: ["notifications.dlq"],
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const { failure, event } = JSON.parse(message.value.toString());
      const { attemptCount } = failure;

      if (attemptCount >= 3) {
        console.log("Maximum retry attempts reached. Skipping replay.");
        return;
      }

      await producer.send({
        topic: "notifications.send",
        messages: [
          {
            key: "notification-1",
            value: JSON.stringify({
              event,
              failure: {
                reason: failure.reason,
                attemptCount: attemptCount + 1,
                originalTopic: failure.originalTopic,
                timestamp: failure.timestamp,
              },
            }),
          },
        ],
      });

      console.log("Notification event sent for retry");
    },
  });
}

main().catch(console.error);
