import kafka from "../utils/kafka.js";

const producer = kafka.producer();

async function main() {
  await producer.connect();

  await producer.send({
    topic: "notifications.send",
    messages: [
      {
        key: "notification-1",
        value: JSON.stringify({
          event: {
            eventId: "evt-3",
            userId: "user-2",
            email: "fail@test.com",
          },
        }),
      },
    ],
  });

  console.log("Notification event sent");

  await producer.disconnect();
}

main().catch(console.error);
