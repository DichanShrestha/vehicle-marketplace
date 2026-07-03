import kafka from "../utils/kafka.js";

const producer = kafka.producer();

async function main() {
  await producer.connect();

  await producer.send({
    topic: "bookings.created",
    messages: [
      {
        key: "vehicle-1",
        value: JSON.stringify({
          eventId: "evt-4",
          vehicleId: "vehicle-4",
          userId: "user-4",
          createdAt: new Date().toISOString(),
        }),
      },
    ],
  });

  console.log("Transaction event sent");

  await producer.disconnect();
}

main().catch(console.error);
