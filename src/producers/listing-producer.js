import kafka from "../utils/kafka.js";

const producer = kafka.producer();

async function main() {
  await producer.connect();

  await producer.send({
    topic: "listings.price.updated",
    messages: [
      {
        key: "vehicle-1",
        value: JSON.stringify({
          vehicleId: "vehicle-1",
          price: 30000,
          version: 3,
        }),
      },
      {
        key: "vehicle-1",
        value: JSON.stringify({
          vehicleId: "vehicle-1",
          price: 10000,
          version: 1,
        }),
      },
      {
        key: "vehicle-1",
        value: JSON.stringify({
          vehicleId: "vehicle-1",
          price: 20000,
          version: 2,
        }),
      },
    ],
  });

  console.log("Listing event sent");

  await producer.disconnect();
}

main().catch(console.error);
