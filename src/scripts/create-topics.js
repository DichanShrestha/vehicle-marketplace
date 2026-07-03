import kafka from "../utils/kafka.js";

async function createTopics() {
  const admin = kafka.admin();

  try {
    await admin.connect();
    console.log("Admin connected successfully");

    const topicsToCreate = [
      { topic: "bookings.created", numPartitions: 1 },
      { topic: "notifications.send", numPartitions: 1 },
      { topic: "notifications.dlq", numPartitions: 1 },
      { topic: "listings.price.updated", numPartitions: 1 },
    ];

    const success = await admin.createTopics({
      topics: topicsToCreate,
    });

    if (success) {
      console.log("Topics created successfully");
    } else {
      console.log("Topics already exist or creation failed");
    }
  } catch (error) {
    console.error("Error creating topics", error);
  } finally {
    await admin.disconnect();
  }
}

createTopics();
