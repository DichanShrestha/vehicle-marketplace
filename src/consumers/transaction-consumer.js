import "dotenv/config";
import { prisma } from "../utils/prisma.js";
import kafka from "../utils/kafka.js";
import { Prisma } from "@prisma/client";

const consumer = kafka.consumer({ groupId: "transaction-consumer" });

async function start() {
  try {
    console.log("Kafka connecting..");

    await consumer.connect();
    console.log("Kafka connected..");

    await consumer.subscribe({
      topics: ["bookings.created"],
    });

    await consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;

        const { eventId, vehicleId, userId, createdAt } = JSON.parse(
          message.value.toString(),
        );

        // saving to db
        try {
          await prisma.$transaction(async (tx) => {
            const existing = await tx.processedEvent.findUnique({
              where: {
                eventId,
              },
            });

            // checking if the event already exists
            if (existing) {
              console.log(`Duplicate event ${eventId}. Skipping.`);
              return;
            }

            try {
              await tx.processedEvent.create({
                data: { eventId },
              });
            } catch (error) {
              if (
                // Another consumer may have inserted the same event after our findUnique()
                // Handle that race condition by treating P2002 as a duplicate delivery.
                error instanceof Prisma.PrismaClientKnownRequestError &&
                error.code === "P2002"
              ) {
                console.log(`Duplicate event ${eventId}. Skipping.`);
                return;
              }

              throw error;
            }

            await tx.booking.create({
              data: {
                eventId,
                vehicleId,
                userId,
              },
            });
            console.log(
              `Booking created successfully for vehicle ${vehicleId}`,
            );
          });
        } catch (error) {
          console.error(`Failed processing event ${eventId}`, error);
        }
      },
    });
  } catch (error) {
    console.error("Transaction Consumer failed:", error);
  }
}

start();
