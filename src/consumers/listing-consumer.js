import "dotenv/config";
import { prisma } from "../utils/prisma.js";
import kafka from "../utils/kafka.js";
import { Prisma } from "@prisma/client";

const consumer = kafka.consumer({ groupId: "listings-price-updated" });

async function start() {
  try {
    await consumer.connect();

    await consumer.subscribe({
      topics: ["listings.price.updated"],
    });

    await consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;

        const { price, version, vehicleId } = JSON.parse(
          message.value.toString(),
        );

        const { count } = await prisma.listing.updateMany({
          where: {
            vehicleId,
            version: {
              lt: version,
            },
          },
          data: {
            price,
            version,
          },
        });

        if (count === 1) {
          console.log("Listing updated successfully");
        } else if (count === 0) {
          const existingListing = await prisma.listing.findUnique({
            where: {
              vehicleId,
            },
          });

          if (!existingListing) {
            try {
              await prisma.listing.create({
                data: {
                  vehicleId,
                  price,
                  version,
                },
              });
            } catch (error) {
              if (
                error instanceof Prisma.PrismaClientKnownRequestError &&
                error.code === "P2002"
              ) {
                console.log("Another consumer created the listing.");
                return;
              }

              throw error;
            }
            console.log("Listing created");
          } else {
            console.log("Ignoring the event...");
          }
        }
      },
    });
  } catch (error) {
    console.log("Listing Consumer failed: ", error);
  }
}

start();
