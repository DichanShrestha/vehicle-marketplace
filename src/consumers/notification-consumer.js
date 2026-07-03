import "dotenv/config";
import { prisma } from "../utils/prisma.js";
import kafka from "../utils/kafka.js";
import { Prisma } from "@prisma/client";

const consumer = kafka.consumer({ groupId: "notification-consumer" });
const producer = kafka.producer();

function sendEmail(email) {
  if (email === "fail@test.com") throw new Error("SMTP timeout");
}

async function start() {
  try {
    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({
      topics: ["notifications.send"],
    });

    await consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;

        const payload = JSON.parse(message.value.toString());
        const { eventId, userId, email } = payload.event;
        const currentAttempt = payload.failure?.attemptCount ?? 0;

        let record;
        try {
          record = await prisma.processedNotification.create({
            data: { eventId, status: "pending", attemptCount: currentAttempt },
          });
        } catch (error) {
          if (
            error instanceof Prisma.PrismaClientKnownRequestError &&
            error.code === "P2002"
          ) {
            record = await prisma.processedNotification.findUnique({
              where: { eventId },
            });
            if (record.status === "sent") {
              console.log(`Event ${eventId} already sent. Skipping.`);
              return;
            }
          } else {
            throw error;
          }
        }

        try {
          sendEmail(email);
          await prisma.processedNotification.update({
            where: { eventId },
            data: { status: "sent" },
          });
          console.log("email sent successfully");
        } catch (error) {
          await prisma.processedNotification.update({
            where: { eventId },
            data: { status: "failed", attemptCount: currentAttempt + 1 },
          });

          await producer.send({
            topic: "notifications.dlq",
            messages: [
              {
                value: JSON.stringify({
                  event: { eventId, email, userId },
                  failure: {
                    reason: error.message,
                    attemptCount: currentAttempt + 1,
                    originalTopic: "notifications.send",
                    timestamp: new Date().toISOString(),
                  },
                }),
              },
            ],
          });
          console.log("email sent to dlq for retries");
        }
      },
    });
  } catch (error) {
    console.log("Notification Consumer failed: ", error);
  }
}

start();
