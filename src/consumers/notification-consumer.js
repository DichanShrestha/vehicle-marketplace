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
        const { event } = payload;

        const { eventId, userId, email } = event;

        const currentAttempt = payload.failure?.attemptCount ?? 0;

        try {
          sendEmail(email);

          try {
            await prisma.processedNotification.create({
              data: {
                eventId,
              },
            });
          } catch (error) {
            if (
              error instanceof Prisma.PrismaClientKnownRequestError &&
              error.code === "P2002"
            ) {
              console.log(`Duplicate event ${eventId}. Skipping...`);
              return;
            }

            throw error;
          }

          console.log("email sent successfully");
        } catch (error) {
          if (error instanceof Error) {
            const dlqMessage = {
              event: {
                eventId,
                email,
                userId,
              },
              failure: {
                reason: error.message,
                attemptCount: currentAttempt + 1,
                originalTopic: "notifications.send",
                timestamp: new Date().toISOString(),
              },
            };

            await producer.send({
              topic: "notifications.dlq",
              messages: [
                {
                  value: JSON.stringify(dlqMessage),
                },
              ],
            });

            console.log("email sent to dlq for retries");
          }
        }
      },
    });
  } catch (error) {
    console.log("Notification Consumer failed: ", error);
  }
}

start();
