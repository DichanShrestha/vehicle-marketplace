-- CreateTable
CREATE TABLE "processed_notifications" (
    "eventId" TEXT NOT NULL,
    "processedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "processed_notifications_pkey" PRIMARY KEY ("eventId")
);
