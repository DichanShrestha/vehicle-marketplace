-- CreateTable
CREATE TABLE "listings" (
    "id" TEXT NOT NULL,
    "vehicleId" TEXT NOT NULL,
    "price" INTEGER NOT NULL,
    "version" INTEGER NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "listings_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "listings_vehicleId_key" ON "listings"("vehicleId");
