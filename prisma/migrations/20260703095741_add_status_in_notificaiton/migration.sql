/*
  Warnings:

  - Added the required column `status` to the `processed_notifications` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "processed_notifications" ADD COLUMN     "status" TEXT NOT NULL;
