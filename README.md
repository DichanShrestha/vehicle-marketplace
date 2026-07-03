# Vehicle Marketplace

A Node.js project for a Kafka-based backend assessment.

## Prerequisites

- Node.js
- Docker & Docker Compose
- npm

## Installation

1. Install dependencies:
   ```bash
   npm install
   ```
2. Set up environment variables:
   ```bash
   cp .env.example .env
   ```
   (Adjust variables if necessary).

## Docker Compose Up

Start Redpanda and PostgreSQL:

```bash
docker compose up -d
```

## Prisma Generate

Generate the Prisma client (after adding models to `schema.prisma`):

```bash
npx prisma generate
```

## Create Kafka Topics

Create the required Kafka topics:

```bash
npm run topics
```

## Available npm Scripts

- `npm run dev`: Run nodemon
- `npm run topics`: Create Kafka topics
- `npm run transaction`: Start the transaction consumer
- `npm run notification`: Start the notification consumer
- `npm run listing`: Start the listing consumer
- `npm run replay`: Run the DLQ replay producer

# Task 1 – Idempotent Kafka Consumer

## Overview

This consumer listens to the `bookings.created` Kafka topic and ensures that the same booking event is never processed more than once, even if Kafka delivers the event multiple times.

## Database Schema

### `processed_events`

Stores every successfully processed event.

| Column        | Description                            |
| ------------- | -------------------------------------- |
| `eventId`     | Unique event identifier (Primary Key)  |
| `processedAt` | Timestamp when the event was processed |

### `bookings`

Stores vehicle bookings.

| Column      | Description                   |
| ----------- | ----------------------------- |
| `id`        | Booking ID                    |
| `eventId`   | Kafka event ID                |
| `vehicleId` | Vehicle being booked (Unique) |
| `userId`    | User who booked the vehicle   |
| `createdAt` | Booking creation timestamp    |

## Idempotency Approach

When a message is received from the `bookings.created` topic:

1. The consumer starts a database transaction.
2. It checks whether the `eventId` already exists in the `processed_events` table.
3. If the event has already been processed, it logs the duplicate and skips processing.
4. If the event is new, it inserts the `eventId` into `processed_events`.
5. It then creates the booking in the `bookings` table.
6. Both operations are executed inside a single database transaction to ensure atomicity.

This guarantees that:

- Duplicate Kafka deliveries do not create duplicate bookings.
- If booking creation fails, the processed event record is rolled back automatically.
- A vehicle cannot be booked twice because `vehicleId` has a unique constraint.

## Race Condition Handling

Two consumers may receive the same event simultaneously. To handle this safely:

- The consumer first checks whether the event already exists.
- A unique constraint on `eventId` provides a second layer of protection.
- If another consumer inserts the same event between the existence check and the insert, Prisma throws a `P2002` unique constraint error.
- The consumer catches this error, logs it as a duplicate event, and exits without crashing.

## Testing Performed

The following scenarios were tested:

- Successful processing of a new booking event.
- Duplicate delivery of the same event (same `eventId`) is skipped.
- Attempting to book an already booked vehicle with a different `eventId` fails because of the unique constraint on `vehicleId`.
- Database transactions roll back correctly when booking creation fails.

# Task 2 – Dead Letter Queue (DLQ) Handler

## Overview

The Notification Service consumes messages from the `notifications.send` Kafka topic. If email delivery fails (for example, due to an SMTP timeout), the original event is not lost. Instead, it is published to a dedicated Dead Letter Queue (`notifications.dlq`) along with structured failure metadata. A separate replay script can later consume these failed events and republish them to the original topic.

## Failure Flow

```
notifications.send
        │
        ▼
Notification Consumer
        │
        ├── Success
        │      ▼
        │  Email Sent
        │
        └── Failure
               ▼
      notifications.dlq
               │
               ▼
        DLQ Replay Script
               │
               ▼
      notifications.send
```

## DLQ Message Structure

Each failed notification is published to the DLQ in the following format:

```json
{
  "event": {
    "eventId": "evt-100",
    "userId": "user-1",
    "email": "john@example.com"
  },
  "failure": {
    "reason": "SMTP timeout",
    "attemptCount": 1,
    "originalTopic": "notifications.send",
    "timestamp": "2026-07-03T04:30:00Z"
  }
}
```

## Failure Metadata

Each DLQ message contains:

| Field           | Description                                   |
| --------------- | --------------------------------------------- |
| `reason`        | Reason the notification failed                |
| `attemptCount`  | Number of delivery attempts                   |
| `originalTopic` | Kafka topic from which the message originated |
| `timestamp`     | Time when the failure occurred                |

## Replay Process

The replay script subscribes to the `notifications.dlq` topic and republishes failed events back to the original `notifications.send` topic.

For every replay:

1. The current retry count is read.
2. If the retry count has reached the configured maximum (3), the event is skipped.
3. Otherwise, the retry count is incremented.
4. The event is republished to `notifications.send`.

This prevents infinite retry loops while allowing transient failures to be retried.

## Idempotency

Notification processing is idempotent using the `processed_notifications` table.

Before recording a successful notification, the consumer checks whether the `eventId` has already been processed. If the same event is replayed multiple times, duplicate deliveries are skipped.

This guarantees that replaying the DLQ multiple times does not result in the same notification being processed more than once.

## Testing Performed

The following scenarios were verified:

- Successful notification delivery.
- Failed notification is published to `notifications.dlq`.
- Failure metadata is correctly attached to the DLQ message.
- Replay republishes failed events to `notifications.send`.
- Retry count increases after each replay.
- Replay stops after three failed attempts.
- Duplicate notification events are skipped through idempotency.

# Task 3 – Event Ordering & Version Guard

## Overview

The Listing Service consumes events from the `listings.price.updated` Kafka topic. Since Kafka events may arrive out of order due to retries or consumer rebalancing, the consumer ensures that only the latest version of a listing is stored.

Each event contains:

- `vehicleId`
- `price`
- `version`

The `version` is a monotonically increasing integer for each vehicle.

---

## Database Schema

### `listings`

| Column      | Description                 |
| ----------- | --------------------------- |
| `id`        | Listing ID                  |
| `vehicleId` | Vehicle identifier (Unique) |
| `price`     | Current listing price       |
| `version`   | Latest processed version    |
| `createdAt` | Record creation timestamp   |

---

## Version Guard Strategy

The consumer uses a database-level conditional update to prevent stale events from overwriting newer data.

When an event is received:

1. The consumer attempts to update the listing only if the stored version is less than the incoming version.
2. If no rows are updated:
   - The consumer checks whether the listing already exists.
   - If it does not exist, a new listing is created.
   - If it already exists, the event is considered stale and is ignored.
3. A unique constraint on `vehicleId` prevents duplicate listing creation if multiple consumers attempt to create the same listing simultaneously.

The conditional update is performed using:

```ts
await prisma.listing.updateMany({
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
```

This approach relies entirely on the database and does not require any external locking.

---

## Race Condition Handling

If two consumers attempt to create the same listing simultaneously, one insert succeeds while the other fails with Prisma's `P2002` unique constraint error.

The consumer catches this exception and safely ignores the duplicate create request, preventing duplicate records.

---

## Testing Performed

The following scenarios were tested:

- Successfully created a new listing (`version = 1`).
- Sent events out of order (`v3 → v1 → v2`).
- Verified that the stored listing remained at `version = 3`, proving stale events were ignored.
- Sent a newer event (`v4`) and verified that the listing was successfully updated from version `3` to version `4`.
- Verified that concurrent listing creation is safely handled using the database unique constraint on `vehicleId`.
