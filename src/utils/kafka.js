import 'dotenv/config';
import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'vehicle-marketplace',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

export default kafka;
