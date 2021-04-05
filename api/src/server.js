import nanoexpress from "nanoexpress";
import { Kafka, logLevel } from "kafkajs";

import { routes } from "./routes";

const kafka = new Kafka({
  clientId: "kafka-services",
  brokers: ["kafka:29092"], // KAFKA_ADVERTISED_HOSTNAME
  connectionTimeout: 3000, // ms
  requestTimeout: 25000, // ms
  retry: {
    initialRetryTime: 300, // ms
    maxRetryTime: 30000, // ms
    retries: 5,
  },
  logLevel: logLevel.INFO,
});

const producer = kafka.producer({
  allowAutoTopicCreation: true,
});

const consumer = kafka.consumer({
  groupId: groupId, // must be unique group id within the cluster
});

const app = nanoexpress();

app.use((req, res, next) => {
  req.producer = producer;
  return next();
});

app.use(routes);

const run = async () => {
  await callConsumer(consumer, "groupId", "topic-service1");
  app.listen(3000);
};

run().catch(console.error);
