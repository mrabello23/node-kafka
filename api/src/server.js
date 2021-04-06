import nanoexpress from "nanoexpress";
import { Kafka, logLevel } from "kafkajs";
import { callProducer, callConsumer } from "./kafka.js";

const kafka = new Kafka({
  clientId: "kafka-services",
  brokers: ["kafka:29092"], // KAFKA_ADVERTISED_HOSTNAME
  connectionTimeout: 3000, // ms
  requestTimeout: 25000, // ms
  retry: {
    initialRetryTime: 300, // ms
    maxRetryTime: 30000, // ms
    retries: 10,
  },
  logLevel: logLevel.INFO,
});

const producer = kafka.producer({
  allowAutoTopicCreation: true,
});

const consumer = kafka.consumer({
  groupId: "api-node", // must be unique group id within the cluster
});

const app = nanoexpress();

app.get("/", (req, res) => {
  return res.send({ status: "ok" });
});

app.post("/produce-service1", async (req, res) => {
  const topic = req.body.topic || "topic-service1";

  await callProducer(req.producer, topic, [
    {
      value: JSON.stringify({
        success: true,
        message: "Hello world kafka!",
        id: 1234,
        origin: 7766,
      }),
    },
    {
      value: JSON.stringify({
        success: true,
        message: "Producer for service 1",
        id: 4321,
        origin: 9988,
      }),
    },
  ]);

  return res.send({ message: "Called producer for service 1" });
});

const run = async () => {
  // await callConsumer(consumer, "topic-service1");
  app.listen(3000);
};

run().catch(console.error);
