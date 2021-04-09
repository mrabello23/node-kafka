import nanoexpress from "nanoexpress";
import { Kafka, logLevel, CompressionTypes } from "kafkajs";

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

const app = nanoexpress();

app.get("/", (req, res) => {
  return res.send({ status: "ok" });
});

app.post("/produce-service1", async (req, res) => {
  const topic = "topic-service1";
  const messages = [
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
  ];

  await producer.connect();

  await producer.send({
    topic: topic,
    timeout: 30000, // ms
    compression: CompressionTypes.GZIP,
    messages: messages,
  });

  await producer.disconnect();

  return res.send({ message: "Called producer for service 1" });
});

const run = async () => app.listen(3000);
run().catch(console.error);
