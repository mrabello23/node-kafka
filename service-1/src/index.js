import { Kafka, logLevel } from "kafkajs";

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

const consumer = kafka.consumer({
  groupId: "service1", // must be unique group id within the cluster
});

const topic = "topic-service1";

const run = async () => {
  await consumer.connect({
    retry: 5,
    readUncommitted: false,
    allowAutoTopicCreation: true,
    sessionTimeout: 3000, // ms
  });

  await consumer.subscribe({
    topic: topic,
    fromBeginning: true,
  });

  await consumer.run({
    partitionsConsumedConcurrently: 2, // will be called up to 2 times concurrently
    autoCommitInterval: 1500, // commit offsets after a given period in milliseconds (ms)
    autoCommitThreshold: 100, // commit offsets after resolving a given number of messages
    eachMessage: async ({ topic, partition, message }) => {
      try {
        console.log("topic", topic);
        console.log("message", {
          key: message.key.toString(),
          value: message.value.toString(),
          headers: message.headers,
        });

        /**
         * TO DO Consumer scripts implementation
         */
      } catch (e) {
        if (e instanceof TooManyRequestsError) {
          // Other partitions will keep fetching and processing,
          // until if / when they also get throttled
          consumer.pause([{ topic, partitions: [partition] }]);

          setTimeout(() => {
            // Other partitions that are paused will continue to be paused
            consumer.resume([{ topic, partitions: [partition] }]);
          }, e.retryAfter * 1000);
        }

        throw e;
      }
    },
  });
};

run().catch(console.error);
