import { Kafka, logLevel, CompressionTypes } from "kafkajs";

const kafka = new Kafka({
  clientId: "kafka-services",
  brokers: ["localhost:29092"], // KAFKA_ADVERTISED_HOSTNAME
  connectionTimeout: 3000, // ms
  requestTimeout: 25000, // ms
  retry: {
    initialRetryTime: 300, // ms
    maxRetryTime: 30000, // ms
    retries: 10,
  },
  logLevel: logLevel.INFO,
  // ssl: {
  //   rejectUnauthorized: true
  // },
  // sasl: {
  //   mechanism: 'scram-sha-256',
  //   username: 'test',
  //   password: 'test-test',
  // },
});

const producerExecute = async (topic, message) => {
  const producer = kafka.producer({
    allowAutoTopicCreation: true,
  });

  await producer.connect();

  await producer.send({
    topic: topic,
    timeout: 30000, // ms
    compression: CompressionTypes.GZIP,
    messages: message,
  });

  await producer.disconnect();
};

const consumerExecute = async (groupId, topic, callback) => {
  const consumer = kafka.consumer({
    groupId: groupId, // must be unique group id within the cluster
  });

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
        console.log("partition", partition);

        if (callback) callback({ topic, partition, message });
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

const fetchPausedTopics = async (consumer) => {
  await consumer.connect({
    retry: 5,
    readUncommitted: false,
    allowAutoTopicCreation: true,
    sessionTimeout: 3000, // ms
  });

  const pausedTopicPartitions = consumer.paused();

  for (const topicPartitions of pausedTopicPartitions) {
    const { topic, partitions } = topicPartitions;
    console.log({ topic, partitions });
  }
};

export { producerExecute, consumerExecute, fetchPausedTopics };
