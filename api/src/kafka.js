import { CompressionTypes } from "kafkajs";

const callProducer = async (producer, topic, messages) => {
  await producer.connect();

  await producer.send({
    topic: topic,
    timeout: 30000, // ms
    compression: CompressionTypes.GZIP,
    messages: messages,
  });

  await producer.disconnect();
};

const callConsumer = async (consumer, topic) => {
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
         * TO DO Conde implementation
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

export { callProducer, callConsumer };
