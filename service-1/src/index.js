import { consumerExecute, fetchPausedTopics } from "./kafka.js";

const groupId = "groupId-service1";
const topic = "topic-service1";

const run = async () => {
  await consumerExecute(groupId, topic, ({ message }) => {
    console.log("message", {
      key: message.key.toString(),
      value: message.value.toString(),
    });
  });
};

run().catch(console.error);
