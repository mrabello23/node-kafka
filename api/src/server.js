import nanoexpress from "nanoexpress";
import { v4 as uuidv4 } from "uuid";

import { producerExecute } from "./kafka.js";

const app = nanoexpress();

app.get("/", (req, res) => {
  return res.send({ status: "ok" });
});

app.post("/producer-1", async (req, res) => {
  const messageId = uuidv4();
  const body = JSON.parse(req.body);

  console.log("message", body.message);
  console.log("topic", body.topic);

  const message = [
    {
      key: messageId,
      value: JSON.stringify({
        id: messageId,
        origin: body.origin || null,
        message: body.message,
      }),
    },
  ];

  await producerExecute(body.topic, message);

  return res.send({ message: "Producer for service 1: " + messageId });
});

const run = async () => app.listen(3000);
run().catch(console.error);
