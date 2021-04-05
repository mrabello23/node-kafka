import { Router } from "nanoexpress";
import { callProducer } from "kafka.js";

const routes = Router();

routes.get("/", (req, res) => {
  return res.send({ status: "ok" });
});

routes.post("/produce-service1", async (req, res) => {
  await callProducer(req.producer, "topic-service1", [
    {
      key: "key1",
      value: "Hello world kafka!",
      headers: {
        "correlation-id": "0459b8f4-3459-46ba-85da-8d521d4e1ff2", // custom
        "system-id": "ad788e20-e057-416e-bb52-9e9121581841", // custom
      },
    },
    {
      key: "key2",
      value: "Producer for service 1",
      headers: {
        "correlation-id": "d3237e60-8660-4d11-a9fd-0a9f0aecbb23", // custom
        "system-id": "2199c2c5-ea9a-42bf-867a-c46df81c163e", // custom
      },
    },
  ]);

  return res.send({ message: "Called producer for service 1" });
});

export default routes;
