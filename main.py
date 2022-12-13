import json
from config import KAFKA_BOOSTRAP_SERVERS, ORDER_KAFKA_TOPIC
from kafka import KafkaProducer
from fastapi import FastAPI
import uvicorn

app = FastAPI()
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOSTRAP_SERVERS)

@app.post("/order")
def order(name: str, items: str, value: float):
    data = {
        "name": name,
        "items": items,
        "value": value
    }
    producer.send(ORDER_KAFKA_TOPIC,json.dumps(data).encode("utf-8"))

    return {"order finished"}



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=30000)