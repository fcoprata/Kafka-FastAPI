import json
from kafka import KafkaConsumer,KafkaProducer
from config import KAFKA_BOOSTRAP_SERVERS, ORDER_KAFKA_TOPIC, ORDER_CONFIRMED_KAFKA_TOPIC

consumer = KafkaConsumer(ORDER_KAFKA_TOPIC,bootstrap_servers=KAFKA_BOOSTRAP_SERVERS)
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOSTRAP_SERVERS)

while True:

    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        print(consumed_message)
        # name = consumed_message["name"]
        # items = consumed_message["items"]
        # value = consumed_message["value"]

        # data = {
        #     "name": name,
        #     "items": items,
        #     "value": value
        # }

        # producer.send(ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))