from fastapi import APIRouter
import json
from kafka import KafkaConsumer, KafkaProducer
from config import KAFKA_BOOSTRAP_SERVERS, ORDER_KAFKA_TOPIC, ORDER_CONFIRMED_KAFKA_TOPIC

consumer = KafkaConsumer(ORDER_KAFKA_TOPIC,bootstrap_servers=KAFKA_BOOSTRAP_SERVERS)
producer = KafkaProducer(bootstrap_servers=KAFKA_BOOSTRAP_SERVERS)

router = APIRouter(prefix='/order')

@router.get("/confirmed")
def order_confirmed():
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        name = consumed_message["name"]
        value = consumed_message["value"]
        cartao = consumed_message["cartao"]
        cvv = consumed_message["cvv"]

        if len(str(cartao)) == 16:
            cartao_validado = cartao
        else:
            print({"cartão inválido"})
            
        if len(str(cvv)) == 3:
            cvv_validado = cvv
        else:
            print({"cvv inválido"})

        data_order_confirmed = {
            "name": name,
            "cartao": cartao_validado,
            "value": value,
            "cvv": cvv_validado
        }
    producer.send(ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(data_order_confirmed).encode("utf-8"))
    return {data_order_confirmed}