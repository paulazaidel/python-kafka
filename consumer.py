import os

import yaml
from confluent_kafka.cimpl import Consumer
from dotenv import load_dotenv

load_dotenv()

consumer = Consumer(
    {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.getenv("SASL_USERNAME"),
        "sasl.password": os.getenv("SASL_PASSWORD"),
        "group.id": os.getenv("GROUP_ID"),
        "auto.offset.reset": "earliest",
    }
)

with open("kafka_topics.yaml") as file:
    topics = yaml.load(file, Loader=yaml.FullLoader)

consumer.subscribe(topics["topics"])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        break

    print("Received message: {}".format(msg.value().decode("utf-8")))

consumer.close()
