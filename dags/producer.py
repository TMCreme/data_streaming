"""
All of Kafka processes
"""
import os
import json
# import socket
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
from confluent_kafka import Producer
# from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
# import asyncio

load_dotenv()

daily_topic = os.environ.get("DAILY_DATA_TOPIC", "dailymetrics")

bootstrap_servers = 'kafka:9092'
# producer = KafkaProducer(
#     bootstrap_servers=bootstrap_servers,
#     value_serializer=lambda x: json.dumps(x).encode('utf-8'),
#     acks='all',
#     retries=5,
#     linger_ms=10,
#     buffer_memory=33554432,
#     max_block_ms=60000,
#     api_version=(0,1,0)
#     )


p = Producer({'bootstrap.servers': bootstrap_servers})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def produce_message(message):
    """Message production"""
    print("Producing the message")
    # for data in some_data_source:
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    p.produce(daily_topic, json.dumps(message).encode('utf-8'), callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()


def consume_message():
    """Read the message from the topic"""
    consumer = KafkaConsumer(
        daily_topic,
        group_id="dailymetricsconsumergroup",
        bootstrap_servers=[bootstrap_servers])
    for message in consumer:
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
    return True


# if __name__ == "__main__":
#     message = json.dumps([1,2,3,4,5,6])

#     for _ in range(5):
#         produce_message(message=message)
    
#     consume_message()
