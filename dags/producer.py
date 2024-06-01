"""
All of Kafka processes
"""
import os
import json
# import socket
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
# from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
# import asyncio

load_dotenv()

daily_topic = os.environ.get("DAILY_DATA_TOPIC", "dailymetrics")

bootstrap_servers = '3.255.212.165:29092'
config = {
    "bootstrap.servers": bootstrap_servers,
    "queue.buffering.max.messages": 1,
    "queue.buffering.max.ms": 0,
    "batch.num.messages": 1
    }
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
    p.produce(daily_topic, json.dumps(message).encode('utf-8'))
    print("Message produced Successfully")

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()
    print("Topic Flushed")


consumer_conf = {'bootstrap.servers': bootstrap_servers,
    'group.id': 'dailymetricsconsumergroup',
    'auto.offset.reset': 'smallest'}
consumer = Consumer(consumer_conf)
running = True

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg.value())
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        shutdown()

def msg_process(message):
    print(f"Processing messag {message}")

def shutdown():
    running = False


# if __name__ == "__main__":
#     message = [1,2,3,4,5,6]

#     for i in range(5):
#         produce_message(message=message)
#         print(f"Iteration: {i+1}")
#     # p.close()
    
#     basic_consume_loop(consumer, [daily_topic])
