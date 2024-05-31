"""
All of Kafka processes
"""
import os
import json
# import socket
from dotenv import load_dotenv
from kafka import KafkaProducer
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
# import asyncio

load_dotenv()

daily_topic = os.environ.get("DAILY_DATA_TOPIC", "dailymetrics")

bootstrap_servers = 'kafka:9092'
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8'))


# async def produce_message(message):
#     producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
#     # Get cluster layout and initial topic/partition leadership information
#     print("Producing the message")
#     await producer.start()
#     try:
#         # Produce message
#         print("Message published")
#         await producer.send_and_wait(daily_topic, message)
#         print("Flushing the messages")
#     finally:
#         # Wait for all pending messages to be delivered or expire.
#         await producer.stop()

# asyncio.run(send_one())


# async def consume():
#     consumer = AIOKafkaConsumer(
#         daily_topic,
#         bootstrap_servers=bootstrap_servers)
#     # Get cluster layout and join group `my-group`
#     await consumer.start()
#     try:
#         # Consume messages
#         async for msg in consumer:
#             print("consumed: ", msg.topic, msg.partition, msg.offset,
#                   msg.key, msg.value, msg.timestamp)
#     finally:
#         # Will leave consumer group; perform autocommit if enabled.
#         await consumer.stop()

# asyncio.run(consume())


def produce_message(message):
    """Message production"""
    print("Producing the message")

    producer.send(daily_topic, value=message)
    print(f"Message published: {message}")
    producer.flush()
    print("Flushing the messages")
    return True


# def acked(err, msg):
#     if err is not None:
#         print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
#     else:
#         print("Message produced: %s" % (str(msg)))


# producer.produce(daily_topic, key="key", value="value", callback=acked)

# # Wait up to 1 second for events. Callbacks will be invoked during
# # this method call if the message is acknowledged.
# producer.poll(1)
