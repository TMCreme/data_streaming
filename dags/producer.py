"""
All of Kafka processes
"""
import os
# import socket
from dotenv import load_dotenv
# from kafka import KafkaProducer
from aiokafka import AIOKafkaProducer
import asyncio

load_dotenv()

daily_topic = os.environ.get("DAILY_DATA_TOPIC", "dailymetrics")
# conf = {'bootstrap.servers': 'broker:9092',
#         'client.id': socket.gethostname()}

bootstrap_servers = 'kafka:9092'
# producer = KafkaProducer(bootstrap_servers='broker:9092')


async def produce_message(message):
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    # Get cluster layout and initial topic/partition leadership information
    print("Producing the message")
    await producer.start()
    try:
        # Produce message
        print("Message published")
        await producer.send_and_wait(daily_topic, message)
        print("Flushing the messages")
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

# asyncio.run(send_one())


# def produce_message(key, message):
#     """Message production"""
#     print("Producing the message")

#     producer.send(daily_topic, value=message)
#     print("Message published")
#     producer.flush()
#     print("Flushing the messages")
#     return True


# def acked(err, msg):
#     if err is not None:
#         print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
#     else:
#         print("Message produced: %s" % (str(msg)))


# producer.produce(daily_topic, key="key", value="value", callback=acked)

# # Wait up to 1 second for events. Callbacks will be invoked during
# # this method call if the message is acknowledged.
# producer.poll(1)
