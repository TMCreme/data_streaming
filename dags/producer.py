"""
All of Kafka processes
"""
import os
import socket
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

daily_topic = os.environ.get("DAILY_DATA_TOPIC", "dailymetrics")
conf = {'bootstrap.servers': 'broker:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)


def produce_message(key, message):
    """Message production"""
    print("Producing the message")

    producer.produce(daily_topic, key=key, value=message,  callback=acked)
    print("Message published")
    producer.poll(1)
    print("Polling the messages")


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


# producer.produce(daily_topic, key="key", value="value", callback=acked)

# # Wait up to 1 second for events. Callbacks will be invoked during
# # this method call if the message is acknowledged.
# producer.poll(1)
