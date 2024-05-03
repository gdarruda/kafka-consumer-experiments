from confluent_kafka import Producer, Consumer
import socket
from uuid import uuid4

topic = "messages"
bootstrap_servers = "localhost:9093"

def create_producer() -> Producer:

    conf = {'bootstrap.servers': bootstrap_servers,
            'client.id': socket.gethostname()}

    return Producer(conf)

def create_consumer() -> Consumer:

    conf = {'bootstrap.servers': bootstrap_servers,
            'group.id': str(uuid4()),
            'auto.offset.reset': 'smallest',
            'enable.auto.commit': 'false'}
    
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    return consumer