import json
from uuid import uuid4
import numpy as np
from confluent_kafka import Producer

TOPIC = "messages"

def send_message(producer: Producer, content: dict):

    value = json.dumps(content)
    key = content['client_id']

    producer.produce(TOPIC, key=key, value=value)
    producer.poll(0)

def create_sample():
    
    σ_price = 2_000
    μ_price = 10_000

    quantity_range = (1,10)
    products_range = (1,5)

    return {
        'message_id': str(uuid4()),
        'client_id': str(uuid4()),
        'products': [{"sku": str(uuid4()),
                      "price": max(100,
                                   int(σ_price + np.random.randn() * μ_price)),
                      "quantity": np.random.randint(*quantity_range)}
                     for _ in range(np.random.randint(*products_range))]
    } 

def produce_samples(producer: Producer, num_samples: int):

    [send_message(producer, create_sample()) 
     for _ in
     range(num_samples)] 
    
    producer.flush()