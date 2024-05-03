import psycopg
from multiprocessing import Pool
from psycopg import ClientCursor
from multiprocessing import Pool

from kafka import create_consumer
from postgres import create_database, connection_url, save_message, asave_messages, asave_messages_batch, save_message_batch

BATCH_SIZE = 10_000
BATCH_INSERT = 35

def get_conn():
    return psycopg.connect(connection_url)

def reset_database(consumer):

    def inner(num_records):

        conn = get_conn()
        create_database(conn)
        conn.close()

        consumer(num_records)
    
    return inner

def areset_database(consumer):

    async def inner(num_records):

        conn = get_conn()
        create_database(conn)
        conn.close()

        await consumer(num_records)
    
    return inner

@reset_database
def exactly_once_consumer(num_records: int):
    
    consumer = create_consumer()
    conn = get_conn()

    try:
        count = 0

        while count < num_records:
            
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            save_message(conn, msg.value().decode())

            count+=1
            consumer.commit()
    finally:
        conn.close()

@areset_database
async def async_consumer(num_records: int):

    consumer = create_consumer()

    try:

        count = 0

        while count < num_records:
        
            msgs = consumer.consume(BATCH_SIZE, timeout=1.0)
            if len(msgs) == 0: continue
            
            msgs_content = map(lambda x : x.value().decode(), msgs)

            await asave_messages(msgs_content)

            count+=len(msgs)
            consumer.commit()
    finally:
        consumer.close()

@areset_database
async def async_batch_consumer(num_records: int):

    consumer = create_consumer()

    try:

        count = 0

        while count < num_records:
        
            msgs = consumer.consume(BATCH_SIZE, timeout=1.0)
            num_msgs = len(msgs)

            if num_msgs == 0: continue
        
            msgs_content = list(map(lambda x : x.value().decode(), msgs))

            if num_msgs <= BATCH_INSERT:
                chunks = [msgs_content]
            else:
                chunks = [
                    msgs_content[
                        i*BATCH_INSERT:
                        min((i*BATCH_INSERT)+BATCH_INSERT, num_msgs)
                    ]
                    for i
                    in range(num_msgs // BATCH_INSERT)]

            await asave_messages_batch(chunks)

            count+=len(msgs)
            consumer.commit()

    finally:
        consumer.close()

def save_message_conn(msg: str): save_message(conn, msg)

@reset_database
def multiprocess_consumer(num_records: int):

    consumer = create_consumer()
    conn = None

    def set_global_conn():
        global conn
        conn = get_conn()

    try:

        count = 0

        while count < num_records:

            msgs = consumer.consume(BATCH_SIZE, timeout=1.0)
        
            if len(msgs) == 0: continue
            
            msgs_content = map(lambda x : x.value().decode(), msgs)

            with Pool(processes=48,
                      initializer=set_global_conn) as pool:

                pool.map(save_message_conn, msgs_content)

            count+=len(msgs)
            consumer.commit()

    finally:
        consumer.close()

def save_message_batch_conn(msgs: list[str]): save_message_batch(conn, msgs)

@reset_database
def multiprocess_batch_consumer(num_records: int):

    consumer = create_consumer()
    conn = None

    def set_global_conn():
        global conn
        conn = get_conn()

    try:

        count = 0

        while count < num_records:
        
            msgs = consumer.consume(BATCH_SIZE, timeout=1.0)
            num_msgs = len(msgs)

            if num_msgs == 0: continue
        
            msgs_content = list(map(lambda x : x.value().decode(), msgs))

            if num_msgs <= BATCH_INSERT:
                chunks = [msgs_content]
            else:
                chunks = [
                    msgs_content[
                        i*BATCH_INSERT:
                        (i*BATCH_INSERT)+BATCH_INSERT
                    ]
                    for i
                    in range(num_msgs // BATCH_INSERT + 1)]

            with Pool(processes=48,
                      initializer=set_global_conn) as pool:
                pool.map(save_message_batch_conn, chunks)

            count+=num_msgs
            consumer.commit()

    finally:
        consumer.close()
