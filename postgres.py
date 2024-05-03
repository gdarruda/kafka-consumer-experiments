import json
from datetime import datetime
import psycopg
import asyncio

connection_url = "postgresql://demo:demo@localhost:5432"

def create_database(conn):
    
    with conn.cursor() as cur:
        cur.execute('''create table if not exists 
                        messages(message_id char(36) primary key, 
                                 client_id char(36),
                                 datetime timestamp,
                                 content text)''')
        cur.execute('truncate table messages')

    conn.commit()

def save_message(conn, msg: str) -> callable:

    content =  json.loads(msg)

    values = (
        content['message_id'],
        content['client_id'],
        datetime.now(),
        msg,
    )

    insert = "insert into messages VALUES (%s, %s, %s, %s)"

    with conn.cursor() as cur:
        cur.execute(insert, values)

    conn.commit()

def save_message_batch(conn, msgs: list[str]):

    values = []

    for msg in msgs:
    
        content =  json.loads(msg)

        values.append((
            content['message_id'],
            content['client_id'],
            datetime.now(),
            msg,
        ))

    insert = "insert into messages VALUES (%s, %s, %s, %s)"

    with conn.cursor() as cur:
        cur.executemany(insert, values)
        
    conn.commit()

async def asave_message(aconn, msg: str):
    
    async with aconn.cursor() as acur:

        content =  json.loads(msg)

        values = (
            content['message_id'],
            content['client_id'],
            datetime.now(),
            msg,
        )

        insert = "insert into messages VALUES (%s, %s, %s, %s)"

        await acur.execute(insert, values)

async def asave_message_batch(aconn, msgs: list[str]):
    
    async with aconn.cursor() as acur:

        values = []

        for msg in msgs:
    
            content =  json.loads(msg)

            values.append((
                content['message_id'],
                content['client_id'],
                datetime.now(),
                msg,
            ))

        insert = "insert into messages VALUES (%s, %s, %s, %s)"

        await acur.executemany(insert, values)

async def asave_messages_batch(batch: list[list[str]]):
    
    aconn = await psycopg.AsyncConnection.connect(connection_url)

    async with aconn:
        async with asyncio.TaskGroup() as tg:
            for msgs in batch:
                tg.create_task(asave_message_batch(aconn, msgs))

        await aconn.commit()

async def asave_messages(msgs: list[str]):
    
    aconn = await psycopg.AsyncConnection.connect(connection_url)

    async with aconn:
        async with asyncio.TaskGroup() as tg:
            for msg in msgs:
                tg.create_task(asave_message(aconn, msg))

        await aconn.commit()