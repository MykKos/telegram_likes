"""
Script which creates the database and needed table
"""
import asyncio
from aioch import Client
import os


async def create_database(database_name):
    password = os.environ.get('CLICKHOUSE_PASSWORD')
    port = os.environ.get('CLICKHOUSE_PORT')
    client = Client('localhost', password=password, port=port)
    await client.execute(f'CREATE DATABASE if not exists {database_name}')


async def create_tables(database_name):
    password = os.environ.get('CLICKHOUSE_PASSWORD')
    port = os.environ.get('CLICKHOUSE_PORT')
    client = Client('localhost', password=password,
                    port=port, database=database_name)
    await client.execute('''create table if not exists likes(
        Id UInt64, UserId UInt64, ChannelId String,
        Reaction String, MessageId UInt64, EventTime DateTime,
        EventDate Date, LikeTime Float64, Url String
        ) Engine = MergeTree() PARTITION BY toYYYYMM(EventDate)
        ORDER BY (Id, EventTime)
    ''')

if __name__ == '__main__':
    db_name = os.environ.get('CLICKHOUSE_DATABASE')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_database(db_name))
    loop.run_until_complete(create_tables(db_name))
