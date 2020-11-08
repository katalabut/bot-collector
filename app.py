import asyncio
import time
import os
import re
# import psycopg2

from dotenv import load_dotenv
from telethon import TelegramClient, events
from redis import Redis

load_dotenv()


def get_env(name, message, cast=str):
    if name in os.environ:
        return os.environ[name]
    while True:
        value = input(message)
        try:
            return cast(value)
        except ValueError as e:
            print(e)
            time.sleep(1)


# conn = psycopg2.connect(dbname='test', user='test', password='napoprtestavku', host='localhost')

REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = os.environ['REDIS_PORT']
SESSION = os.environ['SESSION']
API_ID = int(os.environ['API_ID'])
API_HASH = os.environ['API_HASH']
CHATS = os.environ['CHATS'].split(sep=',')


class App:
    def __init__(self, client, chats, redis, queue):
        self.cl = client
        self.redis = redis
        self.chats = chats
        self.queue = queue

        self.cl.add_event_handler(self.on_message, events.NewMessage)

    async def run_redis(self):

        while True:
            q = self.redis.lpop(self.queue)

            if q is None:
                continue

            query = q.decode("utf-8")

            await self.cl.send_message(self.chats[0], query)

            await asyncio.sleep(1)

    def on_message(self, event: events.NewMessage.Event):
        if event.chat.username not in self.chats:
            return

        match_phones = re.findall(r'((\+7|7)+([0-9]){10})', event.message.raw_text)
        match_emails = re.findall(r'[\w\.-]+@[\w\.-]+', event.message.raw_text)

        print(match_phones)
        print(match_emails)


async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    try:
        await client.connect()
    except Exception as e:
        print('Failed to connect', e)
        return

    print('Connected to telegram')

    redis = Redis(host=REDIS_HOST, port=REDIS_PORT)
    print('Connected to redis')

    app = App(client, redis)
    await asyncio.gather(app.cl.run_until_disconnected(), app.run_redis())


if __name__ == "__main__":
    asyncio.run(main())
