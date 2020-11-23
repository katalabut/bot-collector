import asyncio
import os
import re

import psycopg2
from dotenv import load_dotenv
from redis import Redis
from telethon import TelegramClient, events

load_dotenv()

REDIS_HOST = os.environ['REDIS_HOST']
REDIS_PORT = os.environ['REDIS_PORT']

SESSION = os.environ['SESSION']
API_ID = int(os.environ['API_ID'])
API_HASH = os.environ['API_HASH']

DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_HOST']

CHATS = os.environ['CHATS'].split(sep=',')
QUEUE = os.environ['QUEUE']


class App:
    def __init__(self, client, chats, redis, db, queue):
        self.cl = client
        self.redis = redis
        self.chats = chats
        self.db = db
        self.queue = queue

        self.cl.add_event_handler(self.on_message, events.NewMessage)

    async def run_redis(self):

        while True:
            await asyncio.sleep(6)

            q = self.redis.lpop(self.queue)

            if q is None:
                continue

            query = q.decode("utf-8")

            print(query)

            for chat in self.chats:
                await self.cl.send_message(chat, query)

    def on_message(self, event: events.NewMessage.Event):
        if event.chat.username not in self.chats:
            return

        matches_phone = re.findall(r'((\+7|7)+([0-9]){10})', event.message.raw_text)
        matches_email = re.findall(r'[\w\.-]+@[\w\.-]+', event.message.raw_text)
        targets = matches_email + [match[0] for match in matches_phone]

        match_emails_logins = re.findall(
            r'(Логин: ([\w\.-]+)|Домен: ([\w\.-]+))',
            event.message.raw_text
        )

        if len(match_emails_logins) > 1:
            targets.append(match_emails_logins[0][1] + '@' + match_emails_logins[1][2])

        print('>>> targets', targets)

        cursor = self.db.cursor()
        sql = '''
            INSERT INTO botcollector_searches (target, chat, search_text)
            VALUES (%s, %s, %s)
            ON CONFLICT (target, chat) DO UPDATE SET 
            (target, chat, search_text) = (EXCLUDED.target, EXCLUDED.chat, EXCLUDED.search_text);
        '''

        message = '<a href="https://t.me/%s/%s">Сообщение</a>%s' % (
            event.chat.username,
            event.message.id,
            event.message.text
        )

        for target in targets:
            cursor.execute(sql, (target, event.chat.username, message))

        self.db.commit()


async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    client.parse_mode = 'html'

    try:
        await client.connect()
        await client.start()
    except Exception as e:
        print('Failed to connect', e)
        return

    print('Connected to telegram')

    redis = Redis(host=REDIS_HOST, port=REDIS_PORT)
    print('Connected to redis')

    db = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
    print('Connected to database')

    app = App(client, CHATS, redis, db, QUEUE)
    await asyncio.gather(app.cl.run_until_disconnected(), app.run_redis())


if __name__ == "__main__":
    asyncio.run(main())
