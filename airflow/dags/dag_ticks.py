import asyncio
import json
import logging
import yfinance as yf
import pendulum
from airflow.sdk import dag, task
from aiokafka import AIOKafkaProducer
from datetime import datetime

@task(task_id="run_market_ticks")
async def market_ticks(symbol:list):
    async def handler(message:dict):
        timestamp = datetime.fromtimestamp(
            float(
                message.get('time', '0')
            )/ 1000
        )
        tick_content = {
            'id' : f"{symbol}{timestamp.strftime('%Y%m%d%H%M%S')}",
            'symbol': symbol,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'price': message.get('price'),
            'change_percent': message.get('change_percent')
        }

        try:
            await producer.send(
                topic='market-ticks',
                value=json.dumps(tick_content).encode('utf-8'))

        except Exception as e:
            logging.error(f'An error occured: {e}')

    producer = AIOKafkaProducer(bootstrap_servers=['broker:29092'])
    await producer.start()

    async with yf.AsyncWebSocket() as ws:
        await ws.subscribe(symbol)
        await ws.listen(handler)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def market_ticks_dag():
    """Wrapper to run async function"""
    market_ticks(['NVDA'])

market_ticks_dag()
