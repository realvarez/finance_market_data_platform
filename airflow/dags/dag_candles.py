import json
import pendulum
import yfinance as yf
from airflow.sdk import dag, task
from kafka import KafkaProducer

@task(task_id='get_candles')
def market_candle(symbol:str = 'NVDA', interval:str = '1m'):
    ticker= yf.Ticker(symbol)
    history=ticker.history(
        start='2026-07-01',
        end='2026-07-05',
        interval=interval
    )[['Open', 'High', 'Low', 'Close', 'Volume']]

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    for row in history.itertuples():
        candle = {
            'id': f'{symbol}{row.Index.strftime('%Y%m%d%H%M%S')}',
            'symbol': symbol,
            'timestamp': row.Index.strftime('%Y-%m-%d %H:%M:%S'),
            'interval': interval,
            'open': row.Open,
            'high': row.High,
            'low': row.Low,
            'close': row.Close,
            'volume': row.Volume
        }
        producer.send(
            topic=f'market-candle-{interval}',
            value=json.dumps(candle).encode('utf-8')
        )

    producer.flush()
    producer.close()


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def market_candles_dag():
    market_candle(symbol='NVDA', interval='1m')

market_candles_dag()