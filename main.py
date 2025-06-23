import asyncio
import json
import logging
from datetime import datetime, UTC
import aio_pika
from dotenv import load_dotenv
from influxdb_client_3 import InfluxDBClient3
from tenacity import retry, stop_after_attempt, wait_exponential
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('stream_processor')

INFLUXDB_HOST = os.getenv('INFLUXDB_HOST')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_DATABASE = os.getenv('INFLUXDB_DATABASE')
RABBITMQ_URL = os.getenv('RABBITMQ_URL')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')


class StreamProcessor:
    def __init__(self):
        self.influx_client = InfluxDBClient3(
            host=INFLUXDB_HOST,
            token=INFLUXDB_TOKEN,
            database=INFLUXDB_DATABASE
        )
        self.rabbit_connection = aio_pika.Connection

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)) #повтор, если influx недоступен
    async def write_to_influx(self, entity_id: str, action: str, timestamp: datetime) -> None:
        timestamp_ns = int(timestamp.timestamp() * 1_000_000_000)
        record = (
            f"{INFLUXDB_DATABASE},entity_id={entity_id},action={action} value=1 {timestamp_ns}"
        )

        await asyncio.to_thread(self.influx_client.write, record)
        logger.info(f"success wrote data to {INFLUXDB_DATABASE}: entity_id={entity_id}, action={action}, timestamp={timestamp}")

    async def process_message(self, message: aio_pika.IncomingMessage) -> None:
        logger.debug(f"received message: {message}")
        async with message.process():
            try:
                data = json.loads(message.body.decode())

                entity_id = data['entity_id']
                action = data['action']
                timestamp_str = datetime.now(UTC).isoformat()
                timestamp = datetime.fromisoformat(timestamp_str)

                await self.write_to_influx(entity_id, action, timestamp)

            except Exception as e:
                logger.error(f"failed to process message: {e}")

    async def start_consuming(self) -> None:
        logger.info("starting stream processor...")

        try:
            self.rabbit_connection = await aio_pika.connect_robust(RABBITMQ_URL)
            async with self.rabbit_connection:
                channel = await self.rabbit_connection.channel()
                await channel.set_qos(prefetch_count=1)

                queue = await channel.declare_queue(RABBITMQ_QUEUE, durable=True)
                await queue.consume(self.process_message)

                logger.info("waiting for messages...")
                await asyncio.Future()

        except Exception as e:
            logger.error(f"failed to start consumer: {e}")
            raise

    async def shutdown(self) -> None:
        logger.info("shutting down stream processor...")
        if self.rabbit_connection:
            await self.rabbit_connection.close()
        self.influx_client.close()


async def main():
    processor = StreamProcessor()
    try:
        await processor.start_consuming()
    except KeyboardInterrupt:
        logger.info("received shutdown signal")
        await processor.shutdown()


if __name__ == "__main__":
    asyncio.run(main())