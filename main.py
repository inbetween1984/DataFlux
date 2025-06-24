import asyncio
import json
import logging
from datetime import datetime, UTC
import aio_pika
from dotenv import load_dotenv
from questdb.ingress import Sender, TimestampNanos
from tenacity import retry, stop_after_attempt, wait_exponential
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s: %(message)s'
)
logger = logging.getLogger('stream_processor')

QUESTDB_HOST = os.getenv('QUESTDB_HOST')
QUESTDB_PORT = os.getenv('QUESTDB_PORT')
QUESTDB_TABLE = os.getenv('QUESTDB_TABLE')
RABBITMQ_URL = os.getenv('RABBITMQ_URL')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')

class StreamProcessor:
    def __init__(self):
        self.rabbit_connection = aio_pika.Connection

    def get_sender(self):
        conf = f'http::addr={QUESTDB_HOST}:{QUESTDB_PORT};'
        return Sender.from_conf(conf)


    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def write_to_questdb(self, entity_id: str, action: str, timestamp: datetime) -> None:
        with self.get_sender() as sender:
            timestamp_ns = TimestampNanos(int(timestamp.timestamp() * 1_000_000_000))
            sender.row(
                QUESTDB_TABLE,
                symbols={'entity_id': entity_id, 'action': action},
                columns={'value': 1.0},
                at=timestamp_ns
            )
            sender.flush()
            logger.info(f"success wrote data to {QUESTDB_TABLE}: entity_id={entity_id}, action={action}, timestamp={timestamp}")

    async def process_message(self, message: aio_pika.IncomingMessage) -> None:
        logger.debug(f"received message: {message}")
        async with message.process():
            try:
                data = json.loads(message.body.decode())
                entity_id = data['entity_id']
                action = data['action']
                timestamp_str = datetime.now(UTC).isoformat()
                timestamp = datetime.fromisoformat(timestamp_str)

                await self.write_to_questdb(entity_id, action, timestamp)

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
        logger.info("stream processor shut down")

async def main():
    processor = StreamProcessor()
    try:
        await processor.start_consuming()
    except KeyboardInterrupt:
        logger.info("received shutdown signal")
        await processor.shutdown()

if __name__ == "__main__":
    asyncio.run(main())