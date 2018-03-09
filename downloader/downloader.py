import discord
import asyncio
import settings
import logging
from downloader.data_processor import DataProcessor
from db.client import PGClient

logging.basicConfig(level=logging.INFO)
discord_client = discord.Client()


@discord_client.event
async def on_ready():
    server_id = settings.DISCORD_SERVER_ID
    channels = settings.DISCORD_CHANNELS
    logging.info('Logged in as')
    logging.info(discord_client.user.name)
    logging.info(discord_client.user.id)
    logging.info('------')
    processor = DataProcessor(
                            discord_client,
                            server_id,
                            [c['id'] for c in channels],
                            )
    await processor.collect_data()

    logging.info("DONE!")
    discord_client.close()


def run():
    discord_client.run(settings.DISCORD_API_KEY, bot=False)
