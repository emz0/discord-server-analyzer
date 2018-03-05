import discord
import asyncio
import settings
from downloader.data_processor import DataProcessor
from db.client import PGClient

discord_client = discord.Client()

@discord_client.event
async def on_ready():
    server_id = settings.DISCORD_SERVER_ID
    channels = settings.DISCORD_CHANNELS
    print('Logged in as')
    print(discord_client.user.name)
    print(discord_client.user.id)
    print('------')
    processor = DataProcessor(
                            discord_client,
                            server_id,
                            [c['id'] for c in channels],
                            )
    await processor.collect_data()

    print("DONE!")
    discord_client.close()


def run():
    discord_client.run(settings.DISCORD_API_KEY, bot=False)
