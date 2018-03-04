import discord
import asyncio
import settings
from discord_downloader import DiscordDownloader

client = discord.Client()


@client.event
async def on_ready():
    server_id = settings.DISCORD_SERVER_ID
    channels = settings.DISCORD_CHANNELS
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('------')
    # client.close()
    downloader = DiscordDownloader(client,
                                   server_id,
                                   [c['id'] for c in channels],
                                   )

    await downloader.download_data()

    print("DONE!")
    client.close()

client.run("key", bot=False)
