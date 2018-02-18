import discord
import asyncio
from content_extractor import ContentExtractor

client = discord.Client()

async def save_messages(server_id, channels=[]):
    server = client.get_server(server_id)
    extractor = ContentExtractor(client)
    all_channels = []
    if channels:
        for c in server.channels:
            if str(c) in channels:
                all_channels.append(c)
    else:
        all_channels = server.channels

    i = 1
    for c in all_channels:
        async for log in client.logs_from(c, limit=1000000000):
            await extractor.extract_content(log)

            if i % 1000 == 0:
                print(i)
            i += 1

@client.event
async def on_ready():
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('------')
    await save_messages('263420076919750676', ['general', 'mapping', 'pokemon', 'esports', 'nsfw'])
    print("DONE!")
    client.logout()
    #get_msgs()


client.run('token', bot=False)