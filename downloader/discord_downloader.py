import asyncio
from content_extractor import ContentExtractor
from db.client import PGClient


class DiscordDownloader:

    def __init__(self, discord_client, server_id, channels=[]):
        self.server_id = server_id
        self.channels = channels
        self.client = discord_client

    async def download_data(self):
        client = self.client
        channels = self.channels

        server = client.get_server(self.server_id)
        extractor = ContentExtractor(client)

        for m in server.members:
            PGClient().save_member(m)

        chosen_channels = []

        if self.channels:
            for c in server.channels:
                if c.id in channels:
                    chosen_channels.append(c)
        else:
            chosen_channels = server.channels

        i = 0
        for c in chosen_channels:
            print('Downloading messages from channel ' + str(c) + '...')
            c_i = 0
            async for log in client.logs_from(c, limit=1000000000):
                await extractor.extract_content(log)

                if i % 1000 == 0:
                    print('Processed ' + str(i) + ' messages in total')
                i += 1
                c_i += 1
            print('Channel ' + str(c) + ' done. Downloaded ' + str(c_i) + ' messages')
