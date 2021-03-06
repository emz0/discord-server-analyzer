import asyncio
import emoji
import re
import discord
import settings
import logging
from db.client import PGClient


logging.basicConfig(level=logging.INFO)


class DataProcessor:

    def __init__(self, discord_client, server_id, channels=[]):
        self.server_id = server_id
        self.channels = channels
        self.client = discord_client
        self.db_client = PGClient()
        self.discord_client = discord_client

        self.unicode_emote_list = map(lambda x: ''.join(x.split()),
                                      emoji.UNICODE_EMOJI.keys())
        self.unicode_emote_ptrn = re.compile('|'.join(re.escape(p)
                                             for p in self.unicode_emote_list))
        self.custom_emote_ptrn = re.compile('<:\w+:[0-9]+>')

    async def collect_data(self):
        """
        Download messages from server
        """
        client = self.client
        channels = self.channels
        logging.info('server id: {}'.format(self.server_id))
        server = client.get_server(self.server_id)

        for m in server.members:
            self.db_client.save_member(m)

        chosen_channels = []

        if self.channels:
            for c in server.channels:
                if c.id in channels:
                    chosen_channels.append(c)
        else:
            chosen_channels = server.channels

        i = 0
        for c in chosen_channels:
            logging.info('Downloading messages from channel {} ...'.format(c))
            c_i = 0
            async for log in client.logs_from(c, limit=1000000000):
                await self.save_data(log)

                if i % 1000 == 0:
                    logging.info('Processed {} messages in total'.format(i))
                i += 1
                c_i += 1
            logging.info(
                'Channel {} done. Downloaded {} messages'.format(c, c_i)
                )

    async def save_data(self, log):
        """
        Process and save downloaded data
        """
        reactions = await self.reactions_to_dict(log.reactions)
        emotes = self.extract_emotes(log)

        self.db_client.save_reactions(reactions)
        self.db_client.save_emotes(emotes)
        self.db_client.save_message(log)
        self.db_client.save_member(log.author)

    async def reactions_to_dict(self, reactions):
        """
        Transform reactions to list of dictionaries
        """
        reactions_dict = []
        for r in reactions:
            r_members = []
            if r.custom_emoji:
                emote = r.emoji.id
            else:
                emote = r.emoji

            members = \
                await self.discord_client.get_reaction_users(r, limit=100)
            members = [m.id for m in members]

            reactions_dict.append({'message_id': r.message.id,
                                    'emote_id': emote,
                                    'members': members,
                                    })
        return reactions_dict

    def extract_emotes(self, log):
        """
        Compile regex patterns
        """
        body = log.content
        member_id = log.author.id
        posted_at = log.timestamp
        custom_emotes = {}
        unicode_emotes = {}

        for e in re.findall(self.custom_emote_ptrn, body):
            e_name, e_id = e[2:-1].split(':')
            if e_id not in custom_emotes:
                custom_emotes[e_id] = {'name': e_name,
                                       'count': 1,
                                       'member_id': member_id,
                                       'posted_at': posted_at
                                       }
            else:
                custom_emotes[e_id]['name'] = e_name
                custom_emotes[e_id]['count'] += 1

        for e in re.findall(self.unicode_emote_ptrn, body):
            if e not in unicode_emotes:
                unicode_emotes[e] = {'name': e,
                                     'count': 1,
                                     'member_id': member_id,
                                     'posted_at': posted_at
                                     }
            else:
                unicode_emotes[e]['count'] += 1

        return {**custom_emotes, **unicode_emotes}
