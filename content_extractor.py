from db.client import PGClient
import asyncio
import emoji
import re


class ContentExtractor:

    def __init__(self, discord_client):
        self.db_client = PGClient()
        self.discord_client = discord_client

        self.unicode_emote_list = map(lambda x: ''.join(x.split()), emoji.UNICODE_EMOJI.keys())
        self.unicode_emote_ptrn = re.compile('|'.join(re.escape(p) for p in self.unicode_emote_list))
        self.custom_emote_ptrn = re.compile('<:\w+:[0-9]+>')

    async def reactions_to_dict(self, reactions):
        reactions_dict = []
        for r in reactions:
            r_members = []
            if r.custom_emoji:
                emoji = r.emoji.name + '#' + r.emoji.id
            else:
                emoji = r.emoji

            members = await self.discord_client.get_reaction_users(r, limit=100)
            members = [m.id for m in members]

            reactions_dict.append({'message_id': r.message.id,
                            'emoji': emoji,
                            'members': members,
                            })
        return reactions_dict

    def extract_emotes(self, text, member_id, posted_at):
        custom_emotes = {}
        unicode_emotes = {}

        for e in re.findall(self.custom_emote_ptrn, text):
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

        for e in re.findall(self.unicode_emote_ptrn, text):
            if e not in unicode_emotes:
                unicode_emotes[e] = {'name': e,
                                    'count': 1,
                                    'member_id': member_id,
                                    'posted_at': posted_at
                                    }
            else:
                unicode_emotes[e]['count'] += 1

        return {**custom_emotes, **unicode_emotes}

    async def extract_content(self, log):
        posted_at = log.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        member_name = log.author.name
        member_id = log.author.id
        discriminator = log.author.discriminator
        mentions = [m.id for m in log.mentions]
        body = str(log.content)
        reactions = await self.reactions_to_dict(log.reactions)
        emotes = self.extract_emotes(body, member_id, posted_at)
        self.db_client.save_message(log.id,log.channel.server.id, log.channel.id,
                                posted_at, member_id, body, mentions)
        self.db_client.save_member(member_id, member_name, discriminator)
        self.db_client.save_reactions(reactions)
        self.db_client.save_emotes(emotes)
