import discord
import asyncio
import os
import json
from db.client import PGClient

client = discord.Client()

pg_client = PGClient()


def parse_custom_emoji(emoji):
    return emoji[:-1].split(':')[1:]    #return emoji_name, emoji_id


async def reactions_to_dict(reactions):
    reactions_dict = []
    for r in reactions:
        r_members = []
        if r.custom_emoji:
            emoji = r.emoji.name + '#' + r.emoji.id                
        else:
            emoji = r.emoji

        members = await client.get_reaction_users(r, limit=100)
        members = [m.name + '#' + m.discriminator for m in members]

        reactions_dict.append({'message_id': r.message.id,
                          'emoji': emoji,
                          'members': members,
                         })
    return reactions_dict

async def save_messages(server_id, channels=[]):
    server = client.get_server(server_id)
    all_channels = []
    if channels:
        for c in server.channels:
            if str(c) in channels:
                all_channels.append(c)
    else:
        all_channels = server.channels
    
    for c in all_channels:
        async for log in client.logs_from(c, limit=1000000):
            stringTime = log.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            author_name = log.author.name
            author_id = log.author.id
            discriminator = log.author.discriminator
            mentions = [m.name+'#'+m.discriminator for m in log.mentions]
            content = str(log.content)
            reactions = await reactions_to_dict(log.reactions)
            pg_client.insert_message(log.id,log.channel.server.id, log.channel.id,
                                   stringTime, author_id, author_name, content,
                                   mentions)
            pg_client.insert_member(author_id, author_name, discriminator)
            pg_client.insert_reactions(reactions)

@client.event
async def on_ready():
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('------')
    await save_messages('263420076919750676',['general'])
    print("DONE!")
    client.logout()
    #get_msgs()


client.run('token', bot=False)