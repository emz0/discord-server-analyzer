import discord
import asyncio
import os

client = discord.Client()


@client.event
async def on_ready():
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('------')
    #get_msgs()

@client.event
async def on_message(message):
    print(message.author)
    if message.content.startswith('<:what:357486247746207745>') and str(message.author) == 'claay#4176':
        #await client.delete_message(message)
        with open(str(message.channel)+'_messages.txt', 'a') as the_file:
            async for log in client.logs_from(message.channel, limit=1000000000000000):
                  stringTime = log.timestamp.strftime("%Y-%m-%d %H:%M:%S")
                  try:
                      author = log.author
                  except:
                      author = ''                
                  try:
                      mentions = log.raw_mentions
                  except:
                      mentions = []
                  
                  message = str(log.content.replace('\n',' ').replace('&','& '))

                  template = '{server_id}&&&{channel_id}&&&{id}&&&{stringTime}&&&{author}&&&{message}&&&{mentions}\n'
                  try:
                      the_file.write(template.format(server_id=str(log.channel.server.id),
                                                     channel_id=str(log.channel.id),
                                                     id=log.id, stringTime=stringTime,
                                                     author=author, message=message,
                                                     mentions=mentions))
                  except:
                      author = log.author.discriminator
                      the_file.write(template.format(server_id=str(log.channel.server.id),
                                                     channel_id=str(log.channel.id),
                                                     id=log.id, stringTime=stringTime,
                                                     author=author, message=message,
                                                     mentions=mentions))
#                   print(template.format(server_id=str(log.channel.server.id),
#                                                      channel_id=str(log.channel.id),
#                                                      id=log.id, stringTime=stringTime,
#                                                      author=author, message=message,
#                                                      mentions=mentions))

client.run(os.environ.get('discord_token'), bot=False)