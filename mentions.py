import json
import re
from collections import OrderedDict

def mentions():
    all_mentions = {}
    with open('general_messages.txt','r') as file:
        message = file.readline()
        while message:
            message = message[:-1].split('&&&')
            author, mentions = message[4], eval(message[6])
            if author not in all_mentions:
                    all_mentions[author] = {}
            for m in mentions:
                if m not in all_mentions[author]:
                    all_mentions[author][m] = 1
                else:
                    all_mentions[author][m] += 1
            message = file.readline()


    # with open('mentions.txt','w') as m_file:
    #     m_file.write(json.dumps(all_mentions))
    return all_mentions

def mentioned_times():
    all_mentions = mentions()
    mentioned = {}

    for k, players in all_mentions.items():
        for player, count in players.items():
            #print(i)
            #i += 1
            if player not in mentioned:
                #print('mentioned['+player+']=1')
                mentioned[player] = int(count)
            else:
                mentioned[player] += int(count)
                #print('mentioned['+player+']='+str(mentioned[player]))
    return [(k, mentioned[k]) for k in sorted(mentioned, key=mentioned.get, reverse=True)]
    #return mentioned

def used_mention_times():
    all_mentions = mentions()
    used_mention = []
    for player, mentioned in all_mentions.items():
            used_mention.append((player,sum(mentioned.values())))

    return sorted(used_mention, key=lambda x: x[1], reverse=True)

def member_emotes():
    m_emotes = {}
    pattern = '<:\w+:[0-9]+>'
    with open('general_messages.txt','r') as file:
        message = file.readline()
        while message:
            message = message[:-1].split('&&&')
            author = message[4]
            emotes = re.findall(pattern, message[5])
            if author not in m_emotes:
                    m_emotes[author] = OrderedDict()
            for e in emotes:
                e_name, e_id = e[2:-1].split(':')                
                if e_id not in m_emotes[author]:
                    m_emotes[author][e_id] = {
                                                    'name':e_name, 
                                                    'count':1}
                else:
                    m_emotes[author][e_id]['count'] += 1
                m_emotes[author] = OrderedDict(sorted(
                        m_emotes[author].items(), key=lambda x: x[1]['count'],reverse=True))
            #m_emotes[author] = sorted(m_emotes[author],key=lambda x: x.count, reverse=True)
            message = file.readline()
    return m_emotes

def global_emotes():
    g_emotes = {}
    pattern = '<:\w+:[0-9]+>'
    with open('general_messages.txt','r') as file:
        message = file.readline()
        while message:
            message = message[:-1].split('&&&')
            emotes = re.findall(pattern, message[5])
            for e in emotes:
                e_name, e_id = e[2:-1].split(':')
                if e_id not in g_emotes:
                    g_emotes[e_id] = {'name':e_name,'count':1}
                else:
                    g_emotes[e_id]['count'] += 1
            message = file.readline()
    
    g_emotes =sorted(g_emotes.items(), key=lambda x: x[1]['count'],reverse=True)
    return g_emotes

ems = global_emotes()
print(ems)


    