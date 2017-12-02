import json

all_mentions = {}
with open('general_messages.txt','r') as file:
    message = file.readline()
    while message:
        message = message[:-1].split('&&&')
        author, mentions = message[4], eval(message[6])
        for m in mentions:
            if author not in all_mentions:
                all_mentions[author] = {}
            if m not in all_mentions[author]:
                all_mentions[author][m] = 1
            else:
                all_mentions[author][m] += 1
        message = file.readline()


with open('mentions.txt','w') as m_file:
    m_file.write(json.dumps(all_mentions))