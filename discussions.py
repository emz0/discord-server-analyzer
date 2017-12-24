from db.client import PGClient
from collections import ChainMap
from datetime import datetime
client = PGClient()

q_messages = """SELECT json_build_object('id',id,'posted_at',posted_at,'content',content, 'author_name', author_name) FROM messages 
                WHERE author_id NOT IN (155149108183695360, 349547918966915073, 159985870458322944)
                ORDER BY id
                """ 

cursor = client.query(q_messages)
message = cursor.fetchone()[0]
discussions = {message['id']: [message]}
curr_disc_id = message['id']
previous_message = datetime.strptime(message['posted_at'], '%Y-%m-%dT%H:%M:%S')
print(last_message)
i = 0
while message:
    i += 1
    if i == 50:
        print(discussions)
        break
    message_date = datetime.strptime(message['posted_at'], '%Y-%m-%dT%H:%M:%S')
    time_diff = (message_date - previous_message).total_seconds() / 60
    if(time_diff > 30):
        curr_disc_id = message['id']
        discussions[curr_disc_id] = [message]
        print('message id:'+str(message['id']))
        previous_message = message_date
        
    else:
        discussions[curr_disc_id].append(message)
    message = cursor.fetchone()[0]
    #print(message)