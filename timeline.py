from db.client import PGClient
from collections import ChainMap
import settings


class MemberTraffic:

    def __init__(self):
        self.client = PGClient()
        self.jh_bot_id = settings.JH_BOT_ID


    def get_member_traffic(self):

        q_traffic = """
                        WITH bot_messages AS(
                            SELECT id, posted_at, content
                            FROM messages
                            WHERE author_id = %s
                            AND (content LIKE '%left%' OR content LIKE '%great%')
                            ORDER BY posted_at)
                        )
                        SELECT json_agg(
                            json_build_object('id',id,'posted_at',posted_at,'content',content)) AS js
                        FROM bot_messages
                    """
        
        q_member_ids = """
                        SELECT json_agg(member_ids) 
                        FROM (select json_build_object(name, array_agg(id)) as member_ids
                            FROM members GROUP BY name) AS foo"""

member_ids = client.query(q_member_ids).fetchone()[0]
member_ids = dict(ChainMap(*member_ids))

traffic = client.query(q_traffic).fetchone()[0]

traffic = traffic

parsed_traffic = []
member_count = 26
for t in traffic:
    if 'left' in t['content']:
        member = t['content'].split('**')[1]
        #print(member)
        member_count -= 1
    else:
        member = t['content'][2:].split('>')[0]
        member = [m for m, ids in member_ids.items() if int(member) in ids][0]
        member_count += 1

    parsed_traffic.append((t['posted_at'], member, member_count))
print(parsed_traffic)