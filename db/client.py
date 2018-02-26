import psycopg2 as pg
import re
import settings


class PGClient:

    def __init__(self):
        self.con = pg.connect(settings.DB_CONNECTION)
        self.con.autocommit = True

    def query(self, q, values=None):
        cursor = self.con.cursor()
        if values:
            cursor.execute(q, values)
        else:
            cursor.execute(q)

        return cursor

    def save_message(self, log):
        id = log.id
        server_id = log.channel.server.id
        channel_id = log.channel.id
        posted_at = log.timestamp
        member_id = log.author.id
        content = log.content
        mentions = [m.id for m in log.mentions]

        cursor = self.con.cursor()
        message_exists = cursor.execute("""SELECT count(*) as count FROM
                                        messages WHERE id=%s""",
                                        (id,))
        message_exists = cursor.fetchone()[0]

        mentions = '{{{}}}'.format(','.join(mentions))
        if message_exists > 0:
            cursor.execute("""
                UPDATE messages
                SET server_id= %s, channel_id = %s, posted_at = %s,
                member_id = %s, content = %s, mentions = %s
                WHERE id = %s
            """, (server_id, channel_id, posted_at, member_id,
                  content, mentions, id))
        else:
            cursor.execute("""
                INSERT INTO messages
                (id, server_id, channel_id, posted_at,
                member_id, content, mentions)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (id, server_id, channel_id, posted_at,
                 member_id, content, mentions))

    def save_member(self, member):
        id = member.id
        name = member.name
        discriminator = member.discriminator
        if hasattr(member, 'joined_at'):
            joined_at = member.joined_at
            #print(member.joined_at)
        else:
            joined_at = None

        cursor = self.con.cursor()
        member_exists = cursor.execute("""SELECT count(*) FROM
                                       members WHERE id = %s""", (id,))
        member_exists = cursor.fetchone()[0]
        if member_exists == 0:
            cursor.execute("""INSERT INTO members (id, name, discriminator, joined_at)
                            VALUES (%s, %s, %s, %s)""", (id, name, discriminator, joined_at))

        cursor.close()

    def save_reactions(self, reactions):
        cursor = self.con.cursor()
        for r in reactions:
            members = '{{{}}}'.format(','.join(r['members']))
            reaction_exists = cursor.execute("""SELECT id FROM reactions
                                             WHERE message_id = %s
                                             AND emote_id = %s""",
                                             (r['message_id'], r['emote_id']))
            reaction_exists = cursor.fetchone()
            if reaction_exists:
                cursor.execute("""UPDATE reactions
                               SET members = %s
                                 WHERE id = %s""",
                               (members, reaction_exists[0]))

            else:
                cursor.execute("""INSERT INTO reactions (message_id, emote_id,
                               members) VALUES (%s, %s, %s)""",
                               (r['message_id'], r['emote_id'], members))

    def save_emotes(self, emotes):
        cursor = self.con.cursor()
        for e_id, props in emotes.items():
            q_existing_id = """
                            SELECT id FROM emotes
                            WHERE emote_id = %s AND member_id = %s
                            """
            values = (e_id, props['member_id'])

            cursor.execute(q_existing_id, values)
            existing_id = cursor.fetchone()

            if existing_id:
                q_update = """
                            UPDATE emotes
                            SET count = count + %s,
                            name = %s
                            WHERE id = %s"""

                values = (props['count'], props['name'], existing_id[0])

                cursor.execute(q_update, values)

            else:
                q_insert = """INSERT INTO emotes
                              (emote_id, member_id, name, posted_at, count)
                               VALUES (%s, %s, %s, %s, %s)"""
                values = (e_id, props['member_id'], props['name'], props['posted_at'],
                          props['count'])

                cursor.execute(q_insert, values)
