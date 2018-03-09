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

        message_exists = self.query("""SELECT count(*) as count FROM
                                        messages WHERE id=%s""",
                                        (id,))
        message_exists = message_exists.fetchone()[0]

        mentions = '{{{}}}'.format(','.join(mentions))
        if message_exists > 0:
            self.query("""
                UPDATE messages
                SET server_id= %s, channel_id = %s, posted_at = %s,
                member_id = %s, content = %s, mentions = %s
                WHERE id = %s
            """, (server_id, channel_id, posted_at, member_id,
                  content, mentions, id))
        else:
            self.query("""
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

        member_exists = self.query("""SELECT count(*) FROM
                                       members WHERE id = %s""", (id,))
        member_exists = member_exists.fetchone()[0]
        if member_exists == 0:
            self.query("""INSERT INTO members (id, name, discriminator, joined_at)
                          VALUES (%s, %s, %s, %s)""",
                        (id, name, discriminator, joined_at))

    def save_reactions(self, reactions):
        for current_r in reactions:
            q_existing_r = """
                SELECT id, member_id::varchar
                FROM reactions
                WHERE message_id = %s
                AND emote_id = %s
            """
            existing_r = self.query(q_existing_r,
                                    (current_r['message_id'],
                                     current_r['emote_id']
                                     )).fetchall()

            existing_r_members = [str(r[1]) for r in existing_r]
            new_r_members = [m_id for m_id in current_r['members']
                             if m_id not in existing_r_members]
            deleted_r_ids = []
            for e_r in existing_r:

                if e_r[1] not in current_r['members']:

                    deleted_r_ids.append(e_r[0])

            if deleted_r_ids:
                self.query("DELETE FROM reactions WHERE id IN %s",
                           (tuple(deleted_r_ids), ))

            for m_id in new_r_members:
                insert_q = """
                        INSERT INTO reactions
                        (message_id, emote_id, member_id)
                        VALUES (%s, %s, %s)"""
                values = (current_r['message_id'], current_r['emote_id'], m_id)

                self.query(insert_q, values)

    def save_emotes(self, emotes):
        for e_id, props in emotes.items():
            q_existing_id = """
                            SELECT id FROM emotes
                            WHERE emote_id = %s AND member_id = %s
                            """
            values = (e_id, props['member_id'])

            existing_id = self.query(q_existing_id, values).fetchone()

            if existing_id:
                q_update = """
                            UPDATE emotes
                            SET count = count + %s,
                            name = %s
                            WHERE id = %s"""

                values = (props['count'], props['name'], existing_id[0])

                self.query(q_update, values)

            else:
                q_insert = """INSERT INTO emotes
                              (emote_id, member_id, name, posted_at, count)
                               VALUES (%s, %s, %s, %s, %s)"""
                values = (e_id, props['member_id'], props['name'],
                          props['posted_at'], props['count'])

                self.query(q_insert, values)
