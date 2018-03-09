import psycopg2 as pg
import settings


con = pg.connect(settings.DB_CONNECTION)
con.autocommit = True
cur = self.con.cursor()


def create_tables():
    messages = """
        CREATE TABLE messages (
            id BIGINT PRIMARY KEY NOT NULL,
            server_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            posted_at TIMESTAMP NOT NULL,
            member_id BIGINT NOT NULL,
            content TEXT,
            mentions BIGINT[]
        )
    """

    reactions = """
        CREATE TABLE reactions (
            id SERIAL PRIMARY KEY NOT NULL,
            message_id BIGINT NOT NULL,
            emote_id VARCHAR(64) NOT NULL,
            member_id BIGINT NOT NULL
        )
    """

    members = """
        CREATE TABLE members (
            id BIGINT PRIMARY KEY NOT NULL,
            name VARCHAR(64),
            discriminator INT
        )
    """

    emotes = """
        CREATE TABLE emotes (
            id SERIAL PRIMARY KEY NOT NULL,
            emote_id VARCHAR(64) NOT NULL,
            member_id BIGINT NOT NULL,
            name VARCHAR(32),
            posted_at TIMESTAMP NOT NULL,
            count INT DEFAULT 1
        )
    """

    # self.cur.execute("DROP TABLE IF EXISTS messages")
    # self.cur.execute("DROP TABLE IF EXISTS reactions")
    # self.cur.execute("DROP TABLE IF EXISTS members")
    # cur.execute("DROP TABLE IF EXISTS emotes")
    cur.execute(messages)
    cur.execute(reactions)
    cur.execute(members)
    cur.execute(table_emotes)


create_tables()
