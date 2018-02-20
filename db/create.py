import psycopg2 as pg


class Migration:

    def __init__(self, user='postgres', host='localhost',
                 password='123456', dbname='jh'):
        self.con = pg.connect(user=user, host=host,
                              password=password, dbname=dbname)
        self.con.autocommit = True
        self.cur = self.con.cursor()

    def create_tables(self):
        table_message = """
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

        table_reactions = """
            CREATE TABLE reactions (
                id SERIAL PRIMARY KEY NOT NULL,
                message_id BIGINT NOT NULL,
                emote_id VARCHAR(64) NOT NULL,
                members BIGINT[]
            )
        """

        table_members = """
            CREATE TABLE members (
                id BIGINT PRIMARY KEY NOT NULL,
                name VARCHAR(64),
                discriminator INT
            )
        """

        table_emotes = """
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
        self.cur.execute("DROP TABLE IF EXISTS emotes")
        # self.cur.execute(table_message)
        # self.cur.execute(table_reactions)
        # self.cur.execute(table_members)
        self.cur.execute(table_emotes)


    def import_messages(self, filepath='general_messages.txt'):
        with open('general_messages.txt', 'r') as file:
            message = file.readline()
            #for i in range(0,20):
                #print(message)
            while message:
                message = message[:-1].split('&&&')
                message[6] = message[6].replace('[', '{').replace(']', '}')
                message[6] = message[6].replace("'", "")
                s_id, ch_id, m_id, time, author, content, mentions = message
                #print(s_id, ch_id, m_id, time, author, content, mentions)
                #break
                self.cur.execute("""INSERT INTO messages
                (id,server_id,channel_id,posted_at,author,content,mentions)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (m_id, s_id, ch_id, time, author, content, mentions))

                message = file.readline()

    def run(self, import_data=True):
        self.create_tables()

m = Migration()
m.run()
