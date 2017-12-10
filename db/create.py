import psycopg2 as pg


class Migration:

    def __init__(self, user='postgres', host='localhost',
                 password='123456', dbname='jh'):
        self.con = pg.connect(user=user, host=host,
                              password=password, dbname=dbname)
        self.con.autocommit = True
        self.cur = self.con.cursor()

    def create_table_messages(self):
        table_message = """
            CREATE TABLE messages (
                id BIGINT PRIMARY KEY NOT NULL,
                server_id BIGINT NOT NULL,
                channel_id BIGINT NOT NULL,
                posted_at TIMESTAMP NOT NULL,
                author VARCHAR(64) NOT NULL,
                content TEXT,
                mentions VARCHAR[]
            )
        """
        self.cur.execute("DROP TABLE IF EXISTS messages")
        self.cur.execute(table_message)

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
        self.create_table_messages()
        self.import_messages()

m = Migration()
m.run()
