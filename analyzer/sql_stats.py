import settings
from analyzer.discussions import DiscussionAnalyzer
from db.client import PGClient


class SQLStats:

    def __init__(self):
        self.client = PGClient()
        self.limit = settings.RESULT_LIMIT

    def query_stats(self, q, values):
        """
        Execute query and return list
        of tuples
        """
        cursor = self.client.query(q, values)
        column_names = list(zip(*cursor.description))[0]

        return [column_names, *cursor.fetchall()]

    def most_reacting(self):
        q = """
            SELECT r.member_id, m.name, count(*)
            FROM reactions r
            JOIN members m ON r.member_id = m.id
            WHERE r.member_id NOT IN %s
            GROUP BY r.member_id, m.name
            ORDER BY count DESC
            LIMIT %s
        """
        return self.query_stats(q, (settings.IGNORED_MEMBER_IDS, self.limit))

    def most_reacted(self, order_by_ratio=False):
        order_by_cols = ['num_of_reactions', 'ratio']
        if order_by_ratio:
            order_by = order_by_cols[1]
        else:
            order_by = order_by_cols[0]

        q = """
            WITH reaction_count AS (
                SELECT m.member_id, count(*) AS num_of_reactions
                FROM reactions r
                JOIN messages m ON m.id = r.message_id
                GROUP BY m.member_id
            ),
                message_count AS (
                SELECT member_id, count(*) as num_of_messages
                FROM messages
                GROUP BY member_id
            )
            SELECT m.name, rc.num_of_reactions, mc.num_of_messages,
                    rc.num_of_reactions / mc.num_of_messages::float as ratio
            FROM reaction_count rc
            JOIN message_count mc ON mc.member_id = rc.member_id
            JOIN members m ON m.id = rc.member_id
            WHERE rc.member_id NOT IN %s
            AND mc.num_of_messages >= 10
            ORDER BY {} DESC
            LIMIT %s
        """.format(order_by)

        return self.query_stats(q, (settings.IGNORED_MEMBER_IDS, self.limit))

    def activity_trend(self):
        q = """
            SELECT extract(month from posted_at)::int AS m,
                   extract(year from posted_at)::int AS y,
                   count(*)
            FROM messages
            WHERE member_id NOT IN %s
            GROUP BY y, m
            ORDER BY y,m
        """

        return self.query_stats(q, (settings.IGNORED_MEMBER_IDS,))

    def most_used_emotes(self):
        q = """
            WITH _emotes AS (
                SELECT lower(name) AS e_name, sum(count) AS e_count
                FROM emotes
                WHERE member_id NOT IN %s
                GROUP BY e_name
                ),
                _reactions AS (
                    SELECT lower(e.name) as e_name, count(*) AS r_count
                    FROM reactions r
                    JOIN emotes e ON e.emote_id = r.emote_id
                    WHERE r.member_id NOT IN %s
                    GROUP BY e_name
                )
                SELECT e.e_name, e.e_count, r.r_count, (e.e_count + r.r_count) AS total
                FROM _emotes e
                JOIN _reactions r ON r.e_name = e.e_name
                ORDER BY total DESC
                LIMIT %s;
            """
        ignored_m = 2 * (settings.IGNORED_MEMBER_IDS,)
        values = (*ignored_m, self.limit)

        return self.query_stats(q, values)

    def most_active_member(self):
        q = """
        SELECT member_id, mbr.name, count(*)
        FROM messages m
        JOIN members mbr ON mbr.id = m.member_id
        WHERE m.member_id NOT IN %s
        GROUP BY m.member_id, mbr.name
        ORDER BY count DESC
        LIMIT %s
        """

        return self.query_stats(q, (settings.IGNORED_MEMBER_IDS, self.limit))

    def most_mentioned_member(self):
        q = """
        WITH u_mentions AS (
            SELECT unnest(mentions) as _member_id, count(*)
            FROM messages
            GROUP BY _member_id
        )
        SELECT mbr.name, u.* FROM u_mentions u
        JOIN members mbr ON mbr.id = u._member_id
        WHERE u._member_id NOT IN %s
        ORDER BY u.count DESC
        LIMIT %s
        """

        return self.query_stats(q, (settings.IGNORED_MEMBER_IDS, self.limit))
