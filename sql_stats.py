import settings
from db.client import PGClient
from discussions import DiscussionAnalyzer


class SQLStats:

    def __init__(self):
        self.client = PGClient()
        self.limit = settings.RESULT_LIMIT

    def query_stats(self, q, values):
        cursor = self.client.query(q, values)
        column_names = list(zip(*cursor.description))[0]

        return [column_names, *cursor.fetchall()]

    def most_reacting(self):
        q = """
            WITH members_count AS (
                SELECT unnest(members) AS member_id, count(*) r_count
                FROM reactions
                GROUP BY member_id
                )
            SELECT m.name, mc.r_count
            FROM members_count mc
            JOIN members m ON m.id = mc.member_id
            WHERE mc.member_id NOT IN %s
            ORDER BY r_count DESC
            LIMIT %s
        """
        return self.query_stats(q, (settings.IGNORED_MEMBER_IDS, self.limit))

    def most_reacted(self, order_by_i=0):
        order_by_cols = ['num_of_reactions', 'ratio']
        if order_by_i == 0:
            order_by = order_by_cols[0]
        else:
            order_by = order_by_cols[1]

        q = """
            WITH reaction_count AS (
                SELECT sum(cardinality(r.members)) AS num_of_reactions,
                        m.member_id
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

    def active_hours(self):
        q = """
            SELECT extract(hour from posted_at)::int AS hour_group, count(*)
            FROM messages
            WHERE member_id NOT IN %s
            GROUP BY hour_group
            ORDER BY hour_group
            """

        return self.query_stats(q, (settings.IGNORED_MEMBER_IDS,))

    def active_days(self):
        q = """
            SELECT extract(isodow from posted_at)::int AS day_group, count(*)
            FROM messages
            WHERE member_id NOT IN %s
            GROUP BY day_group
            ORDER BY day_group
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
                filtered_reactions AS (
                    SELECT r.emote_id, r.members FROM reactions r
                    JOIN messages m ON m.id = r.message_id
                    WHERE m.member_id NOT IN %s
                ),
                u_reactions AS (
                    SELECT emote_id, unnest(members) AS member_id
                    FROM filtered_reactions
                ),
                _reactions AS (
                    SELECT lower(e.name) AS e_name, count(*) AS r_count
                    FROM u_reactions r
                    JOIN emotes e ON e.emote_id = r.emote_id
                    WHERE r.member_id NOT IN %s
                    GROUP BY e_name
                )
                SELECT e.e_name, e.e_count, r.r_count, (e.e_count + r.r_count) AS total
                FROM _emotes e
                JOIN _reactions r ON r.e_name = e.e_name
                ORDER BY total DESC
                LIMIT %s
        """
        ignored_m = 3 * (settings.IGNORED_MEMBER_IDS,)
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
