import settings
from db.client import PGClient
import queries as qs
from discussions import DiscussionAnalyzer


class DiscordStats:

    def __init__(self):
        self.client = PGClient()

    def _query_stats(self, q):
        cursor = self.client.query(q)
        column_names = list(zip(*cursor.description))[0]

        return [column_names, *cursor.fetchall()]

    def most_reactions(self, limit):
        q = """
            WITH members_count AS (
                SELECT unnest(members) AS member_id, count(*) r_count
                FROM reactions
                GROUP BY member_id
                )
            SELECT m.name, mc.r_count
            FROM members_count mc
            JOIN members m ON m.id = mc.member_id
            WHERE mc.member_id IN %s
            ORDER BY r_count DESC
            LIMIT %s
        """
        res = self.client.query(q, (settings.IGNORED_MEMBER_IDS, limit))
        column_names = list(zip(*cursor.description))[0]

        return [column_names, res.fetchall()]

    def most_reacted(self, limit):
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
            ORDER BY ratio DESC
            LIMIT %s
        """
        res = self.client.query(q, (settings.IGNORED_MEMBER_IDS, limit))
        column_names = list(zip(*cursor.description))[0]

        return [column_names, res.fetchall()]

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
        res = self.client.query(q, (settings.IGNORED_MEMBER_IDS))
        column_names = list(zip(*cursor.description))[0]

        return [column_names, res.fetchall()]

    def active_hours(self):
        q = """
            SELECT extract(hour from posted_at)::int AS hour_group, count(*)
            FROM messages GROUP BY hour_group
            WHERE member_id NOT IN %s
            ORDER BY hour_group
            """
        res = self.client.query(q, (settings.IGNORED_MEMBER_IDS))
        column_names = list(zip(*cursor.description))[0]

        return [column_names, res.fetchall()]

    def active_days(self):
        q = """
            SELECT extract(isodow from posted_at)::int AS day_group, count(*)
            FROM messages GROUP BY day_group
            WHERE member_id NOT IN %s
            ORDER BY day_group
            """
        res = self.client.query(q, (settings.IGNORED_MEMBER_IDS))
        column_names = list(zip(*cursor.description))[0]

        return [column_names, res.fetchall()]

    def most_used_emotes(self, limit):
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
        ignored_m = settings.IGNORED_MEMBER_IDS
        values = (ignored_m, ignored_m, ignored_m, limit)
        res = self.client.query(q, values)
        column_names = list(zip(*cursor.description))[0]

        return [column_names, res.fetchall()]

        def most_active_member(self, limit):
            q = """-
            """

        def most_popular_member(self, limit):
            q = """
            """

    def run(self):
        active_stats_queries = settings.ACTIVE_STATS
        #print(self._query_stats(active_stats_queries[0]))