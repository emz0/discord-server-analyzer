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

    def run(self):
        active_stats_queries = settings.ACTIVE_STATS
        #print(self._query_stats(active_stats_queries[0]))