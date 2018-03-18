import settings
from datetime import timedelta
from db.client import PGClient


class Activity:

    def __init__(self):
        self.client = PGClient()

    def per_day(self):
        """
        Return average amount of messages per day in a week
        """
        q_grouped_days = """
            SELECT extract(isodow from posted_at)::int AS day_group, count(*)
            FROM messages
            WHERE member_id NOT IN %s
            GROUP BY day_group
            ORDER BY day_group
            """
        q_first_msg = """
            SELECT date_trunc('day', posted_at)
            FROM messages
            WHERE member_id NOT IN %s
                AND posted_at >= '2017-08-01'
            ORDER BY posted_at
            LIMIT 1
        """
        q_last_msg = """
            SELECT date_trunc('day', posted_at)
            FROM messages
            WHERE member_id NOT IN %s
            ORDER BY posted_at DESC
            LIMIT 1
        """

        values = (settings.IGNORED_MEMBER_IDS,)

        grouped_days = self.client.query(q_grouped_days, values).fetchall()
        sums_per_day = [s[1] for s in grouped_days]

        date_from = self.client.query(q_first_msg, values).fetchall()[0][0]
        date_to = self.client.query(q_last_msg, values).fetchall()[0][0]

        avg_activity = self.get_avg_activity(date_from=date_from,
                                             date_to=date_to,
                                             sums_per_segment=sums_per_day,
                                             time_unit='day')
        return avg_activity

    def per_hour(self):
        """
        Return average amount of messages per hour in a day
        """
        q_grouped_hours = """
            SELECT extract(hour from posted_at)::int AS hour_group, count(*)
            FROM messages
            WHERE member_id NOT IN %s
            GROUP BY hour_group
            ORDER BY hour_group
            """
        q_first_msg = """
            SELECT date_trunc('hour', posted_at)
            FROM messages
            WHERE member_id NOT IN %s
                AND posted_at >= '2017-08-01'
            ORDER BY posted_at
            LIMIT 1
        """
        q_last_msg = """
            SELECT date_trunc('hour', posted_at)
            FROM messages
            WHERE member_id NOT IN %s
            ORDER BY posted_at DESC
            LIMIT 1
        """
        values = (settings.IGNORED_MEMBER_IDS,)

        grouped_hours = self.client.query(q_grouped_hours, values).fetchall()
        sums_per_hour = [s[1] for s in grouped_hours]

        date_from = self.client.query(q_first_msg, values).fetchall()[0][0]
        date_to = self.client.query(q_last_msg, values).fetchall()[0][0]

        avg_activity = self.get_avg_activity(date_from=date_from,
                                             date_to=date_to,
                                             sums_per_segment=sums_per_hour,
                                             time_unit='hour')
        return avg_activity

    def get_avg_activity(self, date_from, date_to,
                         sums_per_segment, time_unit):
        """
        Return average amount of messages
        per the chosen time segment (hour or day)
        """
        if time_unit == 'day':
            time_segments = [0] * 7
            td = date_to - date_from + timedelta(days=1)
            complete_period = int(td.days / 7)
            remainder = td.days % 7
            first_segment = date_from.weekday()
        else:
            time_segments = [0] * 24
            td = date_to - date_from + timedelta(hours=1)
            complete_period = int(td.total_seconds()/60/60/24)
            remainder = td.days % 7
            first_segment = date_from.hour

        for segment in range(len(time_segments)):
            time_segments[segment] = complete_period

        for i in range(remainder):
            time_segments[(first_segment + i) % len(time_segments)] += 1

        avg_sum_per_segment = [(time_unit, 'count')]
        n = 0
        for seg_sum, seg in zip(sums_per_segment, time_segments):
            avg_sum = round(seg_sum/seg)
            avg_sum_per_segment.append((n, avg_sum))
            n += 1

        return avg_sum_per_segment
