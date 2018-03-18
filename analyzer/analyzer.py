from analyzer.sql_stats import SQLStats
from analyzer.discussions import DiscussionAnalyzer
from analyzer.activity import Activity
import settings
import time
import pathlib
import csv


class Analyzer:

    def __init__(self):
        pass

    def run(self):
        """
        Run analysis and export results to an output folder
        """
        sql = SQLStats()
        act = Activity()
        results = [
            ('most_reacting', sql.most_reacting()),
            ('most_reacted_by_count', sql.most_reacted()),
            ('most_reacted_by_ratio', sql.most_reacted(order_by_ratio=True)),
            ('activity_trend', sql.activity_trend()),
            ('active_hours', act.per_hour()),
            ('active_days', act.per_day()),
            ('most_used_emotes', sql.most_used_emotes()),
            ('most_mentioned_member', sql.most_mentioned_member())
        ]

        for d in settings.DISCUSSIONS:
            d_analyzed = DiscussionAnalyzer(d['channel_id'],
                                            d['first_id'],
                                            d['last_id'],
                                            type='both').analyze()
            d_analyzed[0] = (d['name']+'_0', d_analyzed[0])
            d_analyzed[1] = (d['name']+'_1', d_analyzed[1])
            results += d_analyzed

        self.export(results)

    def export(self, data):
        """
        Export data to an output folder in csv format
        """
        if not data:
            return
        output_dir = '{}/{}'.format(settings.OUTPUT_DIR,
                                    int(time.time()))
        pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)

        for res in data:
            name = res[0]
            values = res[1]
            print('exporting:' + name)
            filename = '{}/{}.csv'.format(output_dir, name)
            with open(filename, 'w') as out:
                csv_out = csv.writer(out)
                for row in values:
                    csv_out.writerow(row)