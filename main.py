from sql_stats import SQLStats
from discussions import DiscussionAnalyzer
import settings
import time
import pathlib
import csv


class DiscordAnalyzer:

    def __init__(self):
        pass

    def export(self, data):
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

    def run(self):
        results = [
            ('most_reacting', SQLStats().most_reacting()),
            ('most_reacted_by_count', SQLStats().most_reacted()),
            ('most_reacted_by_ratio', SQLStats().most_reacted(order_by_i=1)),
            ('activity_trend', SQLStats().activity_trend()),
            ('active_hours', SQLStats().active_hours()),
            ('active_days', SQLStats().active_days()),
            ('most_used_emotes', SQLStats().most_used_emotes())
        ]

        for d in settings.DISCUSSIONS:
            d_analyzed = DiscussionAnalyzer(d['channel_id'],
                                            d['first_id'],
                                            d['last_id'],
                                            type='both').analyze()
            d_analyzed[0] = ('discussion_0', d_analyzed[0])
            d_analyzed[1] = ('discussion_1', d_analyzed[1])
            results += d_analyzed

        self.export(results)

DiscordAnalyzer().run()
