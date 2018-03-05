from db.client import PGClient
from analyzer.analyzer import Analyzer
import downloader.downloader as Downloader
import sys


def run(argv):
    if argv == 'downloader':
        Downloader.run()
    elif argv == 'analyzer':
        Analyzer().run()

if __name__ == '__main__':
    print(len(sys.argv))
    Downloader.discord_client.close()
    if len(sys.argv) < 2:
        run('analyzer')
    else:
        run(sys.argv[1])
    sys.exit()
