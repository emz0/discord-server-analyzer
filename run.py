from db.client import PGClient
from analyzer.analyzer import Analyzer
import downloader.downloader as Downloader
import sys


def run():
    # Downloader.run()
    Analyzer().run()

# if __name__ == '__main__':
#     print(sys.argv[1:])
