import re
from datetime import datetime
import logging
import logging.handlers
import pandas as pd
import json
from config import LOG_FILENAME


LINE_REGEX = re.compile(r'^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (?P<thread>Thread-\d+) (?P<lvl>[^ ]+) +(?P<msg>.*)$')


class RiotLog(object):
    def __init__(self, path):
        self.lines = []
        timestamps = []

        with open(path) as fh:
            for line in fh:
                m = LINE_REGEX.match(line.strip())
                if m is None:
                    continue
                dct = m.groupdict()
                ts = datetime.strptime(dct['ts'], "%Y-%m-%d %H:%M:%S.%f")
                dct['ts'] = ts
                timestamps += [ts]
                self.lines += [dct]

        self.start, self.end = min(timestamps), max(timestamps)

    def filter(self, **params):
        KEYS = "ts thread lvl msg".split(' ')
        assert all([k in KEYS for k in params.keys()])
        for line_dct in self.lines:
            if any([re.search(v, line_dct[k]) is None for k, v in params.items()]):
                continue
            yield line_dct

    def as_df(self, **params):
        filtered = self.filter(**params)
        return pd.DataFrame(filtered)

    def ntasks_since(self, td):
        '''Return the numer of tasks done in the log's last 'td' timedelta, None if the window starts before the log'''
        window_start = self.end - td
        if window_start < self.start:
            return None
        df = self.as_df(msg="Task")
        filtered = df[df.ts > window_start]
        return filtered.shape[0]

    def newgames(self, td):
        window_start = self.end - td
        if window_start < self.start:
            return None
        df = self.as_df(msg="total_games")
        filtered = df[df.ts > window_start]
        if filtered.shape[0] < 2:
            return None
        earliest, latest = filtered.iloc[0], filtered.iloc[-1]

        def get_counts(msg):
            regex = re.compile(r'total_games/new_games\t(?P<total>\d+)\t(?P<new>\d+)')
            dct = regex.match(msg).groupdict()
            return int(dct['total']), int(dct['new'])

        start_total, start_new = get_counts(earliest.msg)
        end_total, end_new = get_counts(latest.msg)

        if end_total == start_total:
            return 0

        return float(end_new - start_new) / (end_total - start_total)

    def last_counts(self):
        df = self.as_df(msg="GAME COUNTS")
        if df.shape[0] == 0:
            return None
        last = df.iloc[-1]
        return last.ts, json.loads(last.msg.split('\t')[-1])


def init_logging():
    DATEFMT = "%Y-%m-%d %H:%M:%S"
    # Set up a specific logger with our desired output level
    requests_logger = logging.getLogger('requests')
    requests_logger.setLevel(logging.WARNING)
    es_logger = logging.getLogger("elasticsearch")
    es_logger.setLevel(logging.WARNING)
    fmt = '%(asctime)s.%(msecs)03d %(threadName)s %(levelname)-8s %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=fmt, datefmt=DATEFMT, filemode='w')

    # Make a rotating file log
    handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1000000)
    formatter = logging.Formatter(fmt, datefmt=DATEFMT)
    handler.setFormatter(formatter)
    root_logger = logging.getLogger('')
    root_logger.addHandler(handler)


if __name__ == "__main__":
    LOGFILE = '/home/blagarde/riotscrape/riotscrape.log'
    RiotLog(LOGFILE)
