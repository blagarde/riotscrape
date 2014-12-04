from argparse import ArgumentParser
from log import LOG_FILENAME, RiotLog
from datetime import timedelta
from redis import StrictRedis
from config import GAME_SET, USER_SET, GAME_QUEUE, USER_QUEUE


class RedisReport(StrictRedis):
    def display(self):
        print "\n*** Redis Status ***"
        for s in GAME_SET, USER_SET:
            print "%s:\t%s" % (s, self.scard(s))
        for q in GAME_QUEUE, USER_QUEUE:
            print "%s:\t%s" % (q, self.llen(q))


TIME_WINDOWS = [1, 5, 15, 60, 60 * 24]  # Minutes


if __name__ == "__main__":
    ap = ArgumentParser(description="Analyse current Scraper log")
    ap.add_argument("-l", "--logfile", default=LOG_FILENAME, help="Log File to analyse")
    ap.add_argument("-t", "--time", type=float, nargs='+', default=TIME_WINDOWS, help="take lines more recent than that many minutes")

    args = ap.parse_args()
    RL = RiotLog(args.logfile)

    timedeltas = [timedelta(0, m * 60) for m in args.time]
    fmt = "%18s" * (len(args.time) + 1)
    print fmt % tuple(["Time window"] + timedeltas)

    def print_task_stats(msg_filter, object_name):
        f = lambda x: float(x) if x is not None else 0
        ntasks = [RL.ntasks_since(msg_filter, td) for td in timedeltas]
        print fmt % tuple(["N %s" % object_name] + ntasks)
        print fmt % tuple(["%s/sec" % object_name] + [f(n) / td.total_seconds() for n, td in zip(ntasks, timedeltas)])

    print_task_stats("Task", "Tasks")
    print_task_stats("Task:\tgame", "Games")
    print_task_stats("Task:\tuser", "Users")

    print fmt % tuple(["New games ratio"] + [RL.newgames(td) for td in timedeltas])

    rr = RedisReport()
    rr.display()

    res = RL.last_counts()
    if res is None:
        raise SystemExit("No Games per user request available")
    ts, dct = res
    print "\nGames per user - as of", ts
    for k in sorted(map(int, dct)):
        print "%s games:\t%s users" % (k, dct[str(k)])
