from argparse import ArgumentParser
from log import LOG_FILENAME, RiotLog
from datetime import timedelta


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
    print fmt % tuple(["N Tasks"] + [RL.ntasks_since(td) for td in timedeltas])
    print fmt % tuple(["New games ratio"] + [RL.newgames(td) for td in timedeltas])
