#!/usr/bin/env python
from redis import StrictRedis
from argparse import ArgumentParser


def main():
    ap = ArgumentParser(description="List a summary of Redis data structures")
    ap.add_argument('-H', '--host', default='localhost')
    ap.add_argument('-p', '--port', default=6379)
    args = ap.parse_args()
    r = StrictRedis(host=args.host, port=args.port)
    for k in sorted(r.scan()[1]):
        for kind, count in ('list', r.llen), ('sset', r.zcard), ('set', r.scard), ('hash', r.hlen):
            try:
                cnt = count(k)
                print "[%-4s] %-15s %s" % (kind, k, cnt)
            except:
                pass


if __name__ == "__main__":
    main()
