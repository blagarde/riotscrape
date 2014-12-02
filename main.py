# -*- coding: utf-8 -*-
import sys
from threading import Thread, Lock
from collections import defaultdict
from itertools import chain
from config import KEYS, ES_NODES, GAME_DOCTYPE, RIOT_INDEX
from riotwatcher.riotwatcher import RiotWatcher, EUROPE_WEST, RateLimit
from elasticsearch import Elasticsearch
from time import sleep
from redis import StrictRedis


ALL_USERS = '100k_users.txt'  # arbitrary
ALL_GAMES = 'games.txt'  # all IDs we know about
DONE_GAMES = 'done_games.txt'  # in rito
ES = Elasticsearch(ES_NODES)


def squelch_errors(method):
    '''
    Decorator to squelch most errors.
    Just output them to stderr
    '''
    def raising(fn):
        def run(*args):
            try:
                return fn(*args)
            except Exception as e:
                tpl = (args[0].name, fn.__name__, e.__class__.__name__, str(e))
                sys.stderr.write("%s\tException squelched inside:'%s': %s (%s)\n" % tpl)
                sys.stderr.flush()
                pass
        return run
    return raising(method)


class Tasks(object):
    redis = StrictRedis()
    lock = Lock()
    newness = 0

    @classmethod
    def init(cls):

        def load(filename):
            return set([l.rstrip() for l in open(filename) if l.strip() != ''])
        print("**LOAD**\n")
        all_games = load(ALL_GAMES)
        done_games = load(DONE_GAMES)
        cls.redis.sadd('games', done_games)
        cls.redis.lpush('game_queue', all_games.difference(done_games))

        all_users = load(ALL_USERS)
        cls.redis.sadd('users', all_users)
        cls.redis.lpush('user_queue', all_users)
        print("**START**\n")

    @classmethod
    def get(cls):
        '''Return a list of games and users to scrape'''
        NTASKS = 1000
        with cls.lock:
            games = cls.redis.rpop('game_queue', 0, NTASKS)
            new_games = [g for g, is_old in cls._intersect('games', games) if not is_old]

            users = cls.redis.rpop('user_queue', 0, NTASKS - len(new_games))
            users = [u for u, is_old in cls._intersect('users', users)]

            return new_games, users

    @classmethod
    def add(cls, games, users):
        '''Stack some new game IDs and user IDs onto Redis'''
        with cls.lock:
            new_games = [g for g, is_old in cls._intersect('games', games, insert=False) if not is_old]
            ngames = len(games)
            cls.newness = (float(len(new_games)) / ngames) if ngames > 0 else 0
            cls.redis.lpush('game_queue', new_games)

            # TODO - add a user to redis if not seen for a long time
            new_users = [u for u, is_old in cls._intersect('users', users, insert=False) if not is_old]
            cls.redis.lpush('user_queue', new_users)

            with open(ALL_USERS, 'a') as ufh:
                for u in new_users:
                    ufh.write("%s\n" % u)

    @classmethod
    def _intersect(cls, keyspace, lst, insert=True):
        '''Check whether each element of 'lst' is in a redis set at 'keyspace'.
        Optionally insert.'''
        p = cls.redis.pipeline()
        for i in lst:
            p.sismember(keyspace)
        if insert:
            p.sadd(keyspace, *lst)
        return zip(lst, p.execute())


class WatcherThread(Thread):

    def __init__(self, key, *args, **kwargs):
        self.reqs = defaultdict(int)
        self.watcher = RiotWatcher(key, limits=(RateLimit(10, 10), RateLimit(500, 600)))
        super(WatcherThread, self).__init__(*args, **kwargs)

    def run(self):
        while True:
            self.games, self.users = [], []
            games, users = Tasks.get()
            for taskname, lst in [('game', games), ('user', users)]:
                for arg in lst:
                    while True:
                        if self.watcher.can_make_request():
                            task = getattr(self, 'do_' + taskname)
                            task(arg)
                            break
                        else:
                            sleep(0.001)
            Tasks.add(set(self.games), set(self.users + users))
            sleep(0.001)

    @squelch_errors
    def do_user(self, userid):
        games = self.watcher.get_recent_games(userid, region=EUROPE_WEST)['games']
        self.reqs['users'] += 1
        for game_dct in games:
            fellow_players = [dct['summonerId'] for dct in game_dct['fellowPlayers']]
            self.users += fellow_players
            gameid = game_dct['gameId']
            self.games += [gameid]

    @squelch_errors
    def do_game(self, gameid):
        dumpme = self.watcher.get_match(gameid, region=EUROPE_WEST, include_timeline=True)
        self.reqs['games'] += 1
        ES.index(index=RIOT_INDEX, doc_type=GAME_DOCTYPE, id=gameid, body=dumpme)
        participants = [dct['player']['summonerId'] for dct in dumpme['participantIdentities']]
        self.users += participants


class Scraper(object):
    def __init__(self):
        self.threads = [WatcherThread(key) for key in KEYS]
        self.display()
        Tasks.init()
        self.display()

    def scrape(self):
        for t in self.threads:
            t.start()
        while True:
            self.display()
            sleep(0.1)

    def display(self):
        fmt = "\r" + '\t'.join(["u%s/g%s (%%s/%%s)" % (i + 1, i + 1) for i in range(len(self.threads))])
        # out = fmt % tuple(list(range(len(self.threads))) + ['Users', 'Games', 'Index'])
        # out = fmt %
        counts = [(t.reqs['users'], t.reqs['games']) for t in self.threads]
        out = fmt % tuple(chain(*counts))
        out += '\t' + '\t'.join([str(sum(i)) for i in zip(*counts)])
        out += '\t' + '\t'.join(map(str, [Tasks.newness])) + '        '
        sys.stdout.write(out)


if __name__ == "__main__":
    s = Scraper()
    s.scrape()
