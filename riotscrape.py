# -*- coding: utf-8 -*-
from threading import Thread, Lock
from collections import defaultdict
from config import KEYS, ES_NODES, GAME_DOCTYPE, RIOT_GAMES_INDEX
from config import GAME_SET, USER_SET, GAME_QUEUE, USER_QUEUE
from riotwatcher.riotwatcher import RiotWatcher, EUROPE_WEST, RateLimit
from elasticsearch import Elasticsearch
from time import sleep
from redis import StrictRedis
import logging
from report import GameCounterThread
from log import init_logging
from utils import split_seq, load_as_set, squelch_errors
from argparse import ArgumentParser


ALL_USERS = '100k_users.txt'  # arbitrary
ALL_GAMES = 'games.txt'  # all IDs we know about
DONE_GAMES = 'gamesrito.txt'  # in rito


class CustomRedis(StrictRedis):
    '''Redis pimped up with a few bulk operations
    WATCH OUT - NOT THREAD SAFE. LOCKING IS YOUR OWN RESPONSIBILITY'''

    def init(self):
        print("**LOAD**\n")
        all_games = load_as_set(ALL_GAMES)
        done_games = load_as_set(DONE_GAMES)
        self._bulk_sadd(GAME_SET, done_games)
        new_games = all_games.difference(done_games)
        if new_games:
            self.lpush(GAME_QUEUE, *new_games)

        all_users = load_as_set(ALL_USERS)
        self._bulk_sadd(USER_SET, all_users)
        self.lpush(USER_QUEUE, *all_users)
        print("**START**\n")

    def _intersect(self, keyspace, lst, insert=True):
        '''Check whether each element of 'lst' is in a redis set at 'keyspace'.
        Optionally insert.'''
        p = self.pipeline()
        for i in lst:
            p.sismember(keyspace, i)
        res = zip(lst, p.execute())
        if insert:
            self._bulk_sadd(keyspace, lst)
        return res

    def _bulk_sadd(self, set_name, lst, step=1000):
        '''SADD multiple items from 'lst' to 'set_name'.'''
        for seq in split_seq(lst, step):
            self.sadd(set_name, *seq)

    def _bulk_rpop(self, queue_name, n):
        '''RPOP 'n' items from 'queue_name'.'''
        p = self.pipeline()
        for _ in range(n):
            p.rpop(queue_name)
        return [i for i in p.execute() if i is not None]


class Tasks(object):
    redis = CustomRedis()
    lock = Lock()
    new_games, total_games = 0, 0

    @classmethod
    def get(cls):
        '''Return a list of games and users to scrape'''
        NTASKS = 30

        with cls.lock:
            games = cls.redis._bulk_rpop(GAME_QUEUE, NTASKS)
            new_games = [g for g, is_old in cls.redis._intersect(GAME_SET, games) if not is_old]

            users = cls.redis._bulk_rpop(USER_QUEUE, NTASKS - len(new_games))
            users = [u for u, is_old in cls.redis._intersect(USER_SET, users)]

            return new_games, users

    @classmethod
    def add(cls, games, users):
        '''Stack some new game IDs and user IDs onto Redis and store the number of games processed'''
        with cls.lock:
            new_games = [g for g, is_old in cls.redis._intersect(GAME_SET, games, insert=False) if not is_old]
            if new_games:
                cls.redis.lpush(GAME_QUEUE, *new_games)

            # TODO - add a user to redis if not seen for a long time
            new_users = [u for u, is_old in cls.redis._intersect(USER_SET, users, insert=False) if not is_old]
            if new_users:
                cls.redis.lpush(USER_QUEUE, *new_users)

            with open(ALL_USERS, 'a') as ufh:
                for u in new_users:
                    ufh.write("%s\n" % u)

            cls.total_games += len(games)
            cls.new_games += len(new_games)


class WatcherThread(Thread):

    def __init__(self, key, *args, **kwargs):
        self.reqs = defaultdict(int)
        self.watcher = RiotWatcher(key, limits=(RateLimit(10, 10), RateLimit(500, 600)))
        self.ES = Elasticsearch(ES_NODES)
        super(WatcherThread, self).__init__(*args, **kwargs)

    def run(self):
        '''Main loop. Get tasks and execute them. (1 task == 1 request).'''
        while True:
            self.games, self.users = [], []
            games, users = Tasks.get()
            logging.debug("Fetched (games/users):\t%s\t%s" % (len(games), len(users)))
            for taskname, lst in [('game', games), ('user', users)]:
                for arg in lst:
                    while True:
                        if self.watcher.can_make_request():
                            task = getattr(self, 'do_' + taskname)
                            logging.info("Task:\t%s\t%s" % (taskname, arg))
                            task(arg)
                            break
                        else:
                            sleep(0.001)
            Tasks.add(set(self.games), set(self.users + users))
            sleep(0.001)

    @squelch_errors
    def do_user(self, userid):
        games = self.watcher.get_recent_games(userid, region=EUROPE_WEST)['games']
        for game_dct in games:
            fellow_players = [dct['summonerId'] for dct in game_dct['fellowPlayers']]
            self.users += fellow_players
            gameid = game_dct['gameId']
            self.games += [gameid]

    @squelch_errors
    def do_game(self, gameid):
        dumpme = self.watcher.get_match(gameid, region=EUROPE_WEST, include_timeline=True)
        self.ES.index(index=RIOT_GAMES_INDEX, doc_type=GAME_DOCTYPE, id=gameid, body=dumpme)
        participants = [dct['player']['summonerId'] for dct in dumpme['participantIdentities']]
        self.users += participants


class LoggingThread(Thread):
    def run(self):
        LOG_INTERVAL = 10  # seconds
        while True:
            logging.info("total_games/new_games\t%s\t%s" % (Tasks.total_games, Tasks.new_games))
            sleep(LOG_INTERVAL)


class Scraper(object):
    def __init__(self):
        self.threads = [WatcherThread(key) for key in KEYS]

    def scrape(self):
        for t in self.threads:
            t.start()
        LoggingThread().start()
        GameCounterThread().start()


if __name__ == "__main__":
    ap = ArgumentParser(description="Scrape games from the Riot API")
    ap.add_argument('-i', '--init', action="store_true", help="Initialize Redis stacks with the contents of game and user files")
    args = ap.parse_args()

    init_logging()
    if args.init:
        Tasks.redis.init()

    s = Scraper()
    s.scrape()
