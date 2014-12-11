#!/usr/bin/env python
# -*- coding: utf-8 -*-
from threading import Thread, Lock
from collections import defaultdict
from config import KEYS, ES_NODES, GAME_DOCTYPE, RIOT_GAMES_INDEX, TO_CRUNCHER
from config import RIOT_USERS_INDEX, USER_DOCTYPE
from config import REDIS_PARAM, GAME_SET, USER_SET, GAME_QUEUE, USER_QUEUE
from riotwatcher import RiotWatcher, EUROPE_WEST, RateLimit
from elasticsearch import Elasticsearch
from time import sleep
from redis import StrictRedis
import logging
from report import GameCounterThread
from log import init_logging
from utils import split_seq, load_as_set
from argparse import ArgumentParser
from es_utils import bulk_upsert, get_ids
from tempfile import mkstemp


ALL_USERS = '100k_users.txt'  # arbitrary


class CustomRedis(StrictRedis):
    '''Redis pimped up with a few bulk operations
    WATCH OUT - NOT THREAD SAFE. LOCKING IS YOUR OWN RESPONSIBILITY'''

    def init(self, sync_with_cruncher=True):
        print("**LOAD**\n")
        try:
            all_users = load_as_set(ALL_USERS)
            self._bulk_sadd(USER_SET, all_users)
        except:
            raise SystemExit("Failed to load users from file: %s" % ALL_USERS)
        scraped_games = get_ids(RIOT_GAMES_INDEX, GAME_DOCTYPE)
        if sync_with_cruncher:
            crunched_games = get_ids(RIOT_USERS_INDEX, USER_DOCTYPE, nested_field='games_id_list')
            only_in_cruncher = crunched_games.difference(scraped_games)
            assert len(only_in_cruncher) == 0
            only_in_scraper = scraped_games.difference(crunched_games)
            if len(only_in_scraper) > 0:
                _, tmp_path = mkstemp()
                with open(tmp_path, 'w') as fh:
                    for gid in only_in_scraper:
                        fh.write(gid + '\n')
                tpl = (len(only_in_scraper), tmp_path)
                raise SystemExit('Please crunch these before running the scraper (%s games): %s' % tpl)
        for key in GAME_QUEUE, GAME_SET, USER_QUEUE, USER_SET:
            self.delete(key)
        self._bulk_sadd(GAME_SET, scraped_games)

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
    redis = CustomRedis(**REDIS_PARAM)
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
            if users:
                cls.redis.lpush(USER_QUEUE, *users)  # Rotate users

            return new_games, users

    @classmethod
    def add(cls, games, users):
        '''Stack some new game IDs and user IDs onto Redis and store the number of games processed'''
        with cls.lock:
            new_games = [g for g, is_old in cls.redis._intersect(GAME_SET, games, insert=False) if not is_old]
            if new_games:
                cls.redis.lpush(GAME_QUEUE, *new_games)

            # TODO - add a user to redis if not seen for a long time

            cls.total_games += len(games)
            cls.new_games += len(new_games)

    @classmethod
    def rollback(cls, games, users):
        '''Place failed game (resp. user) IDs at the start of the queue so they get redone first'''
        with cls.lock:
            if games:
                cls.redis.srem(GAME_SET, *games)
                cls.redis.rpush(GAME_QUEUE, *games)

            if users:
                cls.redis.rpush(USER_QUEUE, *users)


class WatcherThread(Thread):

    def __init__(self, key, *args, **kwargs):
        self._remaining_cycles = kwargs.pop("cycles", None)  # 'None' means "loop forever"
        self.reqs = defaultdict(int)
        self.watcher = RiotWatcher(key, limits=(RateLimit(10, 10), RateLimit(500, 600)))
        self.ES = Elasticsearch(ES_NODES, timeout=30)
        super(WatcherThread, self).__init__(*args, **kwargs)

    def run(self):
        '''Main loop. Get tasks and execute them. (1 task == 1 request).'''
        while True:
            self.games, self.users, self.scraped_games = [], [], []
            games, users = Tasks.get()
            logging.debug("Fetched (games/users):\t%s\t%s" % (len(games), len(users)))

            try:
                self.process_tasks('game', games)
                self.process_tasks('user', users)
                bulk_upsert(self.ES, RIOT_GAMES_INDEX, GAME_DOCTYPE, self.scraped_games, id_fieldname='matchId')
                if games:
                    Tasks.redis.lpush(TO_CRUNCHER, *games)
                # Report game and user IDs seen during this cycle
                Tasks.add(set(self.games), set(self.users))
            except Exception as e:
                logging.error("Rolling back %s games and %s users: %s" % (len(games), len(users), e))
                Tasks.rollback(games, users)

            if self.my_work_here_is_done:
                break
            sleep(0.001)

    def process_tasks(self, taskname, arg_lst):
        for arg in arg_lst:
            while True:
                if self.watcher.can_make_request():
                    task = getattr(self, 'do_' + taskname)
                    logging.info("Task:\t%s\t%s" % (taskname, arg))
                    task(arg)
                    break
                else:
                    sleep(0.001)

    @property
    def my_work_here_is_done(self):
        if self._remaining_cycles is None:
            return False
        self._remaining_cycles -= 1
        return (self._remaining_cycles == 0)

    def do_user(self, userid):
        games = self.watcher.get_recent_games(userid, region=EUROPE_WEST)['games']
        for game_dct in games:
            if self.is_ranked(game_dct):
                fellow_players = [dct['summonerId'] for dct in game_dct['fellowPlayers']]
                self.users += fellow_players
                gameid = game_dct['gameId']
                self.games += [gameid]

    def do_game(self, gameid):
        dumpme = self.watcher.get_match(gameid, region=EUROPE_WEST, include_timeline=True)
        participants = [dct['player']['summonerId'] for dct in dumpme['participantIdentities']]
        self.users += participants
        self.scraped_games += [dumpme]

    @staticmethod
    def is_ranked(game_dct):
        try:
            return (game_dct['subType'] == "RANKED_SOLO_5x5")
        except:
            logging.error("There be trouble.")
            return False


class LoggingThread(Thread):
    def run(self):
        self.keep_going = True
        LOG_INTERVAL = 10  # seconds
        while self.keep_going:
            logging.info("total_games/new_games\t%s\t%s" % (Tasks.total_games, Tasks.new_games))
            sleep(LOG_INTERVAL)


class Scraper(object):
    def __init__(self):
        self.threads = [WatcherThread(key) for key in KEYS]

    def scrape(self):
        for t in self.threads:
            t.start()
        lt, gct = LoggingThread(), GameCounterThread()
        lt.start()
        gct.start()
        while True:
            try:
                sleep(0.1)
            except KeyboardInterrupt:
                logging.warning("KeyboardInterrupt received. Shutting down all threads NOW")
                for t in self.threads:
                    t._remaining_cycles = 1
                for t in self.threads:
                    t.join()
                lt.keep_going, gct.keep_going = False, False
                break


if __name__ == "__main__":
    ap = ArgumentParser(description="Scrape games from the Riot API")
    ap.add_argument('-i', '--init', action="store_true", help="Initialize Redis stacks with the contents of game and user files")
    ap.add_argument('-s', '--sync', action="store_true", help="During initialization, compare games in Scraper output with Cruncher output")
    args = ap.parse_args()

    init_logging()
    if args.init:
        Tasks.redis.init()

    s = Scraper()
    s.scrape()
