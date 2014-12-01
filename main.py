# -*- coding: utf-8 -*-
import os
import sys
from threading import Thread, Lock
from queue import Queue
from collections import defaultdict
from itertools import chain
from config import KEYS, ES_NODES, GAME_DOCTYPE, RIOT_INDEX
from riotwatcher.riotwatcher import RiotWatcher, EUROPE_WEST, RateLimit
from elasticsearch import Elasticsearch
from time import sleep
import random


USERS_FILE = 'users.txt'
GAMES_FILE = 'games.txt'
ES = Elasticsearch(ES_NODES)
MAX_USERS = 1000000


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
    games = set()
    gq = Queue()
    users = []
    user_set = set()
    lock = Lock()
    user_index = 0

    @classmethod
    def get(cls):
        res = None
        with cls.lock:
            if cls.gq.empty():
                res = ('user', cls.users[cls.user_index])
                cls.user_index += 1
                cls.user_index %= MAX_USERS
            else:
                res = ('game', cls.gq.get())
        return res

    @classmethod
    def put_game(cls, gameid):
        with cls.lock:
            if gameid not in cls.games:
                cls.gq.put(gameid)
                cls.games |= set([gameid])
                cls._log_game(gameid)

    @classmethod
    def put_user(cls, uid):
        with cls.lock:
            if uid not in cls.users and len(cls.users) < MAX_USERS:
                cls.users += [uid]
                cls.user_set |= set([uid])
                cls._log_user(uid)

    @classmethod
    def _log_game(cls, gameid):
        with open(GAMES_FILE, 'a') as gf:
            gf.write('%s\n' % gameid)

    @classmethod
    def _log_user(cls, uid):
        with open(USERS_FILE, 'a') as uf:
            uf.write('%s\n' % uid)


class WatcherThread(Thread):

    def __init__(self, key, *args, **kwargs):
        self.reqs = defaultdict(int)
        self.watcher = RiotWatcher(key, limits=(RateLimit(10, 10), RateLimit(500, 600)))
        super(WatcherThread, self).__init__(*args, **kwargs)

    def run(self):
        while True:
            if self.watcher.can_make_request():
                taskname, arg = Tasks.get()
                assert taskname in ('user', 'game')
                task = getattr(self, 'do_' + taskname)
                task(arg)
            else:
                sleep(0.001)

    @squelch_errors
    def do_user(self, userid):
        games = self.watcher.get_recent_games(userid, region=EUROPE_WEST)['games']
        self.reqs['users'] += 1
        for game_dct in games:
            fellow_players = [dct['summonerId'] for dct in game_dct['fellowPlayers']]
            self._add_users(fellow_players)
            gameid = game_dct['gameId']
            Tasks.put_game(str(gameid))

    @squelch_errors
    def do_game(self, gameid):
        dumpme = self.watcher.get_match(gameid, region=EUROPE_WEST, include_timeline=True)
        self.reqs['games'] += 1
        ES.index(index=RIOT_INDEX, doc_type=GAME_DOCTYPE, id=gameid, body=dumpme)
        participants = [dct['player']['summonerId'] for dct in dumpme['participantIdentities']]
        self._add_users(participants)

    def _add_users(self, usr_lst):
        for uid in usr_lst:
            Tasks.put_user(str(uid))


class Scraper(object):
    def __init__(self, challenger_seed=True):
        self.threads = [WatcherThread(key) for key in KEYS]
        if challenger_seed:
            watcher = self.threads[0].watcher
            challengers = self.get_challengers(watcher)
            new_challengers = [c for c in challengers if c not in Tasks.users]
            Tasks.users = new_challengers
        self.display()
        print("**LOAD**\n")
        if os.path.exists(GAMES_FILE):
            Tasks.games |= set([l.rstrip() for l in open(GAMES_FILE) if l.strip() != ''])
        if os.path.exists(USERS_FILE):
            games = [l.rstrip() for l in open(USERS_FILE) if l.strip() != '']
            random.shuffle(games)
            Tasks.users += games
            Tasks.user_set |= set(Tasks.users)
        print("**START**\n")
        self.display()

    def scrape(self):
        for t in self.threads:
            t.start()
        while True:
            self.display()
            sleep(0.1)

    def get_challengers(self, watcher):
        league_dct = watcher.get_challenger(region=EUROPE_WEST)
        return [e['playerOrTeamId'] for e in league_dct['entries']]

    def display(self):
        fmt = "\r" + '\t'.join(["u%s/g%s (%%s/%%s)" % (i + 1, i + 1) for i in range(len(self.threads))])
        # out = fmt % tuple(list(range(len(self.threads))) + ['Users', 'Games', 'Index'])
        # out = fmt %
        counts = [(t.reqs['users'], t.reqs['games']) for t in self.threads]
        out = fmt % tuple(chain(*counts))
        out += '\t' + '\t'.join([str(sum(i)) for i in zip(*counts)])
        out += '\t' + '\t'.join(map(str, [Tasks.user_index, len(Tasks.users), len(Tasks.games), Tasks.gq.qsize()]))
        sys.stdout.write(out)


if __name__ == "__main__":
    s = Scraper()
    s.scrape()
