# -*- coding: utf-8 -*-
import os
import sys
from threading import Thread, Lock
from queue import Queue
from config import KEYS, ES_NODES
from riotwatcher.riotwatcher import RiotWatcher, EUROPE_WEST, RateLimit
from elasticsearch import Elasticsearch


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
                tpl = (fn.__name__, e.__class__.__name__, str(e))
                sys.stderr.write("Exception squelched inside '%s': %s (%s)\n" % tpl)
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
        cls.lock.acquire()
        if cls.gq.empty():
            res = ('user', cls.users[cls.user_index])
            cls.user_index += 1
        else:
            res = ('game', cls.gq.get())
        cls.lock.release()
        return res

    @classmethod
    def put_game(cls, gameid):
        cls.lock.acquire()
        if gameid not in cls.games:
            cls.gq.put(gameid)
            cls.games |= set([gameid])
            cls._log_game(gameid)
        cls.lock.release()

    @classmethod
    def put_user(cls, uid):
        cls.lock.acquire()
        if uid not in cls.users:
            cls.users += [uid]
            cls.user_set |= set([uid])
            cls._log_user(uid)
        cls.lock.release()

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
        self.watcher = RiotWatcher(key, limits=(RateLimit(10, 10), RateLimit(500, 600)))
        super(WatcherThread, self).__init__(*args, **kwargs)

    def run(self):
        while True:
            if self.watcher.can_make_request():
                taskname, arg = Tasks.get()
                assert taskname in ('user', 'game')
                task = getattr(self, 'do_' + taskname)
                task(arg)

    @squelch_errors
    def do_user(self, userid):
        games = self.watcher.get_recent_games(userid, region=EUROPE_WEST)['games']
        for game_dct in games:
            fellow_players = [dct['summonerId'] for dct in game_dct['fellowPlayers']]
            self._add_users(fellow_players)
            gameid = game_dct['gameId']
            Tasks.put_game(gameid)

    @squelch_errors
    def do_game(self, gameid):
        dumpme = self.watcher.get_match(gameid, region=EUROPE_WEST, include_timeline=True)
        ES.index(index="rito", doc_type="game", id=gameid, body=dumpme)
        participants = [dct['player']['summonerId'] for dct in dumpme['participantIdentities']]
        self._add_users(participants)

    def _add_users(self, usr_lst):
        for uid in usr_lst:
            Tasks.put_user(uid)


class Scraper(object):
    def __init__(self, challenger_seed=True):
        self.threads = [WatcherThread(key) for key in KEYS]
        print("API KEY STATUS")
        print(self)

        if challenger_seed:
            watcher = self.threads[0].watcher
            challengers = self.get_challengers(watcher)
            new_challengers = [c for c in challengers if c not in Tasks.users]
            Tasks.users = new_challengers
            Tasks.user_set = set(new_challengers)
        print(self)
        print("**START**")
        if os.path.exists(GAMES_FILE):
            Tasks.games |= set([l.rstrip() for l in open(GAMES_FILE) if l.strip() != ''])
        if os.path.exists(USERS_FILE):
            Tasks.users += [l.rstrip() for l in open(USERS_FILE) if l.strip() != '']
            Tasks.user_set |= set(Tasks.users)

    def scrape(self):
        for t in self.threads:
            t.start()

    def get_challengers(self, watcher):
        league_dct = watcher.get_challenger(region=EUROPE_WEST)
        return [e['playerOrTeamId'] for e in league_dct['entries']]

    def __str__(self):
        fmt = "\t".join(["%s"] * (4))
        # out = fmt % tuple(list(range(len(self.threads))) + ['Users', 'Games', 'Index'])
        out = fmt % tuple([len(Tasks.users), len(Tasks.games), Tasks.gq.qsize(), Tasks.user_index])
        return out


if __name__ == "__main__":
    s = Scraper()
    s.scrape()
