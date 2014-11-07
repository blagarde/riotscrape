# -*- coding: utf-8 -*-
import os
import sys
from time import sleep
from collections import deque, OrderedDict
from threading import Thread, Lock

from config import KEYS, ES_NODES
from riotwatcher.riotwatcher import RiotWatcher, EUROPE_WEST, LoLException, RateLimit
from elasticsearch import Elasticsearch


USERS_FILE = 'users.txt'
GAMES_FILE = 'games.txt'
ES = Elasticsearch(ES_NODES)
MAX_USERS = 400


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


class WatcherThread(Thread):
    def __init__(self, key, scraper, *args, **kwargs):
        self.watcher = RiotWatcher(key, limits=(RateLimit(10, 10), RateLimit(500, 600)))
        self.scraper = scraper
        self.taskgen = scraper.taskgen()
        super(WatcherThread, self).__init__(*args, **kwargs)

    def run(self):
        while True:
            if self.watcher.can_make_request():
                taskname, arg = next(self.taskgen)
                assert taskname in ('user', 'game')
                task = getattr(self, 'do_' + taskname)
                try:
                    #print(str(self.scraper))
                    task(arg)
                    self.scraper.reqs += 1
                except LoLException:
                    stderr.write("whoopsies (%s)" % self.watcher.key)
            sleep(0.001)

    @squelch_errors
    def do_user(self, userid):
        games = self.watcher.get_recent_games(userid, region=EUROPE_WEST)['games']
        for game_dct in games:
            fellow_players = [dct['summonerId'] for dct in game_dct['fellowPlayers']]
            self._add_users(fellow_players)
            gameid = game_dct['gameId']
            self._add_game(gameid)

    @squelch_errors
    def do_game(self, gameid):
        dumpme = self.watcher.get_match(gameid, region=EUROPE_WEST, include_timeline=True)
        ES.index(index="rito", doc_type="game", id=gameid, body=dumpme)
        self._log_game(gameid)
        participants = [dct['player']['summonerId'] for dct in dumpme['participantIdentities']]
        self._add_users(participants)

    def _add_game(self, gameid):
        self.scraper.game_queue_lock.acquire()
        if gameid not in self.scraper.games:
            self.scraper.gq += [gameid]
            self.scraper.games |= set([gameid])
        self.scraper.game_queue_lock.release()

    def _add_users(self, usr_lst):
        # This lock is necessary to guarantee uniqueness and thread safety:
        self.scraper.user_queue_lock.acquire()
        for uid in usr_lst:
            if uid not in self.scraper.user_set:
                self.scraper.users += [uid]
                self.scraper.user_set |= set([uid])
        self.scraper.user_queue_lock.release()

    def _log_game(self, gameid):
        self.scraper.game_lock.acquire()
        with open(GAMES_FILE, 'a') as gf:
            gf.write('%s\n' % gameid)
        self.scraper.game_lock.release()


class Scraper(object):
    games = set()
    users = []
    user_set = set()
    gq = deque()

    user_queue_lock = Lock()
    game_queue_lock = Lock()
    # Locks for access to the files
    game_lock = Lock()
    user_lock = Lock()
    fifthlock = Lock()
    
    reqs = 0
    def __init__(self, challenger_seed=True):
        self.threads = [WatcherThread(key, self) for key in KEYS]
        self.user_index = 0
        print("API KEY STATUS")
        print(self)

        if challenger_seed:
            watcher = self.threads[0].watcher
            challengers = self.get_challengers(watcher)
            new_challengers = [c for c in challengers if c not in self.users]
            self.users = new_challengers
            self.user_set = set(new_challengers)
        print(self)
        print("**START**")
        if os.path.exists(GAMES_FILE):
            self.games |= set([l.rstrip() for l in open(GAMES_FILE)])

    def scrape(self):
        for t in self.threads:
            t.start()
    
    def get_challengers(self, watcher):
        league_dct = watcher.get_challenger(region=EUROPE_WEST)
        return [e['playerOrTeamId'] for e in league_dct['entries']]

    def taskgen(self):
        '''Scraping strategy lives here'''
        while True:
            assert set(self.users) == self.user_set
            try:
                gameid = self.gq.popleft()
                self.fifthlock.acquire()
                res = ('game', gameid)
                self.games.add(gameid)
                self.fifthlock.release()
                yield res
            except IndexError as e:
                print e
                try:
                    self.user_lock.acquire()
                    userid = self.users[self.user_index]
                    self.user_index += 1
                    self.user_lock.release()
                    yield ('user', userid)
                except IndexError as e:  # Very unlikely. Would only happen if we run out of users before running out of games
                    self.user_index = 0
                    sleep(0.001)
                    # Wait a while, hope that some pending requests will succeed and fill the queue

    def __str__(self):
        fmt = "\t".join(["%s"] * (5))
        # out = fmt % tuple(list(range(len(self.threads))) + ['Users', 'Games', 'Index'])
        out = fmt % tuple([len(self.users), len(self.games), len(self.gq), self.user_index, self.reqs])
        return out

if __name__ == "__main__":
    s = Scraper()
    s.scrape()

def get_game_ids(self):
    request = {}
    scan = helpers.scan(client=self.es, query=req, scroll="5m", index="lbdriot", doc_type="game")
    return (r["_id"] for r in scan)
