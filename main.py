# -*- coding: utf-8 -*-
import os
import sys
from collections import deque
from itertools import cycle
from time import sleep
from config import KEYS, ES_NODES
from riotwatcher.riotwatcher import RiotWatcher, EUROPE_WEST, LoLException, RateLimit
from elasticsearch import Elasticsearch

USERS_FILE = 'users.txt'
GAMES_FILE = 'games.txt'


class EmptyUserQueue(IndexError):
    pass

class EmptyGameQueue(IndexError):
    pass

class UncaughtException(RuntimeError):
    pass


def squelch_errors(method):
    '''
    Decorator to squelch most errors.
    Just output them to stderr
    '''
    def raising(fn):
        def run(*args):
            try:
                return fn(*args)
            except (EmptyGameQueue, EmptyUserQueue) as e:
                raise e
            except Exception as e:
                tpl = (fn.__name__, e.__class__.__name__, str(e))
                sys.stderr.write("Exception squelched inside '%s': %s (%s)\n" % tpl)
                sys.stderr.flush()
                pass
        return run
    return raising(method)


class Scraper(object):
    es = Elasticsearch(ES_NODES)
    games = set()
    users = set()
    uq = deque()
    gq = deque()
    def __init__(self):
        self.WATCHERS = [RiotWatcher(k, limits=(RateLimit(10, 10), RateLimit(500, 600))) for k in KEYS]
        self.watchers = cycle(self.WATCHERS)
        self.watcher = next(self.watchers)
        print("API KEY STATUS")
        for w in self.WATCHERS:
            print("%s\t%s" % (w.key, w.can_make_request()))

        challengers = self.get_challengers()
        self.uq += [c for c in challengers if c not in self.users]
        if os.path.exists(GAMES_FILE):
            self.games |= set([l.rstrip() for l in open(GAMES_FILE)])

    def scrape(self):
        while True:
            while not self.watcher.can_make_request():
                self.watcher = next(self.watchers)
                sleep(0.001)
            try:
                self.do_game()
            except EmptyGameQueue:
                try:
                    self.do_user()
                except EmptyUserQueue:
                    print("empty user Q")
                    continue
            except LoLException:
                sleep(10)
    
    def get_challengers(self):
        league_dct = self.watcher.get_challenger(region=EUROPE_WEST)
        return [e['playerOrTeamId'] for e in league_dct['entries']]

    @squelch_errors
    def do_user(self):
        try:
            userid = self.uq.popleft()
        except IndexError as e:
            raise EmptyUserQueue(e)
        games = self.watcher.get_recent_games(userid, region=EUROPE_WEST)['games']
        for game_dct in games:
            self.uq += [dct['summonerId'] for dct in game_dct['fellowPlayers'] if dct['summonerId'] not in self.users]
            if game_dct['gameId'] not in self.games:
                self.gq += [game_dct['gameId']]
        self.users |= set([userid])
    
    @squelch_errors
    def do_game(self):
        try:
            gameid = self.gq.popleft()
        except IndexError as e:
            raise EmptyGameQueue(e)
        dumpme = self.watcher.get_match(gameid, region=EUROPE_WEST, include_timeline=True)
        self.es.index(index="lbdriot", doc_type="game", id=gameid, body=dumpme)
        self.uq += [dct['player']['summonerId'] for dct in dumpme['participantIdentities'] if dct['player']['summonerId'] not in self.users]
        self.games |= set([gameid])
        with open(GAMES_FILE, 'a') as gf:
            gf.write('%s\n' % gameid)


if __name__ == "__main__":
    s = Scraper()
    s.scrape()
