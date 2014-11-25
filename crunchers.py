from multiprocessing import Pool
from aggregate_extractor import ChampionExtractor, GameModeExtractor,QueueTypeExtractor, LaneExtractor, ParticipantStatsExtractor, TeamStatsExtractor
from elasticsearch.client import Elasticsearch
from config import ES_NODES, REDIS_PARAM, GAME_DOCTYPE, RIOT_INDEX, NB_PROCESSES
from redis import StrictRedis as Buffer
from user import User
from elasticsearch import helpers
from feature_extractor import ProbaExtractor, RulesExtractor
from abc import abstractmethod
from time import time
import sys
import json
import os

class Cruncher(object):

    def __init__(self):
        self.buffer = Buffer(**REDIS_PARAM)
        self.ES = Elasticsearch(ES_NODES)
        self.chunk_size = 100
        self._init_ids()
        self.USERS = {}
        self.baptorgameseffectivelycrunched = set()
        self.idontcareabouttheseids = set()

    @abstractmethod
    def _init_ids(self):
        pass

    @abstractmethod
    def _get_content(self, chunk):
        pass

    @abstractmethod
    def _process_content(self, content):
        pass

    def crunch(self):
        chunks = [self.content[x:x+self.chunk_size] for x in xrange(0, len(self.content), self.chunk_size)]
        for i, chunk in enumerate(chunks):
            t_start = time()
            conts = self._get_content(chunk)
            for cont in conts:
                self._process_content(cont)
            t_end = time()
            out = "\rgames crunched\t%s\tchunk time\t%s" % ((i)*self.chunk_size, t_end-t_start)
            sys.stdout.write(out)
            sys.stdout.flush()
        req = self.buffer.pipeline()
        for gid in self.baptorgameseffectivelycrunched:
            req.lpush("baptor", gid)
        req.execute()
        req = self.buffer.pipeline()
        for uid in self.idontcareabouttheseids:
            req.sadd("bite", uid)
        req.execute()
        self.baptorgameseffectivelycrunched = set()
        self.idontcareabouttheseids = set()
        self.insert_users()

    def insert_users(self, chunk_size=2000):
        users = [user for _, user in self.USERS.items()]
        res = helpers.bulk(client=self.ES, actions=self._build_bulk_request(users), chunk_size=chunk_size)
        return res

    @abstractmethod
    def _build_bulk_request(self, users):
        pass


class GameCruncher(Cruncher):
    
    def __init__(self):
        Cruncher.__init__(self)
        self.AE = [QueueTypeExtractor, GameModeExtractor, ChampionExtractor,
                   ParticipantStatsExtractor, TeamStatsExtractor, LaneExtractor]

    def _init_ids(self):
        self.content = self.buffer.pipeline().lrange('games', 0, 1000).ltrim('games', 1000, -1).execute()[0]
        self.USERS_ID = set(self.buffer.smembers('users_set'))

    def _get_content(self, games_id):
        body = {'ids': games_id}
        games = self.ES.mget(index=RIOT_INDEX, doc_type=GAME_DOCTYPE, body=body)
        return [game for game in games["docs"]]

    def _process_content(self, game):
        if not game['found']:
            return
        for participant in game["_source"]["participantIdentities"]:
            self._process_participant(participant, game)

    def _process_participant(self, participant, game):
        if "player" not in participant:
            return
        else:
            user_id = participant["player"]["summonerId"]
            if str(user_id) not in self.USERS_ID:
                self.idontcareabouttheseids.add(user_id)
                return
            try:
                user = self.USERS[user_id]
            except KeyError:
                user = User(user_id)
            for f in self.AE:
                self.baptorgameseffectivelycrunched.add(int(game['_id']))
                user = f(user, game).apply()
            user["games_id_list"].append(int(game['_id']))
            self.USERS[user_id] = user

    def _build_bulk_request(self, users):
        for user in users:
            query = {
                "_op_type": "update",
                "_id": user['id'],
                "_index": 'test',
                "_type": 'user',
                "script": "update_agg_data",
                "params": {"data": json.dumps(user)},
                "upsert": user,
                }
            yield query


class UserCruncher(Cruncher):
    
    def __init__(self):
        Cruncher.__init__(self)
        self.FE = [ProbaExtractor, RulesExtractor]

    def _init_ids(self):
        self.content = self.buffer.pipeline().lrange('users',0,10000).ltrim('users', 10001, -1).execute()[0]
        self.USERS_ID = self.buffer.smembers('users_set')

    def _get_content(self, userids):
        body = {'ids': userids}
        users = self.ES.mget(index="ritou", doc_type="user", body=body)
        res = [user for user in users["docs"]]
        print len(res)
        return res

    def _process_content(self, user):
        if user["found"]:
            user = user["_source"]
            try:
                for f in self.FE:
                    user = f(user).apply()
                    self.USERS[user["id"]] = user
            except Exception as e:
                print e
                print user

    def _build_bulk_request(self, users):
        for user in users:
            query = {
                "_op_type": "update",
                "_id": user['id'],
                "_index": 'test',
                "_type": 'user',
                "script": "update_agg_data",
                "params": {"data": json.dumps(user)},
                "lang": "python",
                "upsert": user,
                }
            yield query


def launch_cruncher(cruncher):
    cr = cruncher()
    cr.crunch()

if __name__ == '__main__':
    gc = GameCruncher()
    gc.crunch()

    # pool = Pool(processes=NB_PROCESSES)
    # pool.map(launch_cruncher, [GameCruncher for _ in range(NB_PROCESSES*100)])
    #pool.map(launch_cruncher, [UserCruncher for _ in range(NB_PROCESSES*100)])
