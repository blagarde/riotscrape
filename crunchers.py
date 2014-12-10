from multiprocessing import Pool
from aggregate_extractor import ChampionExtractor, QueueTypeExtractor, LaneExtractor, ParticipantStatsExtractor, TeamStatsExtractor,\
    RoleExtractor
from elasticsearch.client import Elasticsearch
from config import ES_NODES, REDIS_PARAM, GAME_DOCTYPE, NB_PROCESSES, USER_DOCTYPE, RIOT_GAMES_INDEX, RIOT_USERS_INDEX,\
    TO_CRUNCHER, TO_USERCRUNCHER
from redis import StrictRedis as Buffer
from user import User
from cluster_service import ClusterService
from elasticsearch import helpers
from feature_extractor import AggregateDataNormalizer, HighLevelFeatureCalculator, EntropicFeatureCalculator
from abc import abstractmethod
import json


class Cruncher(object):

    def __init__(self):
        self.buffer = Buffer(**REDIS_PARAM)
        self.ES = Elasticsearch(ES_NODES)
        self.chunk_size = 100
        self._init_ids()
        self.USERS = {}

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
        chunks = [(x, x+self.chunk_size) for x in xrange(0, len(self.content), self.chunk_size)]
        for a, b in chunks:
            chunk = self.content[a:b]
            conts = self._get_content(chunk)
            for cont in conts:
                self._process_content(cont)
        self._end_crunching()
        self.insert_users()

    @abstractmethod
    def _end_crunching(self):
        pass

    def insert_users(self, chunk_size=100):
        users = [user for _, user in self.USERS.items()]
        return helpers.bulk(client=self.ES, actions=self._build_bulk_request(users), chunk_size=chunk_size)

    @abstractmethod
    def _build_bulk_request(self, users):
        pass


class GameCruncher(Cruncher):

    def __init__(self):
        Cruncher.__init__(self)
        self.gamesnotfound = set()
        self.AE = [QueueTypeExtractor, ChampionExtractor,
                   ParticipantStatsExtractor, TeamStatsExtractor, LaneExtractor, RoleExtractor]

    def _end_crunching(self):
        req = self.buffer.pipeline()
        for i, ui in enumerate(self.USERS):
            req.zadd(TO_USERCRUNCHER, i, ui)
        req.execute()

    def _init_ids(self):
        p = self.buffer.pipeline()
        for _ in range(1000):
            p.rpop(TO_CRUNCHER)
        self.content = [i for i in p.execute() if i is not None]

    def _get_content(self, games_id):
        body = {'ids': games_id}
        games = self.ES.mget(index=RIOT_GAMES_INDEX, doc_type=GAME_DOCTYPE, body=body)
        return [game for game in games["docs"]]

    def _process_content(self, game):
        for participant in game["_source"]["participantIdentities"]:
            self._process_participant(participant, game)

    def _process_participant(self, participant, game):
        if "player" not in participant:
            return
        else:
            user_id = participant["player"]["summonerId"]
            try:
                user = self.USERS[user_id]
            except KeyError:
                user = User(user_id)
            for f in self.AE:
                user = f(user, game["_source"]).apply()
            user["games_id_list"].append(int(game['_id']))
            self.USERS[user_id] = user

    def _build_bulk_request(self, users):
        for user in users:
            query = {
                "_op_type": "update",
                "_id": user['id'],
                "_index": RIOT_USERS_INDEX,
                "_type": USER_DOCTYPE,
                "script": "update_agg_data",
                "params": {"data": json.dumps(user)},
                "upsert": user,
            }
            yield query


class UserCruncher(Cruncher):

    def __init__(self):
        Cruncher.__init__(self)
        self.FE = [AggregateDataNormalizer, HighLevelFeatureCalculator, EntropicFeatureCalculator]

    def _init_ids(self):
        self.content = self.buffer.pipeline().zrange(TO_USERCRUNCHER, 0, 1000).zremrangebyrank(TO_USERCRUNCHER, 0, 1000).execute()[0]

    def _get_content(self, user_ids):
        body = {'ids': user_ids}
        users = self.ES.mget(index=RIOT_USERS_INDEX, doc_type=USER_DOCTYPE, body=body)
        res = [user for user in users["docs"]]
        return res

    def _process_content(self, user):
        if user["found"]:
            user = user["_source"]
            for f in self.FE:
                user = f(user).apply()
            self.USERS[user["id"]] = user

    def _build_bulk_request(self, users):
        for user in users:
            query = {
                "_op_type": "update",
                "_id": user['id'],
                "_index": RIOT_USERS_INDEX,
                "_type": USER_DOCTYPE,
                "doc": {"feature": user["feature"]},
                "upsert": user
                }
            yield query

    def _end_crunching(self):
        pass


def launch_cruncher(cruncher):
    cr = cruncher()
    cr.crunch()

if __name__ == '__main__':
#     #launch_cruncher(GameCruncher)
#     launch_cruncher(UserCruncher)
    pool = Pool(processes=NB_PROCESSES)
    pool.map(launch_cruncher, [GameCruncher for _ in range(NB_PROCESSES*100)])
    # pool.map(launch_cruncher, [UserCruncher for _ in range(NB_PROCESSES*100)])
    #pool.map(launch_cruncher, [UserCruncher for _ in range(NB_PROCESSES*100)])
