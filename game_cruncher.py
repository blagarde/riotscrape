

from elasticsearch import helpers, Elasticsearch
from config import ES_NODES, RIOT_INDEX, RIOT_DOCTYPE
from feature_extractor import FeatureExtractor


class GameCruncher(object):
    ES = Elasticsearch(ES_NODES)
    FE = [FeatureExtractor,]

    def extract_games(self):
        scan = helpers.scan(client=self.ES, query={}, scroll="5m", index=RIOT_INDEX, doc_type=RIOT_DOCTYPE)
        for game in scan:
            yield game

    def insert_user(self,user):
        self.ES.index(RIOT_INDEX, doc_type=RIOT_DOCTYPE, body=user)

    def process(self):
        for game in self.extract_games():
            temp = {}
            for fe in self.FE:
                temp = fe(game,temp)
            # insert in elasticsearch
            for v in temp:
                self.insert_user(v)

