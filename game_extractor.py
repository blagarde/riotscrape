__author__ = 'william'

from elasticsearch import helpers, Elasticsearch
from config import ES_NODES, RIOT_INDEX, RIOT_DOCTYPE
from feature_extractor import FeatureExtractor


class GameExtractor(object):
    ES = Elasticsearch(ES_NODES)
    FE = [FeatureExtractor,]

    def __init__(self):
        self.xgames = self.extract_games()

    def extract_games(self):
        scan = helpers.scan(client=self.ES, query={}, scroll="5m", index=RIOT_INDEX, doc_type=RIOT_DOCTYPE)
        for game in scan:
            yield game

    def insert_team(self):
        pass

    def process(self):
        for game in self.xgames:
            temp = dict()
            for fe in self.FE:
                temp = fe(game,temp)
            # insert in elasticsearch


