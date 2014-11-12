__author__ = 'william'

from elasticsearch import helpers, Elasticsearch
from config import ES_NODES, RIOT_INDEX


class GameExtractor(object):
    ES = Elasticsearch(ES_NODES)

    def extract_games(self):
        request = {}
        scan = helpers.scan(client=self.ES, query={}, scroll="5m", index=RIOT_INDEX, doc_type="game")
        for game in scan:
            yield game
