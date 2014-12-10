from elasticsearch import Elasticsearch
from config import ES_NODES, GAME_DOCTYPE, RIOT_GAMES_INDEX, RIOT_USERS_INDEX
import json


def init_elasticsearch_index(test_index_name):

    es = Elasticsearch(ES_NODES)
    es.indices.create(index=test_index_name, ignore=[400, 404])


def delete_elasticsearch_index(test_index_name):
    """
    WARNING, before using this method, be sure ES_NODES points to your testing Elasticsearch (ie : your local one)
    """
    es = Elasticsearch(ES_NODES)
    es.indices.delete(index=test_index_name)


def init_elasticsearch_for_testing_cruncher():
    # mock rito index
    init_elasticsearch_index(RIOT_GAMES_INDEX)
    es = Elasticsearch(ES_NODES)
    with open('game_sample.txt', 'r') as f:
        game = json.loads(f.read())
    es.index(index=RIOT_GAMES_INDEX, doc_type=GAME_DOCTYPE, body=game, id=game["matchId"])
    # mock rita index
    init_elasticsearch_index(RIOT_USERS_INDEX)

if __name__ == "__main__":
    init_elasticsearch_index("test_init_elasticsearch")

