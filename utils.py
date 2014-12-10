import itertools
import logging
from elasticsearch import Elasticsearch
from config import ES_NODES, RIOT_GAMES_INDEX, GAME_DOCTYPE
import json


def split_seq(iterable, size):
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))


def load_as_set(filename):
    return set([l.rstrip() for l in open(filename) if l.strip() != ''])


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
                logging.error("Exception squelched inside:'%s': %s (%s)" % tpl)
                pass
        return run
    return raising(method)


def jsonify_game(game_id):
    es = Elasticsearch(ES_NODES)
    response = es.get(index=RIOT_GAMES_INDEX, doc_type=GAME_DOCTYPE, id=game_id)
    return json.dumps(response["_source"])
