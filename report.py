import json
from collections import Counter
from elasticsearch import Elasticsearch, helpers
from threading import Thread
import logging
from time import sleep
from config import RIOT_USERS_INDEX, ES_NODES


STEP = 1000
NAP_TIME = 3.6  # seconds


class GameCounterThread(Thread):
    def run(self):
        self.keep_going = True
        while self.keep_going:
            ES = Elasticsearch(ES_NODES)
            users = helpers.scan(client=ES, query={}, scroll="10m", index=RIOT_USERS_INDEX, doc_type='user', timeout="10m")
            cnt = Counter([u['_source']['aggregate']['Game'] for u in users])
            dump = json.dumps(dict(cnt))
            logging.info("GAME COUNTS:\t" + dump)
            for i in range(STEP):
                if not self.keep_going:
                    return
                sleep(NAP_TIME)
