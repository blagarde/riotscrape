from elasticsearch import helpers, Elasticsearch
from config import ES_NODES, RIOT_INDEX, GAME_DOCTYPE, USER_DOCTYPE
from feature_extractor import FeatureExtractor
from user import User

class UserCruncher(object):
    ES = Elasticsearch(ES_NODES)
    FE = [FeatureExtractor]
    USERS_ID = set()
    with open('user_games.txt', 'r') as fh:
        for line in fh:
            user_id, _, _ = line.split("\t")
            USERS_ID.add(user_id)

    def extract_games(self, user_id):
        scan = helpers.scan(client=self.ES, query=self.write_query(user_id), scroll="5m", index=RIOT_INDEX, doc_type=GAME_DOCTYPE)
        for game in scan:
            yield game['_source']

    def insert_user(self, user):
        self.ES.index(RIOT_INDEX, doc_type=USER_DOCTYPE, body=user)

    def get_user(self,user_id):
        res = self.ES.get(index=RIOT_INDEX, doc_type=USER_DOCTYPE, id=user_id)
        return User(res) 

    def process(self):
        for user_id in self.USERS_ID:
            user = self.get_user(user_id)
            games = list(self.extract_games(user_id))
            for f in self.FE:
                user = f.apply(user, games)
            self.insert_user(user)

    @staticmethod
    def write_query(user_id):
        return {'query': {'filtered': {'filter': {'term': {"participantIdentities.player.summonerId": user_id}}}}}
