from elasticsearch import helpers, Elasticsearch
from config import ES_NODES, RIOT_INDEX, GAME_DOCTYPE, USER_DOCTYPE
from feature_extractor import FeatureExtractor
from user import User
from collections import defaultdict


class UserCruncher(object):
    ES = Elasticsearch(ES_NODES)
    FE = [FeatureExtractor]
    USERS_ID = set()
    GAMES_ID = set()
    USERS = defaultdict(User)
    with open('user_games.txt', 'r') as fh:
        for line in fh:
            USERS_ID.add(int(line))
    with open('games.txt', 'r') as f:
        for line in f:
            GAMES_ID.add(int(line))

    def extract_games(self, user_id):
        #TODO: this method is no longer useful
        scan = helpers.scan(client=self.ES, query=self.write_query(user_id),
                            scroll="5m", index=RIOT_INDEX, doc_type=GAME_DOCTYPE)
        for game in scan:
            yield game['_source']

    @staticmethod
    def write_query(user_id):
        return {'query': {'filtered': {'filter': {'term': {"participantIdentities.player.summonerId": user_id}}}}}

    def get_game(self, game_id):
        return self.ES.get(index=RIOT_INDEX, doc_type=GAME_DOCTYPE, id=game_id)

    def get_user(self, user_id):
        res = self.ES.get(index=RIOT_INDEX, doc_type=USER_DOCTYPE, id=user_id)
        return User(res)

    def insert_user(self, user):
        self.ES.index(RIOT_INDEX, doc_type=USER_DOCTYPE, body=user)

    def process(self):
        for game_id in self.GAMES_ID:
            game = self.get_game(game_id)
            for participant in game["_source"]["participantIdentities"]:
                if "summonerId" not in participant:
                    continue
                user_id = participant["summonerId"]
                for f in self.FE:
                    self.USERS[user_id] = f.apply(self.USERS[user_id], game
            #TODO: write 