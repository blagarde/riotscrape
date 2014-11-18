from elasticsearch import Elasticsearch, helpers
from config import ES_NODES, RIOT_INDEX, GAME_DOCTYPE, USER_DOCTYPE
from feature_extractor import QueueTypeExtractor, GameModeExtractor, ChampionExtractor, ParticipantStatsExtractor, TeamStatsExtractor, LaneExtractor
from user import User
from time import time
import sys


class UserCruncher(object):
    ES = Elasticsearch(ES_NODES)
    FE = [QueueTypeExtractor, GameModeExtractor, ChampionExtractor,
          ParticipantStatsExtractor, TeamStatsExtractor, LaneExtractor]
    USERS_ID = set()
    GAMES_ID = set()
    USERS = {}
    with open('users.txt', 'r') as fh:
        for line in fh:
            USERS_ID.add(int(line))
    with open('games.txt', 'r') as f:
        for line in f:
            GAMES_ID.add(int(line))

    @classmethod
    def _log_crunched_games(cls, game_id):
        with open('crunched_games.txt', 'a') as gf:
            gf.write('%s\n' % game_id)

    @classmethod
    def _log_seen_users(cls, user_id):
        with open('seen_users.txt', 'a') as uf:
            uf.write('%s\n' % user_id)

    @classmethod
    def _log_not_found_games(cls, game_id):
        with open('not_found_games.txt', 'a') as uf:
            uf.write('%s\n' % game_id)

    def get_games(self, games_id):
        body = {'ids': games_id}
        games = self.ES.mget(index=RIOT_INDEX, doc_type=GAME_DOCTYPE, body=body)
        return [game for game in games["docs"]]

    @staticmethod
    def write_query(user_id):
        return {'query': {'filtered': {'filter': {'term': {"participantIdentities.player.summonerId": user_id}}}}}

    def get_game(self, game_id):
        return self.ES.get(index=RIOT_INDEX, doc_type=GAME_DOCTYPE, id=game_id)

    def get_user(self, user_id):
        res = self.ES.get(index=RIOT_INDEX, doc_type=USER_DOCTYPE, id=user_id)
        return User(res)

    def insert_users(self, chunk_size=2000):
        users = [user for di, user in self.USERS.items()]
        return helpers.bulk(client=self.ES, actions=self._build_bulk_request(users), chunk_size=chunk_size)

    def _build_bulk_request(self, users):
        for user in users:
            query = {
                    "_op_type": "index",
                    "_id": user['id'],
                    "_index": 'rita',
                    "_type": 'user',
                    "_source": user}
            yield query

    def process(self, chunk_size=1000):
        chunks = [list(self.GAMES_ID)[x:x+chunk_size] for x in xrange(0, len(self.GAMES_ID), chunk_size)]
        for i, chunk in enumerate(chunks):
            t_start = time()
            games = self.get_games(chunk)
            for game in games:
                self._process_game(game)
            t_end = time()
            out = "\rgames crunched\t%s\tchunk time\t%s" % ((i+1)*chunk_size, t_end-t_start)
            sys.stdout.write(out)
            if i > 2:
                break
        self.insert_users()



    def _process_game(self, game):
        if not game['found']:
            self._log_not_found_games(game['_id'])
            return
        for participant in game["_source"]["participantIdentities"]:
            self._process_participant(participant, game)
        self._log_crunched_games(int(game["_id"]))

    def _process_participant(self, participant, game):
        if "player" not in participant:
            return
        else:
            user_id = participant["player"]["summonerId"]
            if user_id not in self.USERS_ID:
                return
            try:
                user = self.USERS[user_id]
            except KeyError:
                user = User(user_id)
                self.USERS[user_id] = user
            if int(game['_id']) not in user['games_id_list']:
                for f in self.FE:
                    user = f(user, game).apply()
                user["games_id_list"].append(int(game['_id']))
                self.USERS[user_id] = user
                self._log_seen_users(user_id)

if __name__ == "__main__":
    uc = UserCruncher()
    uc.process()

