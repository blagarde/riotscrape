from elasticsearch import helpers, Elasticsearch
from config import ES_NODES, RIOT_INDEX, GAME_DOCTYPE, USER_DOCTYPE
from feature_extractor import QueueTypeExtractor, GameModeExtractor, ChampionExtractor, ParticipantStatsExtractor, TeamStatsExtractor, LaneExtractor
from user import User
from elasticsearch.exceptions import NotFoundError
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

    def extract_games(self, user_id):
        # TODO: this method is no longer useful
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
        # TODO: use a bulk update instead
        self.ES.index(RIOT_INDEX, doc_type=USER_DOCTYPE, body=user)

    def process(self):
        for game_id, i in enumerate(self.GAMES_ID):
            self._process_game(game_id)
            out = "games crunched\t%s" % i
            sys.stdout.write(out)
        #self.insert_user(self.USERS)

    def _process_game(self, game_id):
        try:
            game = self.get_game(game_id)
        except NotFoundError:
            self._log_not_found_games(game_id)
            return
        for participant in game["_source"]["participantIdentities"]:
            self._process_participant(participant, game)
        self._log_crunched_games(game_id)

    def _process_participant(self, participant, game):
        if "player" not in participant:
            return
        else:
            user_id = participant["player"]["summonerId"]
            try:
                user = self.USERS[user_id]
            except KeyError:
                user = User(user_id)
                self.USERS[user_id] = user
            if int(game['_id']) not in user['games_id_list']:
                for f in self.FE:
                    user = f(user, game).apply()
                user["games_id_list"].append(int(game['_id']))  # TODO : speed up this dot ?
                self.USERS[user_id] = user  # TODO: is it really necessary ?
                self._log_seen_users(user_id)

if __name__ == "__main__":
    uc = UserCruncher()
    t_start = time()
    uc.process()
    t_end = time()
    print t_end-t_start
