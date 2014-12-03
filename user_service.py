from aggregate_extractor import ChampionExtractor, GameModeExtractor,\
    QueueTypeExtractor, LaneExtractor, ParticipantStatsExtractor, TeamStatsExtractor
from feature_extractor import ProbaExtractor, RulesExtractor
from riotwatcher.riotwatcher import RiotWatcher, LoLException
from user import User
from time import sleep, time
from config import ES_NODES
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError

RIOT_KEY_ID = '94b01087-f844-4f6b-8d00-8520cfbf5eec'
RIOT_KEY_GAME = 'd3928411-a85e-48b2-8686-15cff70e8064'


class UserService(object):
    id_watcher = RiotWatcher(RIOT_KEY_ID)
    game_watcher = RiotWatcher(RIOT_KEY_GAME)
    es = Elasticsearch(ES_NODES)

    def get_crunched_user(self, summoner_name, region):
        while True:
            if self.id_watcher.can_make_request():
                try:
                    summoner_id = self.id_watcher.get_summoner(name=summoner_name, region=region)['id']
                    user = self.es.get(id=summoner_id, index='rita', doc_type='user')["_source"]
                    if not self.features_are_computed(user):
                        luc = LiteUserCruncher(user)
                        user = luc.crunch()
                    return '200', user
                except LoLException:
                    return '404', {}
                except TransportError:
                    # TODO: send user to redis queue
                    try:
                        game_ids = self._get_game_ids(summoner_id, region)
                        games = self._get_games(game_ids, region)
                        luc = LiteGameCruncher(summoner_id, games)
                        user = luc.crunch()
                        if user.is_valid():
                            return '200', user
                        else:
                            return '204', {}
                    except LoLException:
                        return '404', {}
            else:
                sleep(0.001)

    def _get_game_ids(self, summoner_id, region):
        while True:
            if self.id_watcher.can_make_request():
                return [game['gameId'] for game in self.id_watcher.get_recent_games(summoner_id=summoner_id, region=region)['games']]
            else:
                sleep(0.001)

    def _get_game(self, game_id, region):
        return self.game_watcher.get_match_async(match_id=game_id, region=region, include_timeline=False)

    def _get_games(self, game_ids, region):
        games_grequests = []
        for game_id in game_ids:
            games_grequests.append(self._get_game(game_id, region))
            while True:
                if self.game_watcher.can_make_async_request(len(games_grequests)):
                    return self.game_watcher.send_async_requests(games_grequests)
                else:
                    sleep(0.001)

    @staticmethod
    def features_are_computed(user):
        if "feature" in user:
            return bool(user["feature"])
        return False


class LiteUserCruncher(object):
    FE = [ProbaExtractor, RulesExtractor]

    def __init__(self, user):
        self.user = user

    def crunch(self):
        self._process_content()
        return self.user

    def _process_content(self):
        for f in self.FE:
            self.user = f(self.user).apply()


class LiteGameCruncher(object):
    AE = [QueueTypeExtractor, GameModeExtractor, ChampionExtractor,
          ParticipantStatsExtractor, TeamStatsExtractor, LaneExtractor]

    def __init__(self, summoner_id, games):
        self.summoner_id = summoner_id
        self.games = games
        self.user = User(summoner_id)

    def crunch(self):
        for game in self.games:
            self._process_content(game)
        return self.user

    def _process_content(self, game):
        for participant in game["participantIdentities"]:
            self._process_participant(participant, game)

    def _process_participant(self, participant, game):
        if "player" not in participant or participant["player"]["summonerId"] != self.summoner_id:
            return
        else:
            for f in self.AE:
                self.user = f(self.user, game).apply()
            self.user["games_id_list"].append(int(game['matchId']))


if __name__ == "__main__":
    se = UserService()
    t_start = time()
    print se.get_crunched_user('dipl0mate', 'euw')
    t_end = time()
    print t_end-t_start