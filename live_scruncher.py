from aggregate_extractor import ChampionExtractor, GameModeExtractor,\
    QueueTypeExtractor, LaneExtractor, ParticipantStatsExtractor, TeamStatsExtractor
from feature_extractor import ProbaExtractor, RulesExtractor
from riotwatcher.riotwatcher import RiotWatcher, EUROPE_WEST
from user import User
from time import sleep

from config import ES_NODES
from elasticsearch import Elasticsearch
RIOT_KEY_ID= '94b01087-f844-4f6b-8d00-8520cfbf5eec'
RIOT_KEY_GAME = 'd3928411-a85e-48b2-8686-15cff70e8064'


class Service(object):
    id_watcher = RiotWatcher(RIOT_KEY_ID)
    game_watcher = RiotWatcher(RIOT_KEY_GAME)
    es = Elasticsearch(ES_NODES)

    def get_crunched_user(self, summoner_name, region):
        summoner_id = self.id_watcher.get_summoner(name=summoner_name, region=region)['id']
        # TODO : handle the case when id is not found
        try:
            res = self.es.get(id=summoner_id, index='rita', doc_type='user')
            print "known"
            return res['_source']
        except:
            # TODO: handle this particular error TransportError(404, u'{"_index":"rita","_type":"user","_id":"36731656","found":false}')
            # TODO: send to baptor redis
            game_ids = self._get_game_ids(summoner_id, region)
            games = self._get_games(game_ids, region)
            luc = LiteUserCruncher(summoner_id, games)
            return luc.crunch()
            pass

    def _get_game_ids(self, summoner_id, region):
        while True:
            if self.id_watcher.can_make_request():
                return [game['gameId'] for game in self.id_watcher.get_recent_games(summoner_id=summoner_id, region=region)['games']]
            else:
                sleep(0.001)

    def _get_game(self, game_id, region):
        while True:
            if self.game_watcher.can_make_request():
                return self.game_watcher.get_match(match_id=game_id, region=region, include_timeline=False)
            else:
                sleep(0.001)

    def _get_games(self, game_ids, region):
        games = []
        for game_id in game_ids:
            games.append(self._get_game(game_id, region))
        return games


class LiteUserCruncher(object):
    AE = [QueueTypeExtractor, GameModeExtractor, ChampionExtractor,
          ParticipantStatsExtractor, TeamStatsExtractor, LaneExtractor]
    FE = [ProbaExtractor, RulesExtractor]

    def __init__(self, summoner_id, games):
        self.summoner_id = summoner_id
        self.games = games
        self.user = User(summoner_id)

    def crunch(self):
        for game in self.games:
            self._process_game(game)
        self._process_features()

    def _process_game(self, game):
        for participant in game["participantIdentities"]:
            self._process_participant(participant, game)

    def _process_participant(self, participant, game):
        if "player" not in participant or participant["player"]["summonerId"] != self.summoner_id:
            return
        else:
            for f in self.AE:
                self.user = f(self.user, game).apply()
            self.user["games_id_list"].append(int(game['matchId']))

    def _process_features(self):
        for f in self.FE:
            self.user = f(self.user).apply()


if __name__ == "__main__":
    se = Service()
    print se.get_crunched_user('zerbot','euw')