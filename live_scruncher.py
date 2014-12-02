from aggregate_extractor import ChampionExtractor, GameModeExtractor,\
    QueueTypeExtractor, LaneExtractor, ParticipantStatsExtractor, TeamStatsExtractor
from feature_extractor import ProbaExtractor, RulesExtractor
from riotwatcher.riotwatcher import RiotWatcher, EUROPE_WEST
from user import User
from time import sleep
RIOT_KEY_ID= '94b01087-f844-4f6b-8d00-8520cfbf5eec'
RIOT_KEY_GAME = 'd3928411-a85e-48b2-8686-15cff70e8064'


def assert_api_availability(method):
    def decorated(self, *args, **kwargs):
        while True:
            if self.watcher.can_make_request():
                return method(self, *args, **kwargs)
            else:
                sleep(0.001)
    return decorated


class LiveSCruncher(object):
    AE = [QueueTypeExtractor, GameModeExtractor, ChampionExtractor,
          ParticipantStatsExtractor, TeamStatsExtractor, LaneExtractor]
    FE = [ProbaExtractor, RulesExtractor]

    def __init__(self, summoner_name, region):
        self.watcher_id = RiotWatcher(RIOT_KEY_ID)
        self.watcher_games = RiotWatcher(RIOT_KEY_GAME)
        self.summoner_name = summoner_name
        self.region = region
        self.id = self.get_id()
        self.games_id = self.get_games_id()
        self.games = self.get_games()
        self.user = User(self.id)

    @assert_api_availability
    def get_id(self):
        return self.watcher.get_summoner(name=self.summoner_name, region=self.region)['id']

    @assert_api_availability
    def get_games_id(self):
        return [game['gameId'] for game in self.watcher.get_recent_games(self.id, region=self.region)['games']]

    @assert_api_availability
    def get_game(self, id_):
        return self.watcher.get_match(match_id=id_, region=self.region, include_timeline=False)

    def get_games(self):
        games = []
        for id_ in self.games_id:
            games.append(self.get_game(id_))
        return games

    def crunch(self):
        for game in self.games:
            self._process_game(game)
        self._process_features()

    def _process_game(self, game):
        for participant in game["participantIdentities"]:
            self._process_participant(participant, game)

    def _process_participant(self, participant, game):
        if "player" not in participant or participant["player"]["summonerId"] != self.id:
            return
        else:
            for f in self.AE:
                self.user = f(self.user, game).apply()
            self.user["games_id_list"].append(int(game['matchId']))

    def _process_features(self):
        for f in self.FE:
            self.user = f(self.user).apply()


if __name__ == "__main__":
    lsc = LiveSCruncher("dipl0mate", EUROPE_WEST)