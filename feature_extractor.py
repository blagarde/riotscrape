__author__ = 'william'

import abc

class FeatureExtractor(object):

    def __init__(self, user, games):
        self.games = [game for game in games if games["matchId"] in user['game_id_list']]
        self.user = user

    @abc.abstractmethod
    def apply(self):
        return

class QueueTypeExtractor(FeatureExtractor):

    def apply(self):
        for game in self.games:
            if game['queueType'] in ['RANKED_SOLO_5x5','RANKED_PREMADE_5x5','RANKED_PREMADE_3x3','RANKED_TEAM_3x3','RANKED_TEAM_5x5']:
                self.user['nRanked'] +=1
            self.user['nGame'] +=1
        return self.user

class FarmExtractor(FeatureExtractor):
    pass


class ObjectivesExtractor(FeatureExtractor):
    pass


class RoleExtractor(FeatureExtractor):
    pass


class WardsExtractor(FeatureExtractor):
    pass


class KDAExtractor(FeatureExtractor):
    pass


class ChampionExtractor(FeatureExtractor):
    pass


class GameTypeExtractor(FeatureExtractor):
    pass



class GameModeExtractor(FeatureExtractor):
    pass


class SocialExtractor(FeatureExtractor):
    pass