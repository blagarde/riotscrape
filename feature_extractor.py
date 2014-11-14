__author__ = 'william'

import abc


class FeatureExtractor(object):

    def __init__(self, user, games):
        self.games = [game for game in games if games["matchId"] not in user['game_id_list']]  # TODO : Don't forget to add games in user at the end
        self.user = user

    @staticmethod
    def get_participant_id(game, user_id):
        participants = game['participantIdentities']
        for participant in participants:
            if participant["summonerId"] == user_id:
                return participant['participantId']
        raise ValueError("user_id provided not found in game")

    @staticmethod
    def get_participant(game,participant_id):
        for participant in game['participants']:
            if participant['participantId'] == participant_id:
                return participant
        raise ValueError("participant_id not found in game")


    @abc.abstractmethod
    def apply(self):
        return


class QueueTypeExtractor(FeatureExtractor):

    def apply(self):
        for game in self.games:
            if game['queueType'] in ['RANKED_SOLO_5x5', 'RANKED_PREMADE_5x5', 'RANKED_PREMADE_3x3', 'RANKED_TEAM_3x3', 'RANKED_TEAM_5x5']:
                self.user['nRanked'] += 1
            self.user['nGame'] += 1
        return self.user


class GameModeExtractor(FeatureExtractor):

    def apply(self):
        for game in self.games:
            if game['queueType'] in ['RANKED_SOLO_5x5', 'RANKED_PREMADE_5x5', 'RANKED_TEAM_5x5', 'NORMAL_5x5_DRAFT', 'NORMAL_5x5_BLIND']:
                self.user['nClassicGame'] += 1
            else :
                self.user['nSubGame'] += 1
        return self.user


class ChampionExtractor(FeatureExtractor):

    def apply(self):
        for game in self.games:
            participantId = self.get_participant_id(game,self.user["id"])
            participant = self.get_participant(game,participantId)
            self.user['nChampi'][participant['championId']] += 1
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


class SocialExtractor(FeatureExtractor):
    pass