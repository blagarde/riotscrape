import abc


class FeatureExtractor(object):

    def __init__(self, user, game):
        # TODO: remove the init method and pass arguments in the apply method
        self.user = user
        self.aggregate = self.user["aggregate"]
        self.game = game['_source']

    @abc.abstractmethod
    def apply(self):
        return

    @staticmethod
    def get_participant_id(game, user_id):
        participants = game['participantIdentities']
        for participant in participants:
            if participant["player"]["summonerId"] == user_id:
                return participant['participantId']
        raise ValueError("user_id provided not found in game")

    @staticmethod
    def get_participant(game, participant_id):
        for participant in game['participants']:
            if participant['participantId'] == participant_id:
                return participant
        raise ValueError("participant_id not found in game")

    @staticmethod
    def get_team_id(game, participant_id):
        participant = FeatureExtractor.get_participant(game, participant_id)
        return participant["teamId"]

    @staticmethod
    def get_team(game, team_id):
        for team in game['teams']:
            if team["teamId"] == team_id:
                return team
        raise ValueError("teamId not found in game")


class QueueTypeExtractor(FeatureExtractor):

    def apply(self):
        if self.game['queueType'] in ['RANKED_SOLO_5x5', 'RANKED_PREMADE_5x5', 'RANKED_PREMADE_3x3',
                                      'RANKED_TEAM_3x3', 'RANKED_TEAM_5x5']:
            self.aggregate['nRanked'] += 1
        self.aggregate['nGame'] += 1
        return self.user


class GameModeExtractor(FeatureExtractor):

    def apply(self):
        if self.game['queueType'] in ['RANKED_SOLO_5x5', 'RANKED_PREMADE_5x5', 'RANKED_TEAM_5x5',
                                      'NORMAL_5x5_DRAFT', 'NORMAL_5x5_BLIND']:
            self.aggregate['nClassicGame'] += 1
        else:
            self.aggregate['nSubGame'] += 1
        return self.user


class ChampionExtractor(FeatureExtractor):

    def apply(self):
        participant_id = self.get_participant_id(self.game, self.user["id"])
        participant = self.get_participant(self.game, participant_id)
        self.aggregate['nChampi'][participant['championId']] += 1
        return self.user


class ParticipantStatsExtractor(FeatureExtractor):
    P_STATS = [('nKills', 'kills'), ('nDeaths', 'deaths'), ('nAssists', 'assists'),
               ('nCreepsTeam', 'neutralMinionsKilledTeamJungle'), ('nCreepsEnemy', 'neutralMinionsKilledEnemyJungle'),
               ('nMinions', 'minionsKilled'), ('nTowers', 'towerKills'),  ('nLevel', 'champLevel')]

    def apply(self):
        participant_id = self.get_participant_id(self.game, self.user["id"])
        participant = self.get_participant(self.game, participant_id)
        for pair in self.P_STATS:
            self.aggregate[pair[0]] += participant["stats"][pair[1]]
        return self.user


class TeamStatsExtractor(FeatureExtractor):
    T_STATS = [('nDragons', 'dragonKills'), ('nBarons', 'baronKills'), ('nInhibitor', 'inhibitorKills')]

    def apply(self):
        participant_id = self.get_participant_id(self.game, self.user["id"])
        team_id = self.get_team_id(self.game, participant_id)
        team = self.get_team(self.game, team_id)
        for pair in self.T_STATS:
            self.aggregate[pair[0]] += team[pair[1]]
        return self.user


class LaneExtractor(FeatureExtractor):
    LANE_CONV = {'MIDDLE': 'nMid', 'TOP': 'nTop', 'BOTTOM': 'nBot', 'JUNGLE': 'nJungle'}

    def apply(self):
        participant_id = self.get_participant_id(self.game, self.user["id"])
        participant = self.get_participant(self.game, participant_id)
        lane = participant['timeline']['lane']
        self.aggregate[self.LANE_CONV[lane]] += 1
        return self.user


class RoleExtractor(FeatureExtractor):
    pass


class WardsExtractor(FeatureExtractor):
    pass


class KDAExtractor(FeatureExtractor):
    pass



class GameTypeExtractor(FeatureExtractor):
    pass


class SocialExtractor(FeatureExtractor):
    pass