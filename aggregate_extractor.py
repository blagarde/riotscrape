import abc


def classic_game_only(method):
    def decorated(self):
        if self.is_classic_game:
            return method(self)
        else:
            return self.user
    return decorated


class Extractor(object):
    pass


class AggregateExtractor(Extractor):

    def __init__(self, user, game):
        self.user = user
        self.aggregate = self.user["aggregate"]
        self.game = game

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
        participant = AggregateExtractor.get_participant(game, participant_id)
        return participant["teamId"]

    @staticmethod
    def get_team(game, team_id):
        for team in game['teams']:
            if team["teamId"] == team_id:
                return team
        raise ValueError("teamId not found in game")

    def is_classic_game(self):
        return self.game['queueType'] in ['RANKED_SOLO_5x5', 'RANKED_PREMADE_5x5', 'RANKED_TEAM_5x5',
                                          'NORMAL_5x5_DRAFT', 'NORMAL_5x5_BLIND']


class QueueTypeExtractor(AggregateExtractor):

    def apply(self):
        self.aggregate['Game'] += 1
        return self.user


class ChampionExtractor(AggregateExtractor):

    def apply(self):
        participant_id = self.get_participant_id(self.game, self.user["id"])
        participant = self.get_participant(self.game, participant_id)
        self.aggregate['Champ'][participant['championId']] += 1
        return self.user


class ParticipantStatsExtractor(AggregateExtractor):
    P_STATS = [('Kills', 'kills'), ('Deaths', 'deaths'), ('Assists', 'assists'),
               ('CreepsTeam', 'neutralMinionsKilledTeamJungle'), ('CreepsEnemy', 'neutralMinionsKilledEnemyJungle'),
               ('Minions', 'minionsKilled'), ('PlayerTowers', 'towerKills'),  ('Level', 'champLevel'),
               ('WardsKilled', 'wardsKilled'), ('Wards', 'wardsPlaced'), ('FirstBlood', 'firstBloodKill'),
               ('FirstBloodAssist', 'firstBloodAssist'), ('KillingSprees', 'killingSprees'),
               ('VisionWards', 'visionWardsBoughtInGame'), ('CrowedControl', 'totalTimeCrowdControlDealt'),
               ('Gold', 'goldEarned'), ('PlayerInhibitors', 'inhibitorKills')]

    @classic_game_only
    def apply(self):
        participant_id = self.get_participant_id(self.game, self.user["id"])
        participant = self.get_participant(self.game, participant_id)
        for pair in self.P_STATS:
            if pair[1] in participant["stats"]:
                self.aggregate[pair[0]] += participant["stats"][pair[1]]
        return self.user


class TeamStatsExtractor(AggregateExtractor):
    T_STATS = [('Dragons', 'dragonKills'), ('Barons', 'baronKills'),
               ('Inhibitors', 'inhibitorKills'), ('Victory', 'winner'),
               ('Towers','towerKills')]

    @classic_game_only
    def apply(self):
        participant_id = self.get_participant_id(self.game, self.user["id"])
        team_id = self.get_team_id(self.game, participant_id)
        team = self.get_team(self.game, team_id)
        for pair in self.T_STATS:
            self.aggregate[pair[0]] += team[pair[1]]
        return self.user


class RoleExtractor(AggregateExtractor):
    ROLE_CONV = {'NONE':'Jungler', 'SOLO':'Soloer', 'DUO_CARRY':'Adc', 'DUO_SUPPORT':'Support', 'DUO':'Duo' }

    @classic_game_only
    def apply(self):
        participant_id = self.get_participant_id(self.game, self.user["id"])
        participant = self.get_participant(self.game, participant_id)
        role = participant['timeline']['role']
        self.aggregate[self.ROLE_CONV[role]] += 1
        return self.user

class LaneExtractor(AggregateExtractor):
    LANE_CONV = {'MIDDLE': 'Mid', 'TOP': 'Top', 'BOTTOM': 'Bot', 'JUNGLE': 'Jungle'}

    @classic_game_only
    def apply(self):
        participant_id = self.get_participant_id(self.game, self.user["id"])
        participant = self.get_participant(self.game, participant_id)
        lane = participant['timeline']['lane']
        self.aggregate[self.LANE_CONV[lane]] += 1
        return self.user