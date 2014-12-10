from aggregate_extractor import Extractor
from math import log
from math import exp


class FeatureExtractor(Extractor):

    def __init__(self, user):
        self.user = user


class AggregateDataNormalizer(FeatureExtractor):
    """
    Normalise les données aggrégées en probabilités selon des paramètres spécifiés pour chaque donnée.
    Par exemple, la donnée du nombre de minions est rapportée à une moyenne par partie jouée puis
    normalisée entre 0 et 1.
    """
    normalisation_params = {
        "Minions": (0., 250.),
        "Level": (7., 18.),
        "CreepsEnemy": (0., 100.),
        "CreepsTeam": (0., 150.),
        "Kills": (0., 11.),
        "Deaths": (0., 12.),
        "Assists": (0., 20.),
        "Bot": (0., 1.),
        "Top": (0., 1.),
        "Mid": (0., 1.),
        "Jungle": (0., 1.),
        "Towers": (0., 10.),
        "Dragons": (0., 4.),
        "Barons": (0., 2.),
        "Inhibitors": (0., 3.),
        "Wards": (0., 20.),
        "WardsKilled": (0., 8.),
        "Victory": (0., 1.),
        "FirstBlood": (0., 1.),
        "FirstBloodAssist": (0., 1.),
        "KillingSprees": (0., 4.),
        "VisionWards": (0., 4.),
        "CrowedControl": (1000., 5000.),
        "Gold": (4000., 18000.),
        "PlayerTowers": (0., 5.),
        "PlayerInhibitor": (0., 2.),
    }
    
    def apply(self):
        user = self.user
        for k, v in self.probas.items():
            if k not in user["aggregate"]:
                user["aggregate"][k] = 0
            if user["aggregate"]["Game"] != 0:
                user["feature"][k] = (max(v[0], min(v[1], float(user["aggregate"][k])/ user["aggregate"]["Game"])) - v[0]) / (v[1] - v[0])
            else:
                user["feature"][k] = 0
        return user



class HighLevelFeatureCalculator(FeatureExtractor):
    """
    À partir des données normalisées, calcule des données high level
    Ces données high level sont une moyenne pondérée des données normalisées
    ou alors définis par une fonction
    """
    highLevelFeatures = {
            "solo": {
                "Minions": 3,
                "CreepsTeam": 1,
                "Top": 3,
                "Mid": 2,
                "Level": 2,
                "Kills": 1,
                "Deaths": -2,
                "Assists": -1,
                "Bot": -3},
            "action": {
                "Kills": 2,
                "Deaths": 2,
                "Assists": 2,
                "Mid": 1,
                "Jungle": 1,
                "FirstBlood": 3,
                "FirstBloodAssists": 3,
                "Duration": -2,
                "Top": -2,
                "Bot": -2,
                "CreepEnemy": 2},
            "teamplay": {
                "Bot": 2,
                "Jungle": 2,
                "Dragons": 2,
                "Nashors": 2,
                "Top": -2,
                "Minions": -1,
                "Level": -1,
                "Wards": 2},
            "strategy": {
                "Towers": 1,
                "Dragons": 3,
                "Nashors": 3,
                "Inhibitors": 2,
                "Wards": 3,
                "WardsKilled": 2},
            "loyalty": HighLevelFeatureCalculator.entropy,
        }

    def apply(self):
        user = self.user
        for k, v in self.highLevelFeatures.items():
            if type(v) != dict:
                user["feature"][k] = v(user)
            else:
                coeff_sum = 0
                res = 0
                for key, val in v.items():
                    if key not in user["feature"]:
                        user["feature"][key] = 0
                    if val < 0:
                        res += -val*(1-user["feature"][key])
                        coeff_sum += -val
                    else:
                        res += val*user["feature"][key]
                        coeff_sum += val
                user["feature"][k] = res/coeff_sum
        return user

    @staticmethod
    def entropy(user):
        xlogx = lambda x: x * log(x) if x != 0 else 0
        champs_normalized_vals = [float(champ_count)/user["aggregate"]["Game"] for _, champ_count in user["aggregate"]["Champ"].items()]
        normalized_entropy = exp(sum(map(xlogx, champs_normalized_vals)))
        return normalized_entropy