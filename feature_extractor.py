from aggregate_extractor import Extractor
from math import log
from math import exp


class FeatureExtractor(Extractor):

    def __init__(self, user):
        self.user = user


class AggregateDataNormalizer(FeatureExtractor):
    """
    Normalise les donnees aggregees en probabilites selon des parametres specifies pour chaque donnee.
    Par exemple, la donnee du nombre de minions est rapportee a une moyenne par partie jouee puis
    normalisee entre 0 et 1.
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
        "CrowedControl": (20., 600.),
        "Gold": (4000., 18000.),
        "PlayerTowers": (0., 5.),
        "PlayerInhibitors": (0., 2.),
        "Soloer": (0.,1.),
        "Adc": (0.,1.),
        "Support": (0., 1.),
        "Jungler": (0., 1.),
    }
    
    def apply(self):
        user = self.user
        for k, v in self.normalisation_params.items():
            if k not in user["aggregate"]:
                user["aggregate"][k] = 0
            if user["aggregate"]["Game"] != 0:
                user["feature"][k] = (max(v[0], min(v[1], float(user["aggregate"][k])/ user["aggregate"]["Game"])) - v[0]) / (v[1] - v[0])
            else:
                user["feature"][k] = 0
        return user


class HighLevelFeatureCalculator(FeatureExtractor):
    """
    A partir des donnees normalisees, calcule des donnees high level
    Ces donnees high level sont une moyenne ponderee des donnees normalisees
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
            "Bot": -3,
            "Soloer": 3
            },
        "action": {
            "Kills": 2,
            "Deaths": 2,
            "Assists": 2,
            "Mid": 2,
            "Jungle": 2,
            "FirstBlood": 3,
            "FirstBloodAssist": 3,
            "Level": -2,
            "Top": -2,
            "Bot": -2,
            "CreepEnemy": 2},
        "teamplay": {
            "Bot": 2,
            "Jungle": 2,
            "Dragons": 2,
            "Barons": 2,
            "Top": -2,
            "Minions": -1,
            "Level": 2,
            "Wards": 2,
            "VisionWards": 1,
            "Soloer": -2,
            "Support": 3,
            "Adc": 2,
            "Deaths": 2
            },
        "strategy": {
            "Towers": 2,
            "PlayerTowers": 3,
            "Dragons": 3,
            "Barons": 3,
            "Inhibitors": 2,
            "PlayerInhibitors": 3,
            "Wards": 3,
            "WardsKilled": 2,
            "VisionWards": 2,
            }
    }

    def apply(self):
        user = self.user
        for k, v in self.highLevelFeatures.items():
            user["feature"][k] = self.compute_weighted_average(k, v)
        return user

    def compute_weighted_average(self, feature, coeff):
        user = self.user
        coeff_sum = 0
        res = 0
        for key, val in coeff.items():
            if key not in user["feature"]:
                user["feature"][key] = 0
            if val < 0:
                res += -val*(1-user["feature"][key])
                coeff_sum += -val
            else:
                res += val*user["feature"][key]
                coeff_sum += val
        return res/coeff_sum


class EntropicFeatureCalculator(FeatureExtractor):
    """
    Computes Entropic Features. Actually it only computes Entropy for the loyalty feature
    """

    def __init__(self, user):
        FeatureExtractor.__init__(self, user)
        self.entropies = {
                     "loyalty": [float(champ_count)/user["aggregate"]["Game"] for _, champ_count in user["aggregate"]["Champ"].items()],
                     "position": [float(user["aggregate"][p])/user["aggregate"]["Game"] for p in ['Bot', 'Top', 'Mid', 'Jungle'] if p in user["aggregate"]],
                     "role": [float(user["aggregate"][r])/user["aggregate"]["Game"] for r in ['Jungler', 'Soloer', 'Adc', 'Support'] if r in user["aggregate"]]
                     }

    def apply(self):
        for k,v in self.entropies.items():
            self.user["feature"][k] = self.entropy(v)
        return self.user
 
    @staticmethod
    def entropy(vals):
        xlogx = lambda x: x * log(x) if x != 0 else 0
        normalized_entropy = exp(sum(map(xlogx, vals)))
        return normalized_entropy
