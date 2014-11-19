from aggregate_extractor import Extractor
from math import log
from math import exp

class FeatureExtractor(Extractor):
    pass

class probaExtractor(FeatureExtractor):
    probas =   {
                 "pMinions":        {"min":0, "max":300, "name":"nMinions", "ref":"nClassicGame"},
                 "pCreepsTeam":     {"min":0, "max":150, "name":"nCreepsTeam", "ref":"nClassicGame"},
                 "pCreepsEnemy":    {"min":0, "max":100, "name":"nCreepsEnemy", "ref":"nClassicGame"},
                 "pLevel":          {"min":7, "max":18, "name":"nLevel", "ref":"nClassicGame"},
                 "pKills":          {"min":0, "max":20, "name":"nKills", "ref":"nClassicGame"},
                 "pDeaths":         {"min":0, "max":20, "name":"nDeaths", "ref":"nClassicGame"},
                 "pAssists":        {"min":0, "max":30, "name":"nAssists", "ref":"nClassicGame"},
                 "pBot":            {"min":0, "max":1, "name":"nBot", "ref":"nClassicGame"},
                 "pTop":            {"min":0, "max":1, "name":"nTop", "ref":"nClassicGame"},
                 "pMid":            {"min":0, "max":1, "name":"nMid", "ref":"nClassicGame"},
                 "pJungle":         {"min":0, "max":1, "name":"nJungle", "ref":"nClassicGame"},
                 "pTowers":         {"min":0, "max":11, "name":"nTowers", "ref":"nClassicGame"},
                 "pDragons":        {"min":0, "max":7, "name":"nDragons", "ref":"nClassicGame"},
                 "pNashors":        {"min":0, "max":11, "name":"nNashors", "ref":"nClassicGame"},
                 "pInhibitors":     {"min":0, "max":7, "name":"nInhibitors", "ref":"nClassicGame"},
                 "pWards":          {"min":0, "max":25, "name":"nWards", "ref":"nClassicGame"},
                 "pWardsKilled":    {"min":0, "max":15, "name":"nWardsKilled", "ref":"nClassicGame"},
                 "pWin":            {"min":0, "max":1, "name": "nWin", "ref":"nClassicGame"},
                 "pClassicGame":    {"min":0, "max":1, "name": "nClassicGame", "ref":"nGame"},
                 "pRanked":         {"min":0, "max":1, "name": "nRanked", "ref":"nGame"},
                 "pSubGame":        {"min":0, "max":1, "name": "nSubGame", "ref":"nGame"},
                }
    
    def apply(self, user):
        res = user["feature"]
        for k,v in self.probas.items():
            if user[v["name"]] < v["min"]:
                res[k] = 0
            else:
                res[k] = (min(v["max"],user["aggregate"][v["name"]] / user["aggregate"][v["ref"]]) - v["min"]) / (v["max"] - v["min"])
        user["feature"] = res

class RulesExtractor(FeatureExtractor):
    categories = { "solo": { "pMinions": 3,
                            "pCreepsTeam": 1,
                            "pTop": 3,
                            "pMid": 2,
                            "pLevel": 2,
                            "pKills": 1,
                            "pDeaths": -2,
                            "pAssists": -1,
                            "pBot": -3 },
                  "action": { "pKills": 2,
                             "pDeaths": 2,
                             "pAssists": 2,
                             "pMid": 1,
                             "pJungle": 1,
                             "pFirstBlood": 3,
                             "pDuration": -2,
                             "pTop": -2,
                             "pBot": -2,
                             "pCreepEnemy": 2 },
                  "teamplay": { "pBot": 2,
                               "pJungle": 2,
                               "pDragons": 2,
                               "pNashors": 2,
                               "pTop": -2,
                               "pMinions": -1,
                               "pLevel": -1,
                               "pWards": 2 },
                  "strategy": { "pTowers": 1,
                               "pDragons": 3,
                               "pNashors": 3,
                               "pInhibitors": 2,
                               "pWards": 3,
                               "pWardsKilled": 2 },
                  "loyalty": RulesExtractor._loyalty,
                  "competition": { "pRanked": 1 },
                  "diversity": { "pSubGame": 1 }
                  }

    @staticmethod
    def _loyalty(user):
        f = lambda x: x * log(x) if x != 0 else 0
        s = sum(map(f, user["aggregate"]["nChamp"]))
        return exp(s)

    def apply(self, user):
        for k,v in self.categories.items():
            if type(v) != dict:
                user["feature"][k] = v(user)
            else:
                coeffSum = 0
                res = 0
                for key,val in v.items():
                    if val < 0:
                        res += -val * (1-user["feature"][key])
                        coeffSum += -val
                    else:
                        res += val * user["feature"][key]
                        coeffSum += val 
                user["feature"][k] = res / coeffSum
        return user