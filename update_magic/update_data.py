
user = {
    u'aggregate': {
        u'nAssists': 80,
        u'nBarons': 7,
        u'nBot': 3,
        u'nChampi': {u'127': 1, u'15': 3, u'61': 1, u'89': 1},
        u'nClassicGame': 7,
        u'nCreepsEnemy': 10,
        u'nCreepsTeam': 90,
        u'nDeaths': 40,
        u'nDragons': 15,
        u'nGame': 7,
        u'nInhibitor': 12,
        u'nJungle': 1,
        u'nKills': 47,
        u'nLevel': 16,
        u'nMid': 1,
        u'nMinions': 281,
        u'nRanked': 4,
        u'nTowers': 5,
        u'nVictory': 0,
        u'nWards': 70,
        u'nWardsKilled': 4},
    u'games_id_list': [1816304028]
}

to_add = {
    u'aggregate': {
        u'nAssists': 7,
        u'nBarons': 1,
        u'nBot': 1,
        u'nChampi': {u'121': 1},
        u'nClassicGame': 0,
        u'nCreepsEnemy': 3,
        u'nCreepsTeam': 1,
        u'nDeaths': 47,
        u'nDragons': 15,
        u'nGame': 7,
        u'nInhibitor': 12,
        u'nJungle': 1,
        u'nKills': 7,
        u'nLevel': 100,
        u'nMid': 1,
        u'nMinions': 1000,
        u'nRanked': 3,
        u'nTowers': 0,
        u'nVictory': 5,
        u'nWards': 8,
        u'nWardsKilled': 10},
    u'games_id_list': [1816444685,
                       1816776555,
                       1812746831,
                       1814814227,
                       1825753589,
                       1816125633]}

user_updated = {
    u'aggregate': {
        u'nAssists': 87,
        u'nBarons': 8,
        u'nBot': 4,
        u'nChampi': {u'121': 1, u'127': 1, u'15': 3, u'61': 1, u'89': 1},
        u'nClassicGame': 7,
        u'nCreepsEnemy': 13,
        u'nCreepsTeam': 91,
        u'nDeaths': 47,
        u'nDragons': 15,
        u'nGame': 7,
        u'nInhibitor': 24,
        u'nJungle': 1,
        u'nKills': 47,
        u'nLevel': 116,
        u'nMid': 2,
        u'nMinions': 1281,
        u'nRanked': 7,
        u'nTowers': 5,
        u'nVictory': 5,
        u'nWards': 78,
        u'nWardsKilled': 14},
    u'games_id_list': [1816304028,
                       1816444685,
                       1816776555,
                       1812746831,
                       1814814227,
                       1825753589,
                       1816125633]}

mapping = {"user": {
    "properties" : {
        "games_id_list" : {
            "type" : "long"
        },
        "aggregate" : {
            "properties" : {
                "nLevel" : {
                    "type" : "long"
                },
                "nDeaths" : {
                    "type" : "long"
                },
                "nJungle" : {
                    "type" : "long"
                },
                "nRanked" : {
                    "type" : "long"
                },
                "nWardsKilled" : {
                    "type" : "long"
                },
                "nClassicGame" : {
                    "type" : "long"
                },
                "nAssists" : {
                    "type" : "long"
                },
                "nCreepsTeam" : {
                    "type" : "long"
                },
                "nMinions" : {
                    "type" : "long"
                },
                "nVictory" : {
                    "type" : "long"
                },
                "nMid" : {
                    "type" : "long"
                },
                "nInhibitor" : {
                    "type" : "long"
                },
                "nDragons" : {
                    "type" : "long"
                },
                "nChampi" : {
                    "type" : "nested"
                },
                "nBot" : {
                    "type" : "long"
                },
                "nWards" : {
                    "type" : "long"
                },
                "nTowers" : {
                    "type" : "long"
                },
                "nKills" : {
                    "type" : "long"
                },
                "nGame" : {
                    "type" : "long"
                },
                "nCreepsEnemy" : {
                    "type" : "long"
                },
                "nBarons" : {
                    "type" : "long"
                }
            }
        }
    }
}
}

