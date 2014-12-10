
user = {
    u'aggregate': {
        u'Assists': 80,
        u'Barons': 7,
        u'Bot': 3,
        u'Champ': {u'127': 1, u'15': 3, u'61': 1, u'89': 1},
        u'ClassicGame': 7,
        u'CreepsEnemy': 10,
        u'CreepsTeam': 90,
        u'Deaths': 40,
        u'Dragons': 15,
        u'Game': 7,
        u'Inhibitor': 12,
        u'Jungle': 1,
        u'Kills': 47,
        u'Level': 16,
        u'Mid': 1,
        u'Minions': 281,
        u'Ranked': 4,
        u'Towers': 5,
        u'Victory': 0,
        u'Wards': 70,
        u'WardsKilled': 4},
    u'games_id_list': [1816304028]
}

to_add = {
    u'aggregate': {
        u'Assists': 7,
        u'Barons': 1,
        u'Bot': 1,
        u'Champ': {u'121': 1},
        u'ClassicGame': 0,
        u'CreepsEnemy': 3,
        u'CreepsTeam': 1,
        u'Deaths': 47,
        u'Dragons': 15,
        u'Game': 7,
        u'Inhibitor': 12,
        u'Jungle': 1,
        u'Kills': 7,
        u'Level': 100,
        u'Mid': 1,
        u'Minions': 1000,
        u'Ranked': 3,
        u'Towers': 0,
        u'Victory': 5,
        u'Wards': 8,
        u'WardsKilled': 10},
    u'games_id_list': [1816444685,
                       1816776555,
                       1812746831,
                       1814814227,
                       1825753589,
                       1816125633]}

user_updated = {
    u'aggregate': {
        u'Assists': 87,
        u'Barons': 8,
        u'Bot': 4,
        u'Champ': {u'121': 1, u'127': 1, u'15': 3, u'61': 1, u'89': 1},
        u'ClassicGame': 7,
        u'CreepsEnemy': 13,
        u'CreepsTeam': 91,
        u'Deaths': 87,
        u'Dragons': 30,
        u'Game': 14,
        u'Inhibitor': 24,
        u'Jungle': 2,
        u'Kills': 54,
        u'Level': 116,
        u'Mid': 2,
        u'Minions': 1281,
        u'Ranked': 7,
        u'Towers': 5,
        u'Victory': 5,
        u'Wards': 78,
        u'WardsKilled': 14},
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
                "Level" : {
                    "type" : "long"
                },
                "Deaths" : {
                    "type" : "long"
                },
                "Jungle" : {
                    "type" : "long"
                },
                "Ranked" : {
                    "type" : "long"
                },
                "WardsKilled" : {
                    "type" : "long"
                },
                "ClassicGame" : {
                    "type" : "long"
                },
                "Assists" : {
                    "type" : "long"
                },
                "CreepsTeam" : {
                    "type" : "long"
                },
                "Minions" : {
                    "type" : "long"
                },
                "Victory" : {
                    "type" : "long"
                },
                "Mid" : {
                    "type" : "long"
                },
                "Inhibitor" : {
                    "type" : "long"
                },
                "Dragons" : {
                    "type" : "long"
                },
                "Champi" : {
                    "type" : "nested"
                },
                "Bot" : {
                    "type" : "long"
                },
                "Wards" : {
                    "type" : "long"
                },
                "Towers" : {
                    "type" : "long"
                },
                "Kills" : {
                    "type" : "long"
                },
                "Game" : {
                    "type" : "long"
                },
                "CreepsEnemy" : {
                    "type" : "long"
                },
                "Barons" : {
                    "type" : "long"
                }
            }
        }
    }
}
}

