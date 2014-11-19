from collections import defaultdict


class User(dict):

    def __init__(self, user_id):
        self['id'] = user_id
        self['aggregate'] = defaultdict(int)
        self['aggregate']['nChampi'] = defaultdict(int)
        self['games_id_list'] =[]