from collections import defaultdict


class User(dict):

    def __init__(self, user_id):
        self['id'] = user_id
        self['feature'] = defaultdict(float)
        self['aggregate'] = defaultdict(int)
        self['aggregate']['nChamp'] = defaultdict(int)
        self['games_id_list'] = []

    def is_valid(self):
        return self['games_id_list'] != []

