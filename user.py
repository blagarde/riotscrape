from collections import defaultdict


class User(dict):

    def __init__(self, user_id):
        self['id'] = user_id
        self['feature'] = dict()
        self['aggregate'] = defaultdict(int)
        self['aggregate']['nChamp'] = defaultdict(int)
        self['games_id_list'] =[]
