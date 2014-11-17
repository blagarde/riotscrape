import copy
from config import AGGREGATE_TEMPLATE


class User(dict):

    def __init__(self, user_id):
        self['id'] = user_id
        self['aggregate'] = copy.deepcopy(AGGREGATE_TEMPLATE)
        self['games_id_list'] =[]
