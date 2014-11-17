import copy
from config import AGGREGATE_TEMPLATE


class User(dict):

    def __init__(self, dict):
        if "aggregate" in dict["_source"]:
            super(User, self).__init__(dict)
        else:
            dict["_source"]["aggregate"] = copy.deepcopy(AGGREGATE_TEMPLATE)
            super(User, self).__init__(dict)

