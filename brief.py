from multiprocessing import Pool
from elasticsearch.client import Elasticsearch
from config import ES_NODES, BLANK_FILTERS, RIOT_USERS_INDEX
from copy import deepcopy
from cluster_data import CLUSTER_DATA


def _get_elt(elt):
    res = []
    for key, val in elt.items():
        if key in ["and", "or", "not"]:
            for logic, eslogic in {"and":"must", "or":"should", "not":"must_not"}.items():
                if logic == key:
                    o = { "bool": { eslogic: [] } }
                    for e in val:
                        o["bool"][eslogic] += _get_elt(e) 
                    res.append(o)
        else:
            if key == "label":
                res.append({ "term": { key: val } })
            else:
                res.append({'range':{ ("feature."+str(key)):{"gte":val[0],"lte":val[1]}}})
    return res

def write_filters_in_request(req, filters):
    for logic, eslogic in {"and":"must", "or":"should", "not":"must_not"}.items():
        for k in filters[logic]:
            req["query"]["filtered"]["filter"]["bool"][eslogic] += _get_elt(k)

def init_query():
    return {'query': {'filtered': {'filter': {'bool': {'must': [], 'must_not': [], 'should': []}}}}}

def count(flt):
    '''
    Given a filter, count return the number of users fitting that filter
    Examples:
    flt : {'gender':'male'}
    return value : int representing the number of users with 'male' as gender
    '''
    req = init_query()
    write_filters_in_request(req, flt)
    es = Elasticsearch(ES_NODES)
    res = es.count(index=RIOT_USERS_INDEX, body=req)['count']
    return res


'''
    Takes a function a number of processes and a list of args
    then call the function for every args in argss
    The different calls are distributed among "number of processes" processes
    Returns a list of results (order is respected)
'''
def parallelize(fun, pool, argss):
    r = pool.map_async(fun, argss)
    r.wait()
    return r.get()

class Reader(object):
    '''
    The Reader can use the DAO to ask the DB for its data.
    It should be the only object that uses the DAO
    '''
    pool = Pool(processes=8)
    def get_nb_of(self, requests):
        '''
        get_nb_of count a population which fits some filters
        '''
        res = parallelize(count, self.pool, requests)
        return res

class Filters(dict):

    def __init__(self, flts=BLANK_FILTERS):
        self["and"] = flts["and"]
        self["or"] = flts["or"]
        self["not"] = flts["not"]

    def complete_filters(self, flts):
        ''' complete_filters adds flts to self '''
        for entry in ["and", "or", "not"]:
            for flt in flts[entry]:
                self[entry].append(flt)

class Brief(object):

    def __init__(self, filters, briefSpec, comparison_filters=BLANK_FILTERS):
        self.filters = filters
        self.comp_flts = comparison_filters
        self.specs = briefSpec

    def to_json(self):
        r = Reader()
        data = {"target_size": r.get_nb_of([self.filters])[0],
                "ref_size": r.get_nb_of([self.comp_flts])[0],
                "target_data": r.get_nb_of(self._get_brief_content_filters(self.filters)),
                "ref_data": r.get_nb_of(self._get_brief_content_filters(self.comp_flts))}
        return self._format_data(data)

    def _get_brief_content_filters(self, flts):
        res = []
        for choice in self.specs["choices"]:
            flt = Filters(deepcopy(flts))
            flt.complete_filters(Filters({"and":[{self.specs["key"]:choice}], "or":[], "not":[]}))
            res.append(flt)
        return res

    def _format_data(self, data):
        xrefdata = self._xdata(data["ref_data"])
        xtgtdata = self._xdata(data["target_data"])
        res = []
        for choice in self.specs["choices"]:
            res.append({ "name": choice,
                         "target_val": self._get_val(xtgtdata.next(), data["target_size"]),
                         "ref_val": self._get_val(xrefdata.next(), data["ref_size"]) })
        return res

    def _get_val(self, data, size):
        if self.specs["format"] == "PCTG":
            return 0 if size == 0 else round(float(data) / float(size) * 100)
        if self.specs["format"] == "ABS":
            return data
        raise Exception("format number unknown")

    def _xdata(self, data):
        for d in data:
            yield d


def gen_feature_bar(feature, user, cluster):
    _, cluster_avg, genpop_delta = CLUSTER_DATA[cluster]["top"][feature["index"]]
    res = {
            "feature": feature["name"],
            "user": user["feature"][feature["name"]],
            "cluster": cluster_avg,
            "genpop": cluster_avg - genpop_delta
        }
    return res


def gen_histo(feature, user, cluster, filters, comp_filters):
    """
        #feature: { "name": "n" }
        #user: { "aggregate": { "a": "aa", "b": "bb" },
                 "feature": { "1": "11", "2": "22" } }
        #cluster: "cluster_name"
        #filters: { "and": [], "or": [], "not": [] }
    """
    choices = [[float(i)/10, float(i+1)/10] for i in range(10)]
    b = Brief(filters, { "key":feature, "choices": choices, "format":"PCTG" }, comp_filters)
    return b.to_json()