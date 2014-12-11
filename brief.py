from multiprocessing import Pool
from elasticsearch.client import Elasticsearch
from config import ES_NODES, BLANK_FILTERS
from copy import deepcopy


CLUSTER_DATA = {
        0: {
            'count': 19314,
            'top': [
                ('role',
                0.5250400539890705,
                0.15503665010391465),
                ('solo',
                0.5728517060761398,
                0.15167097920733186),
                ('loyalty',
                0.2624550089086491,
                0.113467147344253),
                ('teamplay',
                0.315978534244557,
                -0.10469925656239937),
                ('position',
                0.4035167229234775,
                0.03800076865933871),
                ('strategy',
                0.22818659296544017,
                -0.012370542132689427),
                ('action',
                0.3290872741509492,
                0.005114788130947567)
            ]
        },
        1: {
            'count': 32581,
            'top': [
                ('solo',
                0.3069566023768998,
                -0.11422412449190811),
                ('position',
                0.4754761555633881,
                0.10996020129924933),
                ('teamplay',
                0.504724412959805,
                0.08404662215284858),
                ('action',
                0.29011932808563173,
                -0.03385315793436988),
                ('strategy',
                0.2722542407654162,
                0.03169710566728662),
                ('loyalty',
                0.1785755337646831,
                0.02958767220028702),
                ('role',
                0.3777979150991183,
                0.007794511213962463)
            ]
        },
        2: {
            'count': 70481,
            'top': [
                ('role',
                0.30779551393885074,
                -0.062207889946305106),
                ('position',
                0.32368777107428287,
                -0.04182818318985593),
                ('loyalty',
                0.11418326228437363,
                -0.034804599280022455),
                ('solo',
                0.3955936562592702,
                -0.025587070609537743),
                ('teamplay',
                0.4414749248175933,
                0.02079713401063693),
                ('strategy',
                0.24905612195507912,
                0.008498986856949525),
                ('action',
                0.32224013216396,
                -0.0017323538560415908)
            ]
        },
        3: {
            'count': 69071,
            'top': [
                ('position',
                0.297169494056667,
                -0.06834646020747182),
                ('solo',
                0.48160468085175384,
                0.06042395398294592),
                ('teamplay',
                0.37240318353173474,
                -0.04827460727522165),
                ('loyalty',
                0.11495852738465474,
                -0.03402933417974134),
                ('strategy',
                0.21422586133142854,
                -0.026331273766701052),
                ('action',
                0.3473686477596499,
                0.023396161739648258),
                ('role',
                0.35653235908076997,
                -0.013471044804385879)
            ]
        },
        4: {
            'count': 8553,
            'top': [
                ('position',
                0.7574591751192588,
                0.39194322085511996),
                ('role',
                0.6116264597737138,
                0.24162305588855792),
                ('loyalty',
                0.34166910454555155,
                0.19268124298115546),
                ('solo',
                0.2366866669758876,
                -0.1844940598929203),
                ('teamplay',
                0.5554154316098552,
                0.1347376408028988),
                ('action',
                0.2667159053240329,
                -0.05725658069596873),
                ('strategy',
                0.29035388111463156,
                0.04979674601650197)
            ]
        }
    }

def _get_elt(elt):
    res = []
    for key, val in elt.items():
    # for key, val in elt:
        if key in ["and", "or", "not"]:
            for logic, eslogic in {"and":"must", "or":"should", "not":"must_not"}.items():
                if logic == key:
                    o = { "bool": { eslogic: [] } }
                    for e in val:
                        o["bool"][eslogic] += _get_elt(e) 
                    res.append(o)
        else:
            if key == "label":
                res.append({ "term": {key:val} })
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
    res = es.count(index="lagrosserita", body=req)['count']
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
    def get_nb_of(self, requests, aud="undefined"):
        '''
        get_nb_of count a population which fits some filters
        '''
        #res = parallelize(count, self.pool, requests)
        res = []
        for r in requests:
            res.append(count(r))
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
    res = {
            "feature": feature["name"],
            "user": user["feature"][feature["name"]],
            "cluster": CLUSTER_DATA[cluster]["top"][feature["index"]][1],
            "genpop": CLUSTER_DATA[cluster]["top"][feature["index"]][1]-CLUSTER_DATA[cluster]["top"][feature["index"]][2]
        }
    return res

"""
    #feature: { "name": "n",
                "choices": ["a","b","c"] }
    #user: { "aggregate": { "a": "aa", "b": "bb" },
             "feature": { "1": "11", "2": "22" } }
    #cluster: "cluster_name"
    #filters: { "and": [], "or": [], "not": [] }
"""
def gen_histo(feature, user, cluster, filters, comp_filters):
    b = Brief(filters, { "key":feature, "choices": [[float(i)/10, float(i+1)/10] for i in range(10)], "format":"PCTG" }, comp_filters)
    return b.to_json()