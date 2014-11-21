
from elasticsearch import helpers, Elasticsearch


class RitaGetter(object):

    def __init__(self):
        self.es = Elasticsearch([{'host': '146.148.125.71', 'port': 8000}, {'host': '146.148.120.191', 'port': 8000}, {'host': '130.211.49.140', 'port': 8000}])

    def get_all(self):
        return helpers.scan(client=self.es, query={}, scroll="20m", index='ritu', doc_type='user', timeout="10m")

    def get_by_id(self, user_id):
        return self.es.get(index='ritu', doc_type='user', id=user_id)



rita = RitaGetter()
g = rita.get_all()

i = 144300
m = 0
try:
    while i > 0:
        i -= 1
        u = g.next()["_source"]
        if u["feature"] != {} and len(u["games_id_list"]) > 2:
            m += 1
except:
    print i
print m
#    if "nClassicGame" in u["aggregate"] and u["aggregate"]["nClassicGame"] != 0:
#        print float(u["aggregate"]["nAssists"])/u["aggregate"]["nClassicGame"]