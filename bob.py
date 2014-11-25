
from elasticsearch import helpers, Elasticsearch
from collections import defaultdict

class RitaGetter(object):

    def __init__(self, index):
        self.es = Elasticsearch([{'host': '146.148.120.191', 'port': 8000},{'host': '104.155.13.217', 'port': 8000},])
        self.index = index
    def get_all(self):
        return helpers.scan(client=self.es, query={}, scroll="30m", index=self.index, doc_type='user', timeout="200m")

    def get_by_id(self, user_id):
        return self.es.get(index=self.index, doc_type='user', id=user_id)



rita = RitaGetter("ritu")
g = rita.get_all()

i = 400000
m = 0
p = 0
gg = 0
nc = 0
ng = 0
nsg = 0
pnc = 0
png = 0
pnsg = 0
lll = 0
ddd = defaultdict(int)
try:
    while i > 0:
        i -= 1
        u = g.next()["_source"]
        if True:
            ddd[u["aggregate"]["nGame"]] += 1
            m += 1
            if  'nGame' not in u["aggregate"] or u["aggregate"]['nGame'] == 0:
                lll += 1
            nsg += u["aggregate"]['nSubGame'] if 'nSubGame' in u["aggregate"] else 0
            nc += u["aggregate"]["nClassicGame"] if 'nClassicGame' in u["aggregate"] else 0
            ng += u["aggregate"]["nGame"] if 'nGame' in u["aggregate"] else 0
            gg += len(u["games_id_list"])
            pnc = float(nc)/m
            png = float(ng)/m
            pnsg = float(nsg)/m
            p = float(gg)/m
            
except Exception as e:
    print e
    print i

print "classicgame: " + str(pnc)
print "ngame: " + str(png)
print "nsubgame: " + str(pnsg)
print "ngames: " + str(p)
print "nusers: " + str(m)
print lll
print ddd
#    if "nClassicGame" in u["aggregate"] and u["aggregate"]["nClassicGame"] != 0:
#        print float(u["aggregate"]["nAssists"])/u["aggregate"]["nClassicGame"]