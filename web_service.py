from bottle import route, run, template, request
from user_service import UserService
from cluster_service import ClusterService
import json
from brief import gen_histo, gen_feature_bar
from config import BLANK_FILTERS
from cluster_data import CLUSTER_DATA

## these should go to config once we know where to store / how to store them
scaler_file = "labelling_files/scaler.pkl"
clf_file = "labelling_files/clf.pkl"

us = UserService()
cs = ClusterService(scaler_file, clf_file)

@route("/userservice/<region>/<summonername>/<feature>")
def get_user_data(region,summonername,feature):
    res = us.get_crunched_user(summonername, region)
    if res[0] == "200":
        ret = dict()
        usr = res[1]
        ret["label"] = cs.get_user_cluster(usr)
        if ret["label"] == -1:
            return "Sorry man"
        ret["features"] = [gen_feature_bar({ "name":f[0], "index":i }, usr, ret["label"]) for i,f in enumerate(CLUSTER_DATA[ret["label"]]["top"])]
        ret["graph"] = gen_histo(feature, usr, ret["label"], {"and":[{"cluster":ret["label"]}], "or":[], "not":[]}, BLANK_FILTERS)
        retjs = json.dumps(ret, separators=(",",":"))
        return retjs
    else:
        return "Error not found %s" % res[1]

run(host="0.0.0.0", port=80)

