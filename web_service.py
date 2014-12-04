from bottle import route, run, template, request
from user_service import UserService
from cluster_service import ClusterService
import json

## these should go to config once we know where to store / how to store them
scaler_file = "labelling_files/scaler_9g"
clf_file = "labelling_files/clf_9g"

us = UserService()
cs = ClusterService(scaler_file, clf_file)

@route('/userservice/<region>/<summonername>')
def get_user_data(region,summonername):
    res = us.get_crunched_user(summonername, region)
    if res[0] == '200':
        ret = dict()
        ret["user"] = res[1]
        ret["label"] = cs.get_user_cluster(res[1])
        retjs = json.dumps(ret, separators=(',',':'))
        return retjs
    else:
        return "Error not found %s" % res[1]

run(host='localhost', port=8080)
