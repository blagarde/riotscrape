import json

dat = json.loads(data)
ctx['_source']["games_id_list"] += dat["games_id_list"]
for field, value in dat["aggregate"].items():
    if field == "Champ":
        for champ_id, count in value.items():
            if champ_id in ctx['_source']["aggregate"][field]:
                ctx['_source']["aggregate"][field][champ_id] += count
            else:
                ctx['_source']["aggregate"][field][champ_id] = count
    else:
        if field in ctx['_source']["aggregate"]:
            ctx['_source']["aggregate"][field] += value
        else :
            ctx['_source']["aggregate"][field] = value