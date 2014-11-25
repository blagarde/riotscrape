import json

dat = json.loads(data)
ctx['_source']["games_id_list"] += dat["games_id_list"]
for field, value in dat["aggregate"].items():
    if field == "nChamp":
        for champ_id, count in value.items():
            if champ_id in ctx['_source']["aggregate"]["nChamp"]:
                ctx['_source']["aggregate"]["nChamp"][champ_id] += count
            else:
                ctx['_source']["aggregate"]["nChamp"][champ_id] = count
    else:
       ctx['_source']["aggregate"][field] += value