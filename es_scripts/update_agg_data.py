data, ctx = 0,0
import json
dat = json.loads(data)
ctx['_source']['games_id_list'] += dat['games_id_list']
for field, value in dat['aggregate'].items():
    if field == 'nChamp':
        for champ_id, count in value.items():
            if champ_id in ctx['_source']['aggregate']['nChamp']:
                ctx['_source']['aggregate']['nChamp'][champ_id] += count
            else:
                ctx['_source']['aggregate']['nChamp'][champ_id] = count
    else:
        if field in ctx['_source']['aggregate']:
            ctx['_source']['aggregate'][field] += value
        else:
            ctx['_source']['aggregate'][field] = value
{
    "script": "import json\ndat = json.loads(data)\nctx['_source']['games_id_list'] += dat['games_id_list']\nfor field, value in dat['aggregate'].items():\n\tif field == 'nChamp':\n\t\tpass\n\telse:\n\t\tif field in ctx['_source']['aggregate']:\n\t\t\tctx['_source']['aggregate'][field] += value\n\t\telse:\n\t\t\tctx['_source']['aggregate'][field] = value"
}
