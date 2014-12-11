from elasticsearch import helpers
from elasticsearch import Elasticsearch
from config import ES_NODES


def bulk_action_gen(items, index, doctype, id_fieldname='id'):
    for item in items:
        action = {
            "_op_type": "index",
            "_id": item[id_fieldname],
            "_index": index,
            "_type": doctype,
            "_source": item,
        }
        yield action


def bulk_upsert(client, index, doctype, items, chunk_size=100, id_fieldname='id'):
    action_gen = bulk_action_gen(items, index, doctype, id_fieldname=id_fieldname)
    lst = list(action_gen)
    helpers.bulk(client=client, actions=lst, chunk_size=min(len(lst), chunk_size))


def get_ids(index, doctype, nested_field=None):
    es = Elasticsearch(ES_NODES)
    r = set()
    req = {"fields": [], "query": {"match_all": {}}}
    if nested_field is not None:
        req['fields'] = [nested_field]

    resp1 = es.search(body=req, index=index, doc_type=doctype, size=3000, search_type="scan", scroll="1m")
    scroll = resp1["_scroll_id"]
    try:
        while True:
            resp2 = es.scroll(scroll_id=scroll, scroll="1m")
            if resp2["hits"]["hits"] == []:
                break
            for res in resp2["hits"]["hits"]:
                lst = [res["_id"]] if nested_field is None else res['fields'][nested_field]
                r |= set(lst)
            print len(r)
            scroll = resp2["_scroll_id"]
    except Exception as e:
        print e
    print len(r)
    return r
