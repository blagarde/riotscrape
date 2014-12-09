from elasticsearch import helpers
import json


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
    helpers.bulk(client=client, actions=action_gen, chunk_size=chunk_size)
