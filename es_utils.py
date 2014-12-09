from elasticsearch import helpers


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
