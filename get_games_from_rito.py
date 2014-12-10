from elasticsearch import Elasticsearch
from config import ES_NODES
from argparse import ArgumentParser


def get_ids(index, doctype):
    es = Elasticsearch(ES_NODES)
    r = []
    req = {"fields":[],"query":{"match_all":{}}}

    resp1 = es.search(body=req, index=index, doc_type=doctype, size=3000, search_type="scan", scroll="1m")
    scroll = resp1["_scroll_id"]
    try:
        while True:
            resp2 = es.scroll(scroll_id=scroll,scroll="1m")
            if resp2["hits"]["hits"] == []:
                break
            for gid in resp2["hits"]["hits"]:
                r.append(gid["_id"])
            print len(r)
            scroll = resp2["_scroll_id"]
    except Exception as e:
        print e
    print len(r)
    return r


if __name__ == "__main__":
    ap = ArgumentParser(description="dump game IDs to a file")
    ap.add_argument("-i", "--index", default="rito", help="Elasticsearch Index")
    ap.add_argument("-d", "--doctype", default="game", help="Document type")
    ap.add_argument("-o", "--output", default="games.txt", help="Output filepath")
    args = ap.parse_args()
    with open(args.output, "w") as fh:
        for a in get_ids(args.index, args.doctype):
            fh.write(str(a)+"\n")

