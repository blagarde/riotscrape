from elasticsearch import Elasticsearch, helpers
from update_data import user, to_add, user_updated
import json


class UpdateTraining(object):
    es = Elasticsearch([{'host': '130.211.49.140', 'port': 8000}])

    def index_user(self):
        return self.es.index(index='test', doc_type='user', id=1, body=user)

    def update_user(self, data):
        return helpers.bulk(client=self.es, actions=self.build_actions(data))

    def build_actions(self, data):
        for d in data:
            query = {
                "_op_type": "update",
                "_id": 1,
                "_index": 'test',
                "_type": 'user',
                "script": "update_agg_data",
                "params": {"data": json.dumps(d)},
                "upsert": d,
                }
            yield query

    def update(self):
        self.es.update(index='rita', doc_type='user', id=1, body={"script": "ctx._source.nMinions += 100"})

if __name__ == "__main__":
    ut = UpdateTraining()
    # ut.index_user()
    print ut.update_user([to_add])