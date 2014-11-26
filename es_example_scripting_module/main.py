from elasticsearch import Elasticsearch, helpers
from data_sample import user, to_add
import json


#NOTA : before trying this example you need to :
#        - create a test index on your local ES cluster
#        - install the lang-python plugin
#        - copy the update_agg_data.py file in your directory elasticsearch/config/script/


class ScriptingModuleExample(object):
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

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
                }
            yield query

if __name__ == "__main__":
    sme = ScriptingModuleExample()
    print sme.index_user()
    print sme.update_user([to_add])