import unittest
from elasticsearch import Elasticsearch, helpers
from data_sample import user, to_add, user_updated
import json
from init_elasticsearch import init_elasticsearch_index, delete_elasticsearch_index
from time import sleep


class ScriptTests(unittest.TestCase):
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    def setUp(self):
        init_elasticsearch_index('test_script')
        sleep(2)
        self.index_user()

    def test_update_agg_data_script(self):
        self.update_user([to_add])
        sleep(1)
        result = self.get_user()
        self.assertUserEqual(result, user_updated)

    def tearDown(self):
        delete_elasticsearch_index('test_script')

    def assertUserEqual(self, user1, user2):
        self.assertDictEqual(user1['aggregate']['Champ'], user2['aggregate']['Champ'])
        for field in user1['aggregate']:
            if field == 'Champ':
                continue
            self.assertEqual(user1['aggregate'][field], user2['aggregate'][field])
        self.assertEqual(user1['games_id_list'], user2['games_id_list'])

    def index_user(self):
        return self.es.index(index='test_script', doc_type='user', id=1, body=user)

    def update_user(self, data):
        return helpers.bulk(client=self.es, actions=self._build_actions(data))

    def _build_actions(self, data):
        for d in data:
            query = {
                "_op_type": "update",
                "_id": 1,
                "_index": 'test_script',
                "_type": 'user',
                "script": "update_agg_data",
                "params": {"data": json.dumps(d)}
            }
            yield query

    def get_user(self):
        return self.es.get(index='test_script', doc_type='user', id=1)["_source"]

if __name__ == '__main__':
    unittest.main()