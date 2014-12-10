import unittest
from init_redis import init_redis_for_testing_cruncher
from init_elasticsearch import init_elasticsearch_for_testing_cruncher, delete_elasticsearch_index
from crunchers import GameCruncher
from config import RIOT_GAMES_INDEX, RIOT_USERS_INDEX, ES_NODES, TESTING
from elasticsearch import Elasticsearch
from time import sleep


class CruncherTests(unittest.TestCase):

    def setUp(self):
        '''
        Set up Elasticsearch and Redis
        '''
        if not TESTING:
            raise EnvironmentError("You are not in testing mode, you could have deleted the prod instances!")
        print "WARNING : the script update_agg_data.py needs to be in the config/scripts folder of your local ES instance or cruncher tests will fail"
        init_redis_for_testing_cruncher("game_id_sample.txt", "user_sample.txt")
        init_elasticsearch_for_testing_cruncher()
        sleep(1)

    def test_from_redis_to_elasticsearch(self):
        '''
        Crunch mock data and then retrieve crunched users
        '''
        gc = GameCruncher()
        gc.crunch()
        sleep(2)
        es = Elasticsearch(ES_NODES)
        print "Retreive crunched data"
        nb_user_crunched = es.count(index=RIOT_USERS_INDEX)
        self.assertEqual(10, nb_user_crunched['count'])

    def tearDown(self):
        '''
        Clean Elasticsearch instance
        '''
        delete_elasticsearch_index(RIOT_GAMES_INDEX)
        delete_elasticsearch_index(RIOT_USERS_INDEX)


if __name__ == '__main__':
    unittest.main()
