import os
from riotscrape import WatcherThread, Tasks
from unittest import TestCase
import unittest
from config import TEST_KEY, TEST_KEY2, ES_NODES, GAME_DOCTYPE
from config import GAME_QUEUE, USER_QUEUE, GAME_SET, USER_SET, TO_CRUNCHER
from time import sleep
from interruptingcow import timeout
from elasticsearch import Elasticsearch

HERE = os.path.abspath(os.path.dirname(__file__))
TEST_GAME_FILE = os.path.join(HERE, '53rankedgames.txt')

TEST_GAMES = [l.rstrip() for l in open(TEST_GAME_FILE).readlines()[:10] if l.rstrip() != '\n']
TEST_MANY_GAMES = [l.rstrip() for l in open(TEST_GAME_FILE).readlines()[:40] if l.rstrip() != '\n']
TEST_USERS = ['31868', '21496958', '37877378', '44118661', '29057304']


TEST_ES_INDEX = 'scrapertest'


class ThreadingTests(TestCase):
    def setUp(self):
        self.es = Elasticsearch(ES_NODES)
        print GAME_QUEUE, Tasks.redis.llen(GAME_QUEUE)
        print USER_QUEUE, Tasks.redis.llen(USER_QUEUE)
        print GAME_SET, Tasks.redis.scard(GAME_SET)
        print USER_SET, Tasks.redis.scard(USER_SET)
        print TO_CRUNCHER, Tasks.redis.llen(TO_CRUNCHER)
        Tasks.new_games = 0
        print "Deleting the above-listed Redis keys."
        for key in GAME_QUEUE, USER_QUEUE, GAME_SET, USER_SET, TO_CRUNCHER:
            Tasks.redis.delete(key)
        self.es.delete_by_query(index=TEST_ES_INDEX, doc_type=GAME_DOCTYPE, body={"query": {"match_all": {}}})
        print "Be patient (10s) - making sure API is available"
        sleep(10)
        print "Ready!"

    def test_games_make_it_to_elasticsearch_in_reasonable_time(self):
        Tasks.add(TEST_GAMES, [])
        wt = WatcherThread(TEST_KEY, cycles=1)
        wt.start()
        REASONABLE_TIME = 20  # seconds
        with timeout(REASONABLE_TIME):
            while True:
                try:
                    # TODO - assert that the all items made it to ES
                    docs = self.es.mget(index=TEST_ES_INDEX, doc_type=GAME_DOCTYPE, body={'ids': TEST_GAMES})['docs']
                    assert all([d['found'] for d in docs])
                    break
                except:
                    pass
                sleep(0.1)
        wt.join()

        # 1. check that the game queue is now empty
        ONE_SHITLOAD = 10000
        self.assertGreater(ONE_SHITLOAD, Tasks.redis.llen(GAME_QUEUE))
        newly_queued_games = Tasks.redis._bulk_rpop(GAME_QUEUE, ONE_SHITLOAD)
        self.assertEquals(len(set(newly_queued_games)), 0)

        # 2. check that processed games made it to the GAME_SET
        self.assertEquals(Tasks.redis.scard(GAME_SET), len(set(TEST_GAMES)))
        items, is_old = zip(*Tasks.redis._intersect(GAME_SET, TEST_GAMES, insert=False))
        self.assertTrue(all(is_old))

    def test_games_and_users_properly_queued(self):
        # Init with 10 games and 5 users
        Tasks.add(TEST_GAMES, TEST_USERS)
        wt = WatcherThread(TEST_KEY, cycles=1)
        wt.run()

        # 1. check that none of the test games are now currently queued
        ONE_SHITLOAD = 10000
        newly_queued_games = Tasks.redis._bulk_rpop(GAME_QUEUE, ONE_SHITLOAD)
        self.assertEquals(len(set(newly_queued_games) & set(TEST_GAMES)), 0)

        # 2. check that seeded TEST_GAMEs are still in GAME_SET after the second iteration
        items, is_old = zip(*Tasks.redis._intersect(GAME_SET, TEST_GAMES, insert=False))
        self.assertTrue(all(is_old))

        # 3. check that some new users got added
        self.assertNotEqual(Tasks.redis.scard(USER_SET), 0)

        # 4. check that some new games got added
        self.assertNotEqual(Tasks.redis.scard(GAME_SET), 0)

        # 5. check that game counts are accurate
        self.assertEquals(Tasks.new_games, len(TEST_GAMES) + len(newly_queued_games))

    def test_multi_thread(self):
        Tasks.add(TEST_MANY_GAMES, TEST_USERS)
        wt1 = WatcherThread(TEST_KEY, cycles=1)
        wt2 = WatcherThread(TEST_KEY2, cycles=1)

        wt1.start()
        wt2.start()

        wt1.join()
        wt2.join()

        # 1. check that the game counts are accurate
        self.assertEquals(Tasks.new_games, len(TEST_MANY_GAMES) + Tasks.redis.llen(GAME_QUEUE))

if __name__ == "__main__":
    unittest.main()
