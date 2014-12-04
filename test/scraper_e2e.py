from riotscrape import WatcherThread, Tasks
from unittest import TestCase
from config import TEST_KEY
from config import GAME_QUEUE, USER_QUEUE, GAME_SET, USER_SET
from time import sleep
from redis import StrictRedis
from interruptingcow import timeout
from elasticsearch import Elasticsearch


class ThreadingTests(TestCase):
    def setUp(self):
        self.es = Elasticsearch({"host": "localhost", "port": "8000"})
        for key in GAME_QUEUE, USER_QUEUE, GAME_SET, USER_SET:
            print(key)
        raw_input("About to delete the above-listed Redis keys. CTRL-C to abort.")
        for key in GAME_QUEUE, USER_QUEUE, GAME_SET, USER_SET:
            Tasks.delete(key)
        sleep(10)  # Ensure API available

    def test_games_make_it_to_elasticsearch_in_reasonable_time(self):
        Tasks.add(TEST_GAMES, TEST_USERS)
        wt = WatcherThread(TEST_KEY, cycles=1)
        wt.start()
        REASONABLE_TIME = 10  # seconds
        with timeout(REASONABLE_TIME):
            while True:
                try:
                    self.es.mget(index='test', doc_type='game', body={'ids': TEST_GAMES})
                    # TODO - assert that the all items made it to ES 
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
        Tasks.add(TEST_GAMES, TEST_USERS)
        wt = WatcherThread(TEST_KEY, cycles=2)
        wt.run()

        # 1. check that the game queue is not empty
        self.assertGreater(Tasks.redis.llen(GAME_QUEUE), 0)

        # 2. check that none of the test games are now currently queued
        ONE_SHITLOAD = 10000
        newly_queued_games = Tasks.redis._bulk_rpop(GAME_QUEUE, ONE_SHITLOAD)
        self.assertEquals(len(set(newly_queued_games) | (TEST_GAMES)), 0)

        # 3. check that seeded TEST_GAMEs are still in GAME_SET after the second iteration
        items, is_old = zip(*Tasks.redis._intersect(GAME_SET, TEST_GAMES, insert=False))
        self.assertTrue(all(is_old))

        # 4. check that some new users got queued
        self.assertNotEqual(Tasks.redis.llen(USER_QUEUE), 0)

        # 5. check that no users got processed
        self.assertNotEqual(Tasks.redis.scard(USER_SET), 0)

        # 6. check that task counts are accurate
        self.assertEquals(Tasks.total_games, len(TEST_GAMES))
        self.assertEquals(Tasks.new_games, len(TEST_GAMES))

    def test_multi_thread(self):
        Tasks.add(TEST_MANY_GAMES, TEST_USERS)
        wt1 = WatcherThread(TEST_KEY, cycles=1)
        wt2 = WatcherThread(TEST_KEY2, cycles=1)

        wt1.start()
        wt2.start()

        wt1.join()
        wt2.join()

        # 1. check that the task counts are accurate
        self.assertEquals(Tasks.total_games, )

if __name__ == "__main__":
    unittest.main()
