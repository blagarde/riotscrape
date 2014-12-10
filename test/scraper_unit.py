from unittest import TestCase
from riotscrape import CustomRedis
import re
from config import REDIS_PARAM
from itertools import izip


def get_counts(msg):
    regex = re.compile(r'total_games/new_games\t(?P<total>\d+)\t(?P<new>\d+)')
    dct = regex.match(msg).groupdict()
    return int(dct['total']), int(dct['new'])


QUEUE = 'test_queue'
SET = 'test_set'
TEST_LST = map(str, range(12345))


class RedisQueueTests(TestCase):
    def setUp(self):
        self.r = CustomRedis(**REDIS_PARAM)
        self.r.delete(QUEUE)

    def test_bulk_rpop1(self):
        self.r.lpush(QUEUE, *TEST_LST)
        popped = self.r._bulk_rpop(QUEUE, len(TEST_LST))
        self._compare_iterables(popped, TEST_LST)

    def test_bulk_rpop2(self):
        self.r.lpush(QUEUE, *TEST_LST)
        popped = self.r._bulk_rpop(QUEUE, len(TEST_LST) - 1)
        self._compare_iterables(popped, TEST_LST[:-1])

    def test_bulk_rpop3(self):
        self.r.lpush(QUEUE, *TEST_LST[:-1])
        popped = self.r._bulk_rpop(QUEUE, len(TEST_LST))
        self._compare_iterables(popped, TEST_LST[:-1])

    def tearDown(self):
        self.r.delete(QUEUE)

    def _compare_iterables(self, i1, i2):
        for i, j in izip(i1, i2):
            self.assertEqual(i, j)


class RedisSetTests(TestCase):
    def setUp(self):
        self.r = CustomRedis(**REDIS_PARAM)
        self.r.delete(SET)
        self.r._bulk_sadd(SET, TEST_LST, step=2)

    def test_bulk_sadd(self):
        card = self.r.scard(SET)
        self.assertEqual(card, len(TEST_LST))
        self._assert_stored(TEST_LST)

    def test_intersect_noinsert_1(self):
        lst = self.r._intersect(SET, TEST_LST, insert=False)
        items, stored = zip(*lst)
        self._assert_stored(items)
        self.assertTrue(all(stored))

    def test_intersect_noinsert_2(self):
        lst = self.r._intersect(SET, TEST_LST + [-1], insert=False)
        items, stored = zip(*lst)
        self._assert_stored(items[:-1])
        self.assertTrue(all(stored[:-1]))
        self.assertFalse(self.r.sismember(SET, -1))
        self.assertFalse(self.r.sismember(SET, '-1'))
        self.assertFalse(stored[-1])

    def test_intersect_insert_1(self):
        lst = self.r._intersect(SET, [1, -1, -2], insert=True)
        items, stored = zip(*lst)
        self._assert_stored(items)
        self.assertEquals(stored, (True, False, False))

    def _assert_stored(self, lst):
        for i in lst:
            self.assertTrue(self.r.sismember(SET, i))

    def tearDown(self):
        self.r.delete(SET)
