from redis import StrictRedis as Buffer
from config import REDIS_PARAM


def init_redis_for_testing_cruncher(game_file,user_file):
    buffer = Buffer(**REDIS_PARAM)
    buffer.delete('scraper_out')
    buffer.delete('user_set')
    buffer.delete('cruncher_out')
    with open(game_file, "r") as f:
        for game_id in f.readlines():
            buffer.lpush('scraper_out', game_id.strip())
    with open(user_file, "r") as f:
        for user_id in f.readlines():
            buffer.sadd('user_set', user_id.strip())


if __name__ == "__main__":
    init_redis_for_testing_cruncher("test/game_id_sample.txt","test/user_sample.txt")

