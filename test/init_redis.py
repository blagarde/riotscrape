from redis import StrictRedis as Buffer
from config import REDIS_PARAM, TO_CRUNCHER, TO_USERCRUNCHER


def init_redis_for_testing_cruncher(game_file):
    buffer = Buffer(**REDIS_PARAM)
    buffer.delete(TO_CRUNCHER)
    buffer.delete(TO_USERCRUNCHER)
    with open(game_file, "r") as f:
        for game_id in f.readlines():
            buffer.lpush(TO_CRUNCHER, game_id.strip())

if __name__ == "__main__":
    init_redis_for_testing_cruncher("game_id_sample.txt")

