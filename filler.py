# from config import REDIS_PARAM
# from redis import StrictRedis as Buffer
# from multiprocessing import Pool

users = []
games = []
file_ = open("redis.txt", "a+")

with open("games.txt", "r") as f:
    for gameid in f.readlines():
        file_.write("ZADD games 0 "+str(gameid[:-1])+"\n")


file_.close()
