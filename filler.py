# from config import REDIS_PARAM
# from redis import StrictRedis as Buffer
# from multiprocessing import Pool

users = []
games = []
file_ = open("redis.txt", "w")

with open("games_sample.txt", "r") as f:
    for gameid in f.readlines():
        file_.write("LPUSH games "+str(gameid[:-1])+"\n")

with open("users_sample.txt", "r") as f:
    for userid in f.readlines():
        # file_.write("LPUSH users "+str(userid[:-1])+"\n")
        file_.write("SADD users_set "+str(userid[:-1])+"\n")

file_.close()
