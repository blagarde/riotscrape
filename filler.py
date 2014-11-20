# from config import REDIS_PARAM
# from redis import StrictRedis as Buffer
# from multiprocessing import Pool

users = []
games = []
file_ = open("redis.txt", "a+")


with open("games.txt", "r") as f:
    for gameid in f.readlines():
        #games.append(gameid[:-1])
        file_.write("LPUSH games "+str(gameid[:-1])+"\n")


with open("users.txt", "r") as f:
    for userid in f.readlines():
        #users.append(userid[:-1])
        file_.write("LPUSH users "+str(userid[:-1])+"\n")
        file_.write("SADD users_set "+str(userid[:-1])+"\n")

file_.close()
#  
# def g(games):
#     buffer_ = Buffer(**REDIS_PARAM)
#     for gameid in games:
#         buffer_.lpush("games", gameid)
#  
# def u(users):
#     buffer_ = Buffer(**REDIS_PARAM)
#     for userid in users:
#         buffer_.lpush("users", userid)
#         buffer_.sadd("users_set", userid)
#  
# pool = Pool(processes=8)
# r1 = pool.map_async(g, [games[i:i+10000] for i in xrange(len(games)/10000)])
# r2 = pool.map_async(g, [users[i:i+10000] for i in xrange(len(users)/10000)])
# r1.wait()
# r2.wait()