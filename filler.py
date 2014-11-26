users = []
games = []
file_ = open("redis.txt", "a+")

with open("games.txt", "r") as f:
    for gameid in f.readlines():
        file_.write("ZADD games 0 "+str(gameid[:-1])+"\n")


file_.close()
