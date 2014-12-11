users = []
games = []
file_ = open("redis.txt", "w")

with open("gamesrito.txt", "r") as f:
    for gameid in f.readlines():
        file_.write("RPUSH scraper:games_out "+str(gameid[:-1])+"\n")


file_.close()
