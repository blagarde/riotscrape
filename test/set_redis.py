from subprocess import Popen, PIPE


def create_redis_file():
    file_ = open("redis.txt", "w")
    file_.write("DEL games\n")
    file_.write("DEL users_set\n")
    with open("games_sample.txt", "r") as f:
        for gameid in f.readlines():
            file_.write("LPUSH games "+str(gameid[:-1])+"\n")
    with open("users_sample.txt", "r") as f:
        for userid in f.readlines():
            file_.write("SADD users_set "+str(userid[:-1])+"\n")
    file_.close()


def setup_redis():
    Popen("cat redis.txt | redis-cli", stdout=PIPE, stderr=PIPE, shell=True)

if __name__ == "__main__":
    create_redis_file()
    setup_redis()
