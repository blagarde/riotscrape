from subprocess import Popen, PIPE


def create_redis_file(game_file, user_file):
    file_ = open("redis.txt", "w")
    file_.write("DEL games\n")
    file_.write("DEL users_set\n")
    with open(game_file, "r") as f:
        for gameid in f.readlines():
            file_.write("ZADD games 0 "+str(gameid[:-1])+"\n")
    with open(user_file, "r") as f:
        for userid in f.readlines():
            file_.write("SADD users_set "+str(userid[:-1])+"\n")
    file_.close()


def setup_redis(hostname):
    Popen("cat redis.txt | redis-cli -h " + hostname, stdout=PIPE, stderr=PIPE, shell=True)

if __name__ == "__main__":
    create_redis_file("games.txt","users.txt")
    # setup_redis("130.211.48.74")
