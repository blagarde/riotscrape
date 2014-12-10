from bottle import route, run, template, request
from user_service import UserService

us = UserService()
@route('/userservice/<region>/<summonername>')
def get_user_data(region,summonername):
    res = us.get_crunched_user(summonername, region)
    return res[1]

run(host='localhost', port=8080)