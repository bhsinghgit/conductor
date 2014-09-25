import flask
import json
import functools
import hashlib
import uuid
import time
import MySQLdb

app       = flask.Flask(__name__)
db_conn   = MySQLdb.connect('localhost', 'root', '', 'conductor')
db_cursor = db_conn.cursor()

def transaction(f):
    @functools.wraps(f)
    def f1(*args, **kwargs):
        try:
            response = f(*args, **kwargs)
            db_conn.commit()
            status = 200
        except Exception as e:
            response = str(e)
            db_conn.rollback()
            status = 400

        return flask.Response(json.dumps(response),
                              status,
                              mimetype='application/json')
    return f1

def query(sql, params=None):
    db_cursor.execute(sql, params)
    return db_cursor.fetchall()

def guid():
    return hashlib.md5(str(uuid.uuid4())).hexdigest()

def validate_request():
    req = json.loads(flask.request.data)

    if 'authkey' not in req:
        raise Exception('authkey missing')

    if 'appname' not in req:
        raise Exception('appname missing')

    row = query("""select appid, authkey, state, description
                   from applications
                   where appname=%s and state='active'
                """,
                (req['appname']))

    if 1 != len(row):
        raise Exception('invalid appname')

    if 'active' != row[0][2]:
        raise Exception('application is not active')

    if req['authkey'] != row[0][1]:
        raise Exception('key mismatch')

    req.update(dict(appid=row[0][0],
                    desc=json.loads(row[0][3]),
                    agent_ip=flask.request.remote_addr))

    return req

@app.route('/applications', methods=['POST'])
@transaction
def add_app():
    req  = json.loads(flask.request.data)
    desc = json.dumps(json.loads(req['desc']))
    key  = guid()
  
    query("""insert into applications
             set appname=%s, authkey=%s, state='active', description=%s
          """,
          (req['appname'], key, desc))

    return dict(authkey=key)

@app.route('/applications', methods=['GET'])
@transaction
def get_apps():
    sql = """select appid, appname, authkey, description
             from applications
             where state='active'
          """
    ret = dict()
    for appid, appname, authkey, description in query(sql):
        ret[appid] = json.loads(description)
        ret[appid]['appname'] = appname
        ret[appid]['authkey'] = authkey
    return ret

@app.route('/workers', methods=['POST'])
@transaction
def add_worker():
    req = validate_request()

    workername = req['worker'].get('workername', guid())
    input      = req['worker']['input']

    query("""delete from messages where appid=%s and workername=%s""",
          (req['appid'], workername))
    query("""insert into workers
             set workername=%s, appid=%s, state='active', input=%s, tmp='{}'
          """,
          (workername, req['appid'], input))

    return dict(workername=workername)

def mark_head(appid, workername):
    msgid = query("""select msgid from messages
                     where appid=%s and workername=%s
                     order by timestamp, msgid
                     limit 1
                  """,
                  (appid, workername))
    if len(msgid) > 0:
        query("update messages set state='head' where msgid=%s ", (msgid[0][0]))

@app.route('/messages', methods=['POST'])
@transaction
def add_msg():
    req = validate_request()
    msg = req['msg']

    pool  = msg.get('pool', 'default')
    data  = msg.get('data', None)
    delay = msg.get('delay', 0)

    appid = query("""select appid from workers
                     where workername=%s and state != 'done'
                  """,
                  (msg['workername']))
    if 1 != len(appid):
        raise Exception('invalid workername or state')
    else:
        appid = appid[0][0]

    query("""insert into messages
             set workername=%s, appid=%s, pool=%s, state='queued',
                 code=%s, data=%s, timestamp=now()+interval %s second
          """,
          (msg['workername'], appid, pool, msg['code'], data, delay))

    mark_head(appid, msg['workername'])

    return "OK"

@app.route('/messages', methods=['GET'])
@transaction
def get_msgs():
    sql = """select appid, pool, count(*) as count
             from messages
             where timestamp < now() and state='head'
             group by appid, pool"""
    ret = dict()
    for appid, pool, count in query(sql):
        ret.setdefault(appid, dict())[pool] = count
    return ret

@app.route('/update', methods=['POST'])
@transaction
def update():
    req = validate_request()
    com = req['commit']

    query("update workers set output=%s, tmp=%s where workername=%s",
          (com['output'], com['tmp'], com['workername']))

    return "OK"

@app.route('/commit', methods=['POST'])
@transaction
def commit():
    req = validate_request()
    com = req['commit']

    query("delete from messages where msgid=%s", (com['msgid']))

    if 'SLEEP' == com['retcode']:
        query("""delete from messages
                 where appid=%s and workername=%s and code='ALARM'
              """,
              (req['appid'], com['workername']))
        query("""insert into messages
                 set workername=%s, appid=%s, pool=%s, state='queued',
                     code=%s, timestamp=now()+interval %s second""",
              (com['workername'], req['appid'], com['pool'],
               'ALARM', com['sleep']))

    if 'DONE' == com['retcode']:
        query("""delete from messages where appid=%s and workername=%s""",
              (req['appid'], com['workername']))
        query("""update workers set output=%s, tmp=null, state='done'
                 where workername=%s
              """,
              (com['output'], com['workername']))
    else:
        query("update workers set output=%s, tmp=%s where workername=%s",
              (com['output'], com['tmp'], com['workername']))

    mark_head(req['appid'], com['workername'])

    return "OK"

@app.route('/lockmessage', methods=['POST'])
@transaction
def lockmessage():
    sql2 = """select msgid, workername, code, data from messages
              where timestamp < now() and state='head' and
                    appid=%s and pool=%s and lock_ip is null
              limit 1
           """
    sql3 = """update messages set lock_ip=%s where msgid=%s"""
    sql4 = """select input, tmp from workers where workername=%s"""

    req = validate_request()

    req['desc']['pools']['default'] = req['desc']['hosts'].keys()

    for pool, ip_list in req['desc']['pools'].iteritems():
        for ip in set(ip_list):
            rows = query(sql2, (req['appid'], pool))
            if len(rows) > 0:
                msgid, workername, code, data = rows[0]
                query(sql3, (req['agent_ip'], msgid))
                input, tmp = query(sql4, (workername))[0]
                return dict(msgid       = msgid,
                            workername  = workername,
                            code        = code,
                            data        = data,
                            pool        = pool,
                            input       = input,
                            tmp         = tmp)
    return "NOT_FOUND"

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=5000)
