import flask
import json
import base64
import functools
import hashlib
import uuid
import time
import MySQLdb

app       = flask.Flask(__name__)
conf      = json.load(open('shepherd.json'))
db_conn   = MySQLdb.connect(conf['mysql_host'],
                            conf['mysql_user'],
                            conf['mysql_password'],
                            conf['mysql_db'])
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

    row = query("""select appid, authkey, state
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

    req.update(dict(appid=row[0][0], client_ip=flask.request.remote_addr))

    return req

@app.route('/applications', methods=['POST'])
@transaction
def add_app():
    req = json.loads(flask.request.data)
    app = req['app']

    if '127.0.0.1' != flask.request.remote_addr:
        raise Exception('operation not allowed from remote machine')

    query("""insert into applications
             set appname=%s, authkey=%s, state='active', type=%s, path=%s
          """,
          (req['appname'], req['authkey'], app['type'], app['path']))

    return "OK"

@app.route('/applications', methods=['DELETE'])
@transaction
def delete_app():
    req = validate_request()

    if '127.0.0.1' != req['client_ip']:
        raise Exception('operation not allowed from remote machine')

    query("delete from applications where appid=%s", (req['appid']))
    query("delete from hosts        where appid=%s", (req['appid']))
    query("delete from pools        where appid=%s", (req['appid']))
    query("delete from locks        where appid=%s", (req['appid']))
    query("delete from workers      where appid=%s", (req['appid']))
    query("delete from messages     where appid=%s", (req['appid']))

    return "OK"

@app.route('/hosts', methods=['POST'])
@transaction
def add_ip():
    req = validate_request()
  
    query("""insert into hosts set appid=%s, ip=%s, count=%s""",
          (req['appid'], req['client_ip'], req['host']['count']))

    return "OK"

@app.route('/pools', methods=['POST'])
@transaction
def add_pool():
    req = validate_request()
  
    query("""insert into pools set appid=%s, pool=%s, ip=%s""",
          (req['appid'], req['pool']['pool'], req['client_ip']))

    return "OK"

@app.route('/applications', methods=['GET'])
@transaction
def get_apps():
    sql = """select appid, appname, authkey, type, path
             from applications
             where state='active'
          """
    ret = dict()
    for appid, appname, authkey, type, path in query(sql):
        hosts = query("select ip, count from hosts where appid=%s", (appid))
        pools = query("select pool, ip from pools where appid=%s", (appid))

        pool_dict = dict()
        for pool, ip in pools:
            pool_dict.setdefault(pool, list()).append(ip)

        ret[appid] = dict(appname=appname,
                          authkey=authkey,
                          type=type,
                          path=path,
                          hosts=dict([(h[0], h[1]) for h in hosts]),
                          pools=pool_dict)
    return ret

@app.route('/workers', methods=['POST'])
@transaction
def add_worker():
    req    = validate_request()
    worker = req['worker']

    workername = worker.get('workername', guid())
    pool       = worker.get('pool', 'default')
    priority   = worker.get('priority', 128)

    query("""delete from messages where appid=%s and workername=%s""",
          (req['appid'], workername))
    query("""insert into workers
             set workername=%s, appid=%s, state='active',
             input=%s, continuation=%s
          """,
          (workername, req['appid'], worker['input'], base64.b64encode('')))
    query("""insert into messages
             set workername=%s, appid=%s, pool=%s, state='head',
                 priority=%s, code='init'
          """,
          (workername, req['appid'], pool, priority))

    return dict(workername=workername)

@app.route('/worker_status', methods=['POST'])
@transaction
def get_worker_status():
    req = validate_request()

    rows = query("select status from workers where workername=%s",
                 (req['workername']))
    if 1 != len(rows):
        return dict(status='NOT_FOUND')

    return dict(status=rows[0][0])

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

    pool     = msg.get('pool', 'default')
    priority = msg.get('priority', 128)
    data     = msg.get('data', None)
    delay    = msg.get('delay', 0)

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
                 priority=%s, code=%s, data=%s,
                 timestamp=now()+interval %s second
          """,
          (msg['workername'], appid, pool, priority, msg['code'], data, delay))

    mark_head(appid, msg['workername'])

    return "OK"

@app.route('/messages', methods=['GET'])
@transaction
def get_msgs():
    sql = """select appid, pool, count(*) as count
             from messages
             where timestamp < now() and state='head' and lock_ip is null
             group by appid, pool"""
    ret = dict()
    for appid, pool, count in query(sql):
        ret.setdefault(appid, dict())[pool] = count
    return ret

@app.route('/commit', methods=['POST'])
@transaction
def commit():
    req = validate_request()
    com = req['commit']

    pool = query("select pool from messages where msgid=%s", (com['msgid']))
    query("delete from messages where msgid=%s", (com['msgid']))

    pool = com.get('pool', pool[0][0])

    if 'state' not in com:
        if 'exception' in com:
            workflow_status = com['exception']
            workflow_state  = 'exception'
        elif 'status' in com:
            workflow_status = com['status']
            workflow_state  = 'done'
        else:
            workflow_status = 'unknown'
            workflow_state  = 'exception'

        query("""delete from messages where appid=%s and workername=%s""",
              (req['appid'], com['workername']))
        query("""update workers set status=%s, continuation=null, state=%s
                 where workername=%s
              """,
              (workflow_status, workflow_state, com['workername']))
        return "OK"

    query("""delete from messages
             where appid=%s and workername=%s and code='alarm'
          """,
          (req['appid'], com['workername']))

    def insert_message(appid, workername, pool, code, data=None):
        query("""insert into messages
                 set workername=%s, appid=%s,
                 pool=%s, state='queued', code=%s, data=%s
              """, (workername, appid, pool, code, data))

    def get_lock_holder(lockname):
        row = query("""select appid, workername from locks
                       where lockname=%s order by sequence limit 1
                    """, (lockname))
        if len(row) < 1:
            return None, None
        else:
            return row[0][0], row[0][1]

    if 'lock' in com:
        for lockname in set(com['lock']):
            query("insert into locks set lockname=%s, appid=%s, workername=%s",
                 (lockname, req['appid'], com['workername']))

        counter = 0
        for lockname in set(com['lock']):
            row = query("""select workername from locks
                           where lockname=%s order by sequence limit 1
                        """, (lockname))

            if row[0][0] == com['workername']:
                counter += 1

        if len(set(com['lock'])) == counter:
            insert_message(req['appid'], com['workername'], pool, 'locked')

    if 'unlock' in com:
        for lockname in set(com['unlock']):
            query("""delete from locks
                     where lockname=%s and appid=%s and workername=%s
                  """,
                  (lockname, req['appid'], com['workername']))

        for lockname in set(com['unlock']):
            other_appid, other_workername = get_lock_holder(lockname)

            if other_workername:
                locks = query("""select lockname from locks
                                 where appid=%s and workername=%s
                              """, (other_appid, other_workername))

                counter = 0
                for otherlock in locks:
                    tmp_appid, tmp_workername = get_lock_holder(otherlock)
                    if other_workername == tmp_workername:
                        counter += 1

                if len(locks) == counter:
                    insert_message(other_appid, other_workername,
                                   'default', 'locked')
                    mark_head(other_appid, other_workername)

    if 'message' in com:
        for msg in com['message']:
            appid = query("select appid from workers where workername=%s",
                          (msg['workername']))
            if 1 == len(appid):
                insert_message(appid[0][0], msg['workername'],
                    msg.get('pool', 'default'),
                    msg['code'],
                    msg.get('data', None))
                mark_head(appid[0][0], msg['workername'])

    if 'alarm' in com:
        if int(com['alarm']) < 1:
            com['alarm'] = 0

        query("""insert into messages
                 set workername=%s, appid=%s, pool=%s, state='queued',
                     code='alarm', timestamp=now()+interval %s second""",
              (com['workername'], req['appid'], pool, com['alarm']))

    query("update workers set status=%s, continuation=%s where workername=%s",
          (com['status'], com['continuation'], com['workername']))

    mark_head(req['appid'], com['workername'])

    return "OK"

@app.route('/lockmessage', methods=['POST'])
@transaction
def lockmessage():
    sql1 = """select msgid, workername, code, data from messages
              where timestamp < now() and state='head' and
                    appid=%s and pool=%s and lock_ip is null
              order by priority limit 1
           """

    req   = validate_request()
    appid = req['appid']

    hosts = dict([(r[0], r[1]) for r in
                  query("select ip, count from hosts where appid=%s", (appid))])

    pools = dict()
    for pool, ip in query("select pool, ip from pools where appid=%s", (appid)):
        pools.setdefault(pool, list()).append(ip)

    pools['default'] = hosts.keys()

    for pool, ip_list in pools.iteritems():
        for ip in set(ip_list):
            rows = query(sql1, (req['appid'], pool))
            if len(rows) > 0:
                msgid, workername, code, data = rows[0]

                query("update messages set lock_ip=%s where msgid=%s",
                      (req['client_ip'], msgid))
                query("""update workers set session=session+1
                         where workername=%s
                      """, (workername))

                input, continuation, session = query(
                    """select input, continuation, session from workers
                       where workername=%s """, (workername))[0]

                result = dict(msgid      = msgid,
                            workername   = workername,
                            input        = input,
                            session      = session,
                            continuation = continuation,
                            code         = code,
                            pool         = pool)

                if data:
                    result['data'] = data

                return result

    return "NOT_FOUND"

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=5000)
