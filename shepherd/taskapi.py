import flask
import json
import functools
import hashlib
import uuid
import time
import pymysql

app = flask.Flask(__name__)

db_conn = pymysql.connect('127.0.0.1', 'root', '', 'shepherd')
db_cursor = None

def throw(response_code, error_msg):
    raise Exception((response_code, error_msg))

def transaction(f):
    @functools.wraps(f)
    def f1(*args, **kwargs):
        global db_cursor
        msec = time.time() * 1000

        if db_cursor:
            db_cursor.close()
        db_cursor = db_conn.cursor()

        attempts = 0
        status   = None
        while True:
            attempts += 1
            try:
                response = f(*args, **kwargs)
                db_conn.commit()
                status = 200
            except pymysql.err.InternalError as e:
                import pprint;pprint.pprint(e);
                db_conn.rollback()
                time.sleep(1)
            except Exception as e:
                db_conn.rollback()
                if type(e) is tuple:
                    status   = e[0]
                    response = e[1]
                else:
                    status   = 400
                    response = str(e)

            if status:
                break

        if 200 != status:
            print('attempts({0}) msec({1}) exception{2}'.format(
                    attempts, int(time.time()*1000-msec), response))

        return flask.Response(json.dumps(response, indent=4, sort_keys=True),
                              status,
                              mimetype='application/json')
    return f1

def query(sql, params=None):
    db_cursor.execute(sql, params)
    return db_cursor.fetchall()

def guid():
    return hashlib.md5(str(uuid.uuid4())).hexdigest()

def validate_request(appname=None):
    req     = json.loads(flask.request.data)
    authkey = flask.request.headers.get('X-SHEPHERD-AUTHKEY')

    if appname:
        rows = query("select appid from appnames where appname=%s", (appname))
        if 1 != len(rows):
            throw(404, 'INVALID_APPNAME')

        appid = rows[0][0]
    else:
        appname = flask.request.headers.get('X-SHEPHERD-APPNAME')
        appid   = flask.request.headers.get('X-SHEPHERD-APPID')

    if not authkey:
        throw(401, 'AUTHKEY_MISSING')

    if not (appname or appid):
        throw(400, 'BOTH_APPNAME_APPID_MISSING')

    if not appid:
        appid = query("select appid from appnames where appname=%s",
                      (appname))[0][0]

    row = query("select authkey, state from apps where appid=%s", (appid))

    if 1 != len(row):
        throw(400, 'INVALID_APP')

    if authkey != row[0][0]:
        throw(403, 'KEY_MISMATCH')

    if 'active' != row[0][1]:
        throw(404, 'INACTIVE_APP')

    if not req:
        req = dict()

    req.update(dict(appid=appid, client_ip=flask.request.remote_addr))

    return req

@app.route('/config', methods=['GET'])
@transaction
def get_config():
    return dict([(r[0], r[1]) for r in query("select * from config")])

@app.route('/pending', methods=['GET'])
@transaction
def get_allocation():
    apps = dict([(r[0], dict(authkey=r[1], path=r[2])) for r in  query(
                 "select appid, authkey, path from apps where state='active'")])

    counts = dict()
    for appid in apps.keys():
        counts[appid] = dict([(h[0], h[1]) for h in query("""
            select ip, count_async from hosts where appid=%s""", (appid))])

    msgs = query("""select appid, pool, count(*) as count
                    from messages
                    where timestamp < now() and state='head' and lock_ip is null
                    group by appid, pool""")

    allocation = dict()
    for appid, pool, count in msgs:
        if appid not in apps:
            continue

        if 'default' == pool:
            ip_list = [r[0] for r in query("""select distinct ip
                from hosts where appid=%s""", (appid))]
        else:
            ip_list = [r[0] for r in query("""select distinct ip
                from pools where appid=%s and pool=%s""", (appid, pool))]

        while count > 0:
            start_count = count
            for ip in ip_list:
                allocation.setdefault(ip, dict()).setdefault(appid, 0)

                if allocation[ip][appid] < counts[appid][ip]:
                    allocation[ip][appid] += 1
                    count -= 1
                    if 0 == count:
                        break
            if count == start_count:
                break

    return dict(applications=apps,
                allocation=allocation,
                client_ip=flask.request.remote_addr)

def insert_worker(appid, continuation, pool, priority):
    query("""insert into workers set appid=%s, state='active',
             continuation=%s, status='null'""", (appid, continuation))
    workerid = query("select last_insert_id()")[0][0]
    query("""insert into messages
             set workerid=%s, appid=%s, senderappid=%s, senderworkerid=%s,
                 pool=%s, state='head',
                 priority=%s, code='init'
          """, (workerid, appid, workerid, appid, pool, priority))

    return workerid

def create_worker(req):
    appid    = req['appid']
    pool     = req.get('pool', 'default')
    priority = req.get('priority', 128)

    if 'workflow' in req:
        req['data'] = dict(workflow=req['workflow'], input=req['data'])

    return insert_worker(appid,
                         json.dumps(req['data'], indent=4, sort_keys=True),
                         pool,
                         priority)

@app.route('/workers/<appname>', methods=['POST'])
@transaction
def post_worker(appname):
    return dict(workerid=create_worker(validate_request(appname)))

@app.route('/workers/<appname>/<workername>', methods=['PUT'])
@transaction
def put_worker(appname, workername):
    req = validate_request(appname)

    query("insert into workernames set appid=%s, workername=%s,workerid=%s",
          (req['appid'], workername, create_worker(req)))

    return dict(workername=workername)

@app.route('/workers/<appname>/<workerid>', methods=['GET'])
@transaction
def get_worker_status(appname, workerid):
    req = validate_request(appname)

    if not workerid.isdigit():
        workerid = query("""select workerid from workernames
                            where appid=%s and workername=%s
                         """, (req['appid'], workerid))[0][0]

    rows = query("select status from workers where workerid=%s and appid=%s",
                 (workerid, req['appid']))

    if 1 != len(rows):
        throw(404, 'WORKER_NOT_FOUND')

    return json.loads(rows[0][0])

def mark_head(appid, workerid):
    msgid = query("""select msgid from messages
                     where appid=%s and workerid=%s
                     order by msgid limit 1
                  """,
                  (appid, workerid))
    if len(msgid) > 0:
        query("update messages set state='head' where msgid=%s ", (msgid[0][0]))

@app.route('/messages/<appname>/<workerid>', methods=['POST'])
@transaction
def add_msg(appid, workerid):
    req      = validate_request()
    pool     = req.get('pool', 'default')
    priority = req.get('priority', 128)
    data     = req.get('data', None)
    delay    = req.get('delay', 0)

    if data:
        data = json.dumps(data, indent=4, sort_keys=True)

    rows = query("select appid from appnames where appname=%s", (appname))
    if 1 != len(rows):
        throw(404, 'INVALID_APPNAME')
    appid = rows[0][0]

    if not workerid.isdigit():
        rows = query("""select workerid from workernames
                        where appid=%s and workername=%s
                     """, (appid, workerid))
        if 1 != len(rows):
            throw(404, 'INVALID_WORKERNAME')

        workerid = rows[0][0]

    rows = query("""select appid from workers
                    where workerid=%s and state != 'done'
                 """, (workerid))
    if 1 != len(rows):
        throw(404, 'INVALID_WORKER_STATE')

    query("""insert into messages
             set workerid=%s, appid=%s,
                 senderworkerid=%s, senderappid=%s,
                 pool=%s, state='queued',
                 priority=%s, code=%s, data=%s,
                 timestamp=now()+interval %s second
          """,
          (workerid, appid, 0, req['appid'],
           pool, priority, req['code'], data, delay))

    mark_head(appid, workerid)

    return "OK"

@app.route('/commit', methods=['POST'])
@transaction
def commit():
    req = validate_request()

    pool = query("select pool from messages where msgid=%s", (req['msgid']))
    query("delete from messages where msgid=%s", (req['msgid']))

    pool = req.get('pool', pool[0][0])

    if 'continuation' not in req:
        if 'exception' in req:
            workflow_status = req['exception']
            workflow_state  = 'exception'
        elif 'status' in req:
            workflow_status = req['status']
            workflow_state  = 'done'
        else:
            workflow_status = 'unknown'
            workflow_state  = 'exception'

        query("""delete from messages where appid=%s and workerid=%s""",
              (req['appid'], req['workerid']))
        query("""update workers set status=%s, continuation=null, state=%s
                 where workerid=%s and appid=%s
              """, (json.dumps(workflow_status, indent=4, sort_keys=True),
                    workflow_state,
                    req['workerid'],
                    req['appid']))
        return "OK"

    def insert_message(appid, workerid, pool, code, data=None):
        if data:
            data = json.dumps(data, indent=4, sort_keys=True)

        query("""insert into messages
                 set workerid=%s, appid=%s,
                     senderworkerid=%s, senderappid=%s,
                     pool=%s, state='queued', code=%s, data=%s
              """, (workerid, appid, req['workerid'], req['appid'],
                    pool, code, data))

    def get_lock_holder(lockname):
        row = query("""select appid, workerid from locks
                       where lockname=%s order by sequence limit 1
                    """, (lockname))
        if len(row) < 1:
            return None, None
        else:
            return row[0][0], row[0][1]

    if 'lock' in req:
        for lockname in set(req['lock']):
            query("insert into locks set lockname=%s, appid=%s, workerid=%s",
                 (lockname, req['appid'], req['workerid']))

        counter = 0
        for lockname in set(req['lock']):
            row = query("""select workerid from locks
                           where lockname=%s order by sequence limit 1
                        """, (lockname))

            if row[0][0] == req['workerid']:
                counter += 1

        if len(set(req['lock'])) == counter:
            insert_message(req['appid'], req['workerid'], pool, 'locked')

    if 'unlock' in req:
        for lockname in set(req['unlock']):
            query("""delete from locks
                     where lockname=%s and appid=%s and workerid=%s
                  """,
                  (lockname, req['appid'], req['workerid']))

        to_be_unlocked = set()
        for lockname in set(req['unlock']):
            other_appid, other_workerid = get_lock_holder(lockname)

            if other_workerid:
                locks = query("""select lockname from locks
                                 where appid=%s and workerid=%s
                              """, (other_appid, other_workerid))

                counter = 0
                for otherlock in locks:
                    tmp_appid, tmp_workerid = get_lock_holder(otherlock)
                    if other_workerid == tmp_workerid:
                        counter += 1

                if len(locks) == counter:
                    to_be_unlocked.add((other_appid, other_workerid))

        for app_worker in to_be_unlocked:
            insert_message(app_worker[0], app_worker[1], 'default', 'locked')
            mark_head(app_worker[0], app_worker[1])

    if 'message' in req:
        for msg in req['message']:
            if 'appid' not in msg:
                msg['appid'] = query("""select appid from appnames
                    where appname=%s""", (msg['appname']))[0][0]

            if 'workerid' not in msg:
                msg['workerid'] = query("""select workerid
                    from workernames where appid=%s and workername=%s""",
                    (msg['appid'], msg['workername']))[0][0]

            insert_message(msg['appid'], msg['workerid'],
                msg.get('pool', 'default'),
                msg['code'],
                msg.get('data', None))
            mark_head(msg['appid'], msg['workerid'])

    if 'alarm' in req:
        if int(req['alarm']) < 1:
            req['alarm'] = 0

        query("""delete from messages
                 where appid=%s and workerid=%s and code='alarm'
              """, (req['appid'], req['workerid']))

        query("""insert into messages
                 set workerid=%s, appid=%s, senderworkerid=%s, senderappid=%s,
                     pool=%s, state='queued',
                     code='alarm', timestamp=now()+interval %s second""",
              (req['workerid'], req['appid'], req['workerid'], req['appid'],
               pool, req['alarm']))

    mark_head(req['appid'], req['workerid'])

    query("update workers set status=%s, continuation=%s where workerid=%s",
          (json.dumps(req['status'], indent=4, sort_keys=True),
           json.dumps(req['continuation'], indent=4, sort_keys=True),
           req['workerid']))

    return "OK"

@app.route('/lockmessage', methods=['POST'])
@transaction
def lockmessage():
    sql1 = """select msgid, workerid, code, data from messages
              where timestamp < now() and state='head' and
                    appid=%s and pool=%s and lock_ip is null
              order by priority limit 1
           """

    req   = validate_request()
    appid = req['appid']

    hosts = dict([(r[0], r[1]) for r in
                  query("select ip, count_async from hosts where appid=%s",
                        (appid))])

    pools = dict()
    for pool, ip in query("select pool, ip from pools where appid=%s", (appid)):
        pools.setdefault(pool, list()).append(ip)

    pools['default'] = hosts.keys()

    for pool in sorted(pools.keys()):
        for ip in sorted(set(pools[pool])):
            rows = query(sql1, (req['appid'], pool))
            if len(rows) > 0:
                msgid, workerid, code, data = rows[0]

                query("update messages set lock_ip=%s where msgid=%s",
                      (req['client_ip'], msgid))
                query("""update workers set session=session+1
                         where workerid=%s
                      """, (workerid))

                continuation, session = query("""select continuation, session
                    from workers where workerid=%s """, (workerid))[0]

                result = dict(msgid        = msgid,
                              workerid     = workerid,
                              session      = session,
                              continuation = json.loads(continuation),
                              code         = code,
                              pool         = pool)

                if data:
                    result['data'] = json.loads(data)

                return result

    return "NOT_FOUND"

if __name__ == '__main__':
    app.debug = True
    print('Starting.....')
    app.run(host='0.0.0.0', port=6000)
