import flask
import json
import base64
import functools
import hashlib
import uuid
import time
import MySQLdb

app     = flask.Flask(__name__)
conf    = json.load(open('shepherd.json'))
db_conn = MySQLdb.connect(conf['mysql_host'],
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
    req     = json.loads(flask.request.data)
    appname = flask.request.headers.get('X-SHEPHERD-APPNAME')
    appid   = flask.request.headers.get('X-SHEPHERD-APPID')
    authkey = flask.request.headers.get('X-SHEPHERD-AUTHKEY')

    if not authkey:
        raise Exception('AUTHKEY_MISSING')

    if not (appname or appid):
        raise Exception('BOTH_APPNAME_APPID_MISSING')

    if not appid:
        appid = query("select appid from appname where appname=%s",
                      (appname))[0][0]

    row = query("select authkey, state from apps where appid=%s", (appid))

    if 1 != len(row):
        raise Exception('INVALID_APP')

    if authkey != row[0][0]:
        raise Exception('KEY_MISMATCH')

    if 'active' != row[0][1]:
        raise Exception('INACTIVE_APP')

    if not req:
        req = dict()

    req.update(dict(appid=appid, client_ip=flask.request.remote_addr))

    return req

@app.route('/applications', methods=['GET'])
@transaction
def get_apps():
    sql = "select appid, authkey, path from apps where state='active'"

    ret = dict()
    for appid, authkey, path in query(sql):
        hosts = query("""select ip, count_async, count_sync
                         from hosts where appid=%s
                      """, (appid))
        pools = query("select pool, ip from pools where appid=%s", (appid))

        pool_dict = dict()
        for pool, ip in pools:
            pool_dict.setdefault(pool, list()).append(ip)

        ret[appid] = dict(authkey=authkey,
                          path=path,
                          hosts=dict([(h[0], dict(async=h[1], sync=h[2]))
                                     for h in hosts]),
                          pools=pool_dict)
    return ret

def insert_worker(appid, input, pool, priority):
    query("""insert into workers
             set appid=%s, state='active',
                 input=%s, continuation=%s
          """, (appid, input, base64.b64encode('')))
    workerid = query("select last_insert_id()")[0][0]
    query("""insert into messages
             set workerid=%s, appid=%s, pool=%s, state='head',
                 priority=%s, code='init'
          """, (workerid, appid, pool, priority))

    return workerid

@app.route('/workers', methods=['POST', 'PUT'])
@transaction
def add_worker():
    req = validate_request()

    appid      = req['appid']
    pool       = req.get('pool', 'default')
    priority   = req.get('priority', 128)

    workerid = insert_worker(appid, req['input'], pool, priority)

    if 'PUT' == flask.request.method:
        query("insert into workername set workername=%s, workerid=%s",
              (req['workername'], workerid))
        return dict(workername=req['workername'])

    return dict(workerid=workerid)

@app.route('/workers/<workerid>', methods=['GET'])
@transaction
def get_worker_status(workerid):
    req = validate_request()

    if not workerid.isdigit():
        workerid = query("select workerid from workername where workername=%s",
                         (workerid))[0][0]

    rows = query("select status from workers where workerid=%s and appid=%s",
                 (workerid, req['appid']))

    if 1 != len(rows):
        return dict(status='NOT_FOUND')

    return dict(status=rows[0][0])

def mark_head(appid, workerid):
    msgid = query("""select msgid from messages
                     where appid=%s and workerid=%s
                     order by timestamp, msgid
                     limit 1
                  """,
                  (appid, workerid))
    if len(msgid) > 0:
        query("update messages set state='head' where msgid=%s ", (msgid[0][0]))

@app.route('/messages', methods=['POST'])
@transaction
def add_msg():
    req      = validate_request()
    pool     = req.get('pool', 'default')
    priority = req.get('priority', 128)
    data     = req.get('data', None)
    delay    = req.get('delay', 0)

    if workerid not in req:
        rows = query("""select workerid from workername
                        where workername=%s""", (req['workername']))
        if 1 != len(rows):
            raise Exception('INVALID_WORKERNAME')

        req['workerid'] = rows[0][0]

    rows = query("""select appid from workers
                    where workerid=%s and state != 'done'
                 """, (req['workerid']))
    if 1 != len(rows):
        raise Exception('INVALID_WORKER_STATE')

    appid = rows[0][0]

    query("""insert into messages
             set workerid=%s, appid=%s, pool=%s, state='queued',
                 priority=%s, code=%s, data=%s,
                 timestamp=now()+interval %s second
          """,
          (req['workerid'], appid, pool, priority, req['code'], data, delay))

    mark_head(appid, msg['workerid'])

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

    pool = query("select pool from messages where msgid=%s", (req['msgid']))
    query("delete from messages where msgid=%s", (req['msgid']))

    pool = req.get('pool', pool[0][0])

    if 'state' not in req:
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
                 where workerid=%s
              """,
              (workflow_status, workflow_state, req['workerid']))
        return "OK"

    query("""delete from messages
             where appid=%s and workerid=%s and code='alarm'
          """,
          (req['appid'], req['workerid']))

    def insert_message(appid, workerid, pool, code, data=None):
        query("""insert into messages
                 set workerid=%s, appid=%s,
                 pool=%s, state='queued', code=%s, data=%s
              """, (workerid, appid, pool, code, data))

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
                    insert_message(other_appid, other_workerid,
                                   'default', 'locked')
                    mark_head(other_appid, other_workerid)

    if 'message' in req:
        for msg in req['message']:
            appid = query("select appid from workers where workerid=%s",
                          (req['workerid']))
            if 1 == len(appid):
                if 'workerid' not in msg:
                    msg['workerid'] = query("""select workerid from workername
                                               where workername=%s
                                            """, (msg['workername']))[0][0]

                insert_message(appid[0][0], msg['workerid'],
                    msg.get('pool', 'default'),
                    msg['code'],
                    msg.get('data', None))
                mark_head(appid[0][0], msg['workerid'])

    if 'alarm' in req:
        if int(req['alarm']) < 1:
            req['alarm'] = 0

        query("""insert into messages
                 set workerid=%s, appid=%s, pool=%s, state='queued',
                     code='alarm', timestamp=now()+interval %s second""",
              (req['workerid'], req['appid'], pool, req['alarm']))

    query("update workers set status=%s, continuation=%s where workerid=%s",
          (req['status'], req['continuation'], req['workerid']))

    mark_head(req['appid'], req['workerid'])

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

    for pool, ip_list in pools.iteritems():
        for ip in set(ip_list):
            rows = query(sql1, (req['appid'], pool))
            if len(rows) > 0:
                msgid, workerid, code, data = rows[0]

                query("update messages set lock_ip=%s where msgid=%s",
                      (req['client_ip'], msgid))
                query("""update workers set session=session+1
                         where workerid=%s
                      """, (workerid))

                input, continuation, session = query(
                    """select input, continuation, session from workers
                       where workerid=%s """, (workerid))[0]

                result = dict(msgid      = msgid,
                            workerid     = workerid,
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
