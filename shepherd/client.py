import json
import base64
import httplib

conf = json.loads(open('shepherd.json').read())
conn = httplib.HTTPConnection(conf['api_host'], conf['api_port'])

def http_client(method, resource, args):
    conn.request(method, resource, json.dumps(args))
    response = conn.getresponse()

    if 200 == response.status:
        return json.loads(response.read())
    else:
        print(json.dumps(args, indent=4, sort_keys=True))
        print('{0} {1} {2}'.format(resource, response.status, response.reason))
        print(response.read())
        exit(1)

def http_post(resource, args):
    return http_client('POST', resource, args)

def http_delete(resource, args):
    return http_client('DELETE', resource, args)

def print_result(result):
    print(json.dumps(result, indent=4, sort_keys=True))
    exit(0)

def add_app(type, path):
    conf['app'] = dict(type=type, path=path)
    return http_post('applications', conf)

def del_app():
    return http_delete('applications', conf)

def add_host(count):
    conf['host'] = dict(count=count)
    return http_post('hosts', conf)

def add_pool(pool):
    conf['pool'] = dict(pool=pool)
    return http_post('pools', conf)

def add_worker(input, workername=None):
    conf['worker'] = dict(input=base64.b64encode(input))
    if workername:
        conf['worker']['workername'] = workername

    return http_post('workers', conf)

def send_msg(workername, code, data=None, pool=None, delay=None):
    conf['msg'] = dict(workername=workername, code=code)
    if pool:
        conf['msg']['pool'] = opt.pool
    if data:
        conf['msg']['data'] = base64.b64encode(data)
    if delay:
        conf['msg']['delay'] = int(opt.delay)

    return http_post('messages', conf)
