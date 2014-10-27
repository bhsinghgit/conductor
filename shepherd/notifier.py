import os
import json
import time
import socket
import select
import fcntl
import urllib

host2sock = dict()
context   = dict()
omsg      = dict()
isock     = set()
osock     = set()

def http_get(resource):
    return json.loads(urllib.urlopen( 'http://{0}:{1}/{2}'.format(
        conf['api_host'], conf['api_port'],resource)).read())

def disconnect(sock):
    addr = context[sock]['remote']
    omsg.pop(sock, None)

    host2sock.pop(addr[0])

    if sock in osock:
        osock.remove(sock)

    if sock in isock:
        isock.remove(sock)

    context.pop(sock)
    sock.close()
    log('launcher disconnected from{0}'.format(addr))

def run(timeout):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(('', conf['notifier_port']))
    listener.listen(5)
    listener.setblocking(0)
    log('listening on {0}'.format(listener.getsockname()))

    fcntl.fcntl(listener, fcntl.F_SETFD, fcntl.FD_CLOEXEC)
    isock.add(listener)

    timestamp = 0

    while time.time() < timeout:
        rsock, wsock, esock = select.select(isock, osock, isock.union(osock), 1)

        if len(esock):
            log('exceptions on sockets. investigate')
            exit(0)

        for sock in rsock:
            if sock is listener:
                conn, addr = sock.accept()
                if addr[1] > 999:
                    log('closing connection from{0}'.format(addr))
                    conn.close()
                    continue
    
                fcntl.fcntl(conn, fcntl.F_SETFD, fcntl.FD_CLOEXEC)
                if addr[0] in host2sock:
                    disconnect(host2sock[addr[0]])

                conn.setblocking(0)
                context[conn] = dict(remote=addr)
                host2sock[addr[0]] = conn
                log('launcher connected from{0}'.format(addr))
                continue

            addr = context[sock]['remote']

            try:
                bytes = sock.recv(64*1024)
                if len(bytes) == 0:
                    raise Exception('zero bytes received')
                try:
                    context[sock]['bytes'] += bytes
                    log('from{0} received({1})'.format(addr, len(bytes)))
    
                    context[sock]['msg']    = json.loads(ctx['bytes'])
                    context[sock]['bytes']  = ''
                    isock.remove(sock)
                except ValueError:
                    continue
            except Exception as e:
                log('addr{0} exception: {1}'.format(addr, str(e)))
                disconnect(sock)
                continue
         
        for sock in wsock:
            if sock not in context:
                continue

            ctx = context[sock]
            try:
                m = sock.send(ctx['bytes'])
                ctx['bytes'] = ctx['bytes'][m:]
                n = len(ctx['bytes'])
                log('to{0} sent({1}) remaining({2})'.format(ctx['remote'],m,n))
                if 0 == n:
                    osock.remove(sock)
                    isock.add(sock)
            except socket.error as e:
                log('socket.error: {0}'.format(str(e)))
                disconnect(sock)
                continue

        if time.time() < (timestamp + 1):
            continue

        try:
            apps = http_get('applications')
            msgs = http_get('messages')
        except Exception as e:
            log('Could not access api : {0}'.format(str(e)))
            continue

        allocation = dict()
        for uid, pool_count in msgs.iteritems():
            for pool, count in pool_count.iteritems():
                if uid not in apps:
                    continue

                if 'default' == pool:
                    ip_list = apps[uid]['hosts'].keys()
                else:
                    ip_list = apps[uid]['pools'].get(pool, [])

                ip_list = set(ip_list).intersection(host2sock.keys())

                while count > 0:
                    start_count = count
                    for ip in ip_list:
                        allocation.setdefault(ip, dict()).setdefault(uid, 0)
    
                        if allocation[ip][uid] < apps[uid]['hosts'][ip]:
                            allocation[ip][uid] += 1
                            count -= 1
                            if 0 == count:
                                break
                    if count == start_count:
                        break

        blob(json.dumps(allocation, indent=4))
        for sock in context:
            if (sock in isock) or (sock in osock):
                continue 

            ip = context[sock]['remote'][0]
            if ip not in allocation:
                continue

            request_msg = dict()
            for uid in allocation[ip]:
                if allocation[ip][uid] < 1:
                    continue

                apps[uid]['count']    = allocation[ip][uid]
                apps[uid]['api_host'] = conf['api_host']
                apps[uid]['api_port'] = conf['api_port']
                apps[uid].pop('hosts')
                apps[uid].pop('pools', None)
                request_msg[uid] = apps[uid]

            context[sock]['bytes'] = json.dumps(request_msg)
            osock.add(sock)

        timestamp = time.time()
