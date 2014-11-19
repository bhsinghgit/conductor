import os
import json
import time
import socket
import select
import fcntl

host2sock = dict()
context   = dict()
omsg      = dict()
isock     = set()
osock     = set()

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

def run():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(('', int(conf['notifier_port'])))
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
            pending = api('pending')
        except Exception as e:
            log('Could not access api : {0}'.format(str(e)))
            continue

        for sock in context:
            if (sock in isock) or (sock in osock):
                continue 

            ip = context[sock]['remote'][0]
            if ip not in pending['allocation']:
                continue

            msg = dict()
            for uid in pending['allocation'][ip]:
                if pending['allocation'][ip][uid] < 1:
                    continue

                msg[uid] = dict(
                    async_count=pending['allocation'][ip][uid],
                    authkey=pending['applications'][uid]['authkey'],
                    path=pending['applications'][uid]['path'])

            log('sent to {0} blob({1})'.format(ip, blob(json.dumps(msg))))
            context[sock]['bytes'] = json.dumps(msg)
            osock.add(sock)

        timestamp = time.time()
