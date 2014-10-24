import os
import sys
import json
import socket
import signal

def run():
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((conf['notifier_host'], conf['notifier_port']))

    while True:
        bytes = ''
        while True:
            b = sock.recv(64*1024)
            if 0 == len(b):
                log('disconnected by notifier')
                exit(0)
            bytes += b

            try:
                msg = json.loads(bytes)
                log('received message bytes({0})'.format(len(bytes)))
                break
            except ValueError:
                continue

        proc_count = dict()
        for d in [d for d in os.listdir('/proc') if d.isdigit()]:
            try:
                uid = os.stat('/proc/{0}'.format(d)).st_uid
            except OSError:
                pass

            proc_count.setdefault(uid, 0)
            proc_count[uid] += 1

        blob(json.dumps(msg, indent=4))
        for uid, app in msg.iteritems():
            fd = os.open(uid + '.key', os.O_CREAT|os.O_WRONLY, 0600)
            os.write(fd, json.dumps(dict(appname=app['appname'],
                                         authkey=app['authkey'],
                                         api_host=app['api_host'],
                                         api_port=app['api_port']),
                                         indent=4, sort_keys=True))
            os.close(fd)
            os.chown('{0}.key'.format(uid), int(uid), int(uid))

            count = 0
            for i in range(app['count'] - proc_count.get(int(uid), 0)):
                if 0 == os.fork():
                    sock.close()
                    os.close(4)

                    os.setsid()
                    os.setgid(int(uid))
                    os.setuid(int(uid))

                    if 'worker' == app['type']:
                        command   = os.path.join(app['path'])
                        dirname   = os.path.dirname(sys.argv[0])
                        worker    = os.path.join(dirname, 'worker')
                        arguments = [command, worker]
                    elif 'rpc' == app['type']:
                        pass

                    os.execv(command, arguments)
                else:
                    count += 1
            log('spawned count({0}) procs for uid({1})'.format(count, uid))

        bytes = json.dumps('OK')
        sock.sendall(bytes)
        log('sent message bytes({0})'.format(len(bytes)))