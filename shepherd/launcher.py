import os
import sys
import json
import time
import socket
import signal

def run(timeout):
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    while time.time() < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('', conf['launcher_port']))
            sock.connect((conf['notifier_host'], conf['notifier_port']))
            break
        except:
            time.sleep(5)

    while time.time() < timeout:
        bytes = ''
        while time.time() < timeout:
            b = sock.recv(64*1024)
            if 0 == len(b):
                log('disconnected by notifier')
                return
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
            os.environ['AUTHKEY'] = app['authkey']
            os.environ['APPID']   = uid
            os.environ['APIHOST'] = app['api_host']
            os.environ['APIPORT'] = str(app['api_port'])

            count = 0
            for i in range(app['async_count'] - proc_count.get(int(uid), 0)):
                if 0 == os.fork():
                    sock.close()
                    os.close(4)

                    os.setsid()
                    os.setgid(int(uid))
                    os.setuid(int(uid))

                    command = os.path.join(app['path'])
                    dirname = os.path.dirname(sys.argv[0])
                    worker  = os.path.join(dirname, 'sheep')

                    os.execv(command, [command, worker])
                else:
                    count += 1
            log('spawned count({0}) procs for uid({1})'.format(count, uid))

        bytes = json.dumps('OK')
        sock.sendall(bytes)
        log('sent message bytes({0})'.format(len(bytes)))
