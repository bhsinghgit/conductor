import os
import sys
import json
import time
import socket
import signal
import fcntl

def launch(app_dict, sock=None):
    proc_count = dict()
    for d in [d for d in os.listdir('/proc') if d.isdigit()]:
        try:
            uid = os.stat('/proc/{0}'.format(d)).st_uid
        except OSError:
            pass

        proc_count.setdefault(uid, 0)
        proc_count[uid] += 1

    for uid, app in app_dict.iteritems():
        os.environ['AUTHKEY'] = app['authkey']
        os.environ['APPID']   = uid
        os.environ['APIHOST'] = sys.argv[2]
        os.environ['APIPORT'] = sys.argv[3]

        if not os.path.isdir(uid):
            os.mkdir(uid, 0700)
            os.chown(uid, int(uid), int(uid))

        count = 0
        for i in range(app['async_count'] - proc_count.get(int(uid), 0)):
            if 0 == os.fork():
                if sock:
                    sock.close()

                os.close(4)

                os.setsid()
                os.setgid(int(uid))
                os.setuid(int(uid))

                command = os.path.join(app['path'])
                dirname = os.path.dirname(sys.argv[0])
                worker  = os.path.join(dirname, 'sheep')

                os.chdir(uid)
                signal.signal(signal.SIGCHLD, signal.SIG_DFL)

                os.execv(command, [command, worker])
            else:
                count += 1
        log('spawned count({0}) procs for uid({1})'.format(count, uid))

def poller():
    while time.time() < timeout:
        try:
            pending = api('pending')
        except Exception as e:
            log('Could not access api : {0}'.format(str(e)))

        myip = pending['client_ip']
        if myip in pending['allocation']:
            app_dict = dict()
            for uid, count in pending['allocation'][myip].iteritems():
                app_dict[uid] = dict(
                    async_count=count,
                    authkey=pending['applications'][uid]['authkey'],
                    path=pending['applications'][uid]['path'])

            launch(app_dict)

        time.sleep(30)

def agent():
    while time.time() < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('', int(conf['notifier_port'])))
            sock.connect((conf['notifier_host'], int(conf['notifier_port'])))
            break
        except Exception as e:
            log('could not connect to notifier. exception{0}'.format(str(e)))
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

        launch(msg, sock)

        bytes = json.dumps('OK')
        sock.sendall(bytes)
        log('sent message bytes({0})'.format(len(bytes)))

def run():
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    if ('notifier_host' in conf) and ('notifier_port' in conf):
        agent()
    else:
        poller()
