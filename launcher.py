import os
import ssl
import sys
import time
import uuid
import json
import pprint
import signal
import socket
import select
import logging


logging.basicConfig(format='%(asctime)s %(process)s : %(message)s')
log = logging.critical


def listen(port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('0.0.0.0', port))
        sock.listen(5)
        log('listening on port(%d)', port)
    except:
        log('already running on port(%d)', port)
        os._exit(0)

    if not os.path.isdir('logs'):
        os.mkdir('logs')

    total = 0
    while True:
        if select.select([sock], [], [], 1)[0]:
            s, addr = sock.accept()

            pid = os.fork()
            if not pid:
                env = dict(UUID=uuid.uuid4().hex)
                env.update(json.loads(s.makefile().readline()))
                env.update(dict(
                    USER='someuser',
                    LOGNAME='someuser',
                    USERNAME='someuser',
                    HOME='/tmp'))

                os.dup2(os.open(os.path.join('logs', env['UUID']),
                                os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                                0600), 2)
                os.write(2, 'uuid(%s) pid(%s) addr%s\n' % (
                            env['UUID'], os.getpid(), addr))

                os.dup2(s.fileno(), 0)
                os.dup2(s.fileno(), 1)
                os.closerange(3, 1024)

                args = ['/usr/bin/python',
                        os.path.join(os.getcwd(), env['APP'])]
                os.execve(args[0], args, env)

            s.close()
            total += 1
            log('launched(%d) addr%s total(%d)', pid, addr, total)

        while total:
            pid, status, usage = os.wait3(os.WNOHANG)
            if not pid:
                break

            total -= 1
            log('reaped(%d) total(%d)', pid, total)


if '__main__' == __name__:
    if 'listen' == sys.argv[1]:
        listen(int(sys.argv[2]))
