import os
import ssl
import sys
import time
import uuid
import json
import socket
import signal
import select
import logging

signal.signal(signal.SIGCHLD, lambda signum, frame: None)

logging.basicConfig(format='%(asctime)s %(process)s : %(message)s')
log = logging.critical

port = int(sys.argv[1])
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(('0.0.0.0', port))
sock.listen(128)
log('listening on port(%d)', port)

if not os.path.isdir('logs'):
    os.mkdir('logs')

total = 0
while True:
    try:
        select.select([sock] if total < int(sys.argv[2]) else [], [], [])

        if not os.fork():
            s, addr = sock.accept()
            log('forked(%d) addr%s total(%d)', os.getpid(), addr, total+1)

            env = dict(UUID=uuid.uuid4().hex)
            env.update(json.loads(s.makefile().readline()))

            log('pid(%d) uuid(%s)', os.getpid(), env['UUID'])

            env.update(dict(
                UUID=env['UUID'],
                USER='U{0}'.format(os.getuid()),
                HOME='/tmp'))

            os.dup2(s.fileno(), 0)
            os.dup2(s.fileno(), 1)
            os.dup2(os.open(os.path.join('logs', env['UUID']),
                            os.O_CREAT | os.O_WRONLY | os.O_APPEND,
                            0600), 2)
            os.closerange(3, 128)

            args = ['/usr/bin/python',
                    os.path.join(os.getcwd(), env['APP'])]
            os.execve(args[0], args, env)

        total += 1

    except select.error:
        while total:
            pid, status, usage = os.wait3(os.WNOHANG)
            if not pid:
                break

            total -= 1
            log('reaped(%d) total(%d)', pid, total)
