import os
import time
import socket
import select
import signal
import fcntl
import re

regex = re.compile('^\[(.+) (\d+) (\d+) (\d+) (\d{6}\.\d{6}\.\d{6}) (\.+)\] : ')

def run():
    global timeout
    allowed  = timeout - time.time()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(('', int(conf['collector_port'])))
    listener.listen(5)
    fcntl.fcntl(listener, fcntl.F_SETFD, fcntl.FD_CLOEXEC)
    log('listening on {0}'.format(listener.getsockname()))

    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    while time.time() < timeout:
        r, w, e = select.select([listener], [], [], timeout - time.time())
        if 0 == len(r):
            log('collector listener timedout') 
            return

        sock, addr = listener.accept()

        if os.fork() != 0:
            sock.close()
            continue

        os.close(4)
        listener.close()
        os.setsid()

        timeout = time.time() + allowed

        log('received connection from {0}'.format(addr))

        dirname = os.path.join('logs', addr[0])
        if not os.path.exists(dirname):
            os.makedirs(dirname, 0755)

        filename = sock.recv(12)
        if len(filename) < 12:
            log('invalid filename length({0})'.format(len(filename)))
            exit(0)

        filepath = os.path.join(dirname, filename)
        log('connection received for filename({0})'.format(filename))

        wfd = os.open(filepath, os.O_CREAT|os.O_RDWR|os.O_APPEND, 0644)
        fcntl.flock(wfd, fcntl.LOCK_EX|fcntl.LOCK_NB)
        filesize = os.lseek(wfd, 0, os.SEEK_END)

        log('filename({0}) filesize({1})'.format(filename, filesize))
        sock.sendall('%012d' % (filesize))

        rfd = open(filepath, 'r')
        rfd.seek(0, 2)

        tmp = os.open('xyz', os.O_CREAT|os.O_RDWR|os.O_APPEND, 0644)
        while time.time() < timeout:
            buffer = sock.recv(1024*1024*1024)
            if len(buffer) < 1:
                exit(0)

            os.write(wfd, buffer)

            for line in rfd:
                result = regex.match(line)
                if result:
                    log('match.......')
                    os.write(tmp, '{0}\n'.format(result.group(0)))
                else:
                    log('no match....')

        os.close(wfd)
        close(rfd)
        log('timedout for file({0})'.format(filename))
