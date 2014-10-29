import os
import time
import socket
import select
import signal
import fcntl
import re

pattern = '^([-\w]+)\.(\d+)\.(\d+)\.(\d+)\.(\d+)\.(\d+)\.(\d+).([-\w]+) '
regex = re.compile(pattern)

filedict = dict()

def append(session, line):
    def open_file(filename):
        filedict[filename] = os.open(filename,
                                     os.O_CREAT|os.O_WRONLY|os.O_APPEND,
                                     0644)

    filename = os.path.join('logs', 'sessions', session)

    if filename not in filedict:
        try: 
            open_file(filename)
        except:
            for fd in filedict.values():
                os.close(fd)
            open_file(filename)

    os.write(filedict[filename], '\n' + line)

def run(timeout):
    allowed  = timeout - time.time()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(('', conf['collector_port']))
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

        dirname = os.path.join('logs', 'raw', addr[0])
        if not os.path.exists(dirname):
            os.makedirs(dirname, 0755)

        filename = sock.recv(12)
        if len(filename) < 12:
            log('invalid filename length({0})'.format(len(filename)))
            exit(0)

        filepath = os.path.join(dirname, filename)
        log('connection received for filename({0})'.format(filename))

        fd = os.open(filepath, os.O_CREAT|os.O_RDWR|os.O_APPEND, 0644)

        filesize = os.lseek(fd, 0, os.SEEK_END)
        origsize = filesize
        while filesize > 0:
             filesize = max(0, filesize - 1024*1024)
             os.lseek(fd, filesize, os.SEEK_SET)
             index = os.read(fd, 1024*1024).find('\n')
             if index >= 0:
                 filesize += index + 1
                 break
        os.ftruncate(fd, filesize)
        os.fsync(fd)
        fcntl.flock(fd, fcntl.LOCK_EX|fcntl.LOCK_NB)

        log('filename({0}) original_size({1}) after_truncation({1})'.format(
            filename, origsize, filesize))

        sock.sendall('%012d' % (filesize))

        while time.time() < timeout:
            buffer = sock.recv(1024*1024*1024)
            if len(buffer) < 1:
                exit(0)

            for line in buffer.split('\n'):
                if len(line) == 0:
                    continue

                m = regex.match(line)
                if m:
                    if not m.group(8).isdigit():
                        append(m.group(1) + '.' + filename, line)

            os.write(fd, buffer)

        log('timedout for file({0})'.format(filename))
        exit(0)
