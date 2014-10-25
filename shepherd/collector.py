import os
import time
import socket
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
    log('listening on {0}'.format(listener.getsockname()))

    while time.time() < timeout:
        sock, addr = listener.accept()
        pid = os.fork()
        if pid > 0:
            sock.close()
        elif 0 == pid:
            if os.fork() != 0:
                exit(0)

            timeout = time.time() + allowed

            os.close(4)
            listener.close()
            os.setsid()

            log('received connection from {0}'.format(addr))

            dirname = os.path.join('logs', 'raw', addr[0])
            if not os.path.exists(dirname):
                os.makedirs(dirname, 0755)

            filename = sock.recv(12)
            if len(filename) < 12:
                log('exiting as filename length({0})'.format(len(filename)))
                exit(0)

            filepath = os.path.join(dirname, filename)
            log('connection received for filename({0})'.format(filename))

            fd = os.open(filepath, os.O_CREAT|os.O_WRONLY|os.O_APPEND, 0644)
            fcntl.flock(fd, fcntl.LOCK_EX|fcntl.LOCK_NB)
            os.fsync(fd)

            filesize = os.stat(filepath).st_size
            log('filename({0}) size({1})'.format(filename, filesize))

            sock.sendall('%010d' % (filesize))

            session = 'orphan'
            while time.time() < timeout:
                buffer = sock.recv(1024*1024*1024)
                if len(buffer) < 1:
                    exit(0)

                for line in buffer.split('\n'):
                    if len(line) == 0:
                        continue

                    m = regex.match(line)
                    if m:
                        if m.group(8).isdigit():
                            continue

                        session = '.'.join([m.group(1), filename])

                    append(session, line)

                os.write(fd, buffer)

    listener.close()
