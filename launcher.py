import os
import ssl
import sys
import time
import pprint
import signal
import socket
import select
import logging


logging.basicConfig(format='%(asctime)s %(process)s : %(message)s')
log = logging.critical


def worker():
    for i in range(10000000):
        print('output : {0}'.format(i))


def main(port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('0.0.0.0', port))
        sock.listen(5)
        log('listening on port(%d)', port)
    except:
        log('already running on port(%d)', port)
        os._exit(0)


    workers = dict()
    while True:
        ready, _, _ = select.select([sock] + workers.keys(), [], [], 1)

        for rdy in ready:
            if rdy == sock:
                s, addr = sock.accept()
                log('received connection from %s', addr)

                pipe_r, pipe_w = os.pipe()

                pid = os.fork()
                if pid:
                    workers[pipe_r] = (time.time(), pid)
                    s.close()
                    os.close(pipe_w)
                else:
                    log('worker launched for %s', addr)

                    os.dup2(s.fileno(), 0)
                    os.dup2(s.fileno(), 1)
                    os.dup2(pipe_w, 2)

                    for i in range(3, 1024):
                        try:
                            os.close(i)
                        except:
                            pass

                    worker()

                    log('worker exited for %s', addr)
                    os._exit(0)
            else:
                data = os.read(rdy, 4096)
                if data:
                    print(data)
                else:
                    ts, pid = workers.pop(rdy)
                    pid2, status, rusage = os.wait4(pid, os.WNOHANG)
                    assert(pid == pid2)

                    os.close(rdy)
                    log('worker pid(%d) reaped', pid)

        for w in workers:
            if time.time() > workers[w][0] + 60:
                os.kill(workers[w][1], signal.SIGKILL)
                log('worker pid(%d) killed due to timeout', workers[w][1])


if '__main__' == __name__:
    main(int(sys.argv[1]))
