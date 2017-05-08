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


def worker(sock):
    for i in range(100):
        sock.sendall(pprint.pformat(os.environ))
        time.sleep(1)


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
        ready, _, _ = select.select([sock], [], [], 0.01)
        if ready:
            s, addr = ready[0].accept()
            log('received connection from %s', addr)

            pid = os.fork()
            if pid:
                workers[pid] = time.time()
                s.close()
            else:
                log('worker launched for %s', addr)
                sock.close()
                worker(s)
                log('worker exited for %s', addr)
                os._exit(0)
        else:
            if workers:
                pid, status, usage = os.wait3(os.WNOHANG)
                if pid:
                    workers.pop(pid)
                    log('worker pid(%d) reaped', pid)

            for pid in workers:
                if time.time() > workers[pid] + 60:
                    log('worker pid(%d) killed due to timeout', pid)
                    os.kill(pid, signal.SIGKILL)

        

if '__main__' == __name__:
    main(int(sys.argv[1]))
