import os
import time
import socket
import fcntl

def ymdH(subtract_hour=0):
    now = int(time.time()) - (subtract_hour*3600)
    return int(time.strftime('%y%m%d%H',time.gmtime((now//(6*3600))*6*3600)))

def run(timeout):
    for path in os.listdir('.'):
        fields = path.split('.')
        if (len(fields) != 2) or ('log' != fields[0]):
            continue

        file_seq = int(fields[1])
        if file_seq < ymdH(6):
            os.remove(path)
            log('removed log file({0})'.format(path))
        elif os.fork() == 0:
            os.close(4)
        
            try:
                fd = os.open(path, os.O_RDONLY)
                fcntl.flock(fd, fcntl.LOCK_EX|fcntl.LOCK_NB)
            except:
                log('file({0}) is locked'.format(path))
                exit(0)

            while time.time() < timeout:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.bind(('', conf['shipper_port']))
                    sock.connect((conf['collector_host'],
                                  conf['collector_port']))
                    log('connected to collector {0}'.format(sock.getpeername()))
                    break
                except:
                    time.sleep(5)

            sock.sendall('%s' % (path))
            reply = sock.recv(12)

            if (len(reply) < 12) or (int(reply) < 0):
                exit(0)

            log('filename({0}) size({1})'.format(path, int(reply)))
            os.lseek(fd, int(reply), os.SEEK_SET)

            while time.time() < timeout:
                buffer = os.read(fd, 10*1024*1024)

                if len(buffer) > 0:
                    sock.sendall(buffer)
                else:
                    time.sleep(1)
                    if file_seq < (ymdH(12)):
                        log('stop shipping file({0})'.format(path))
                        break

            log('timedout for file({0})'.format(path))
    exit(0)
