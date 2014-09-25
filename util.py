import os
import time
import fcntl
import json

def log(msg):
    utc  = time.time()
    strf = time.strftime("%y%m%d.%H%M%S", time.gmtime(utc))
    usec = int((utc - int(utc)) * 1000000)
    hdr  = "%s.%06d.%05d : " % (strf, usec, os.getpid())
    os.write(3, ''.join([hdr, msg,'\n']))

def initialize(id):
    map(os.close, range(3))

    os.umask(0)
    os.open('/dev/zero', os.O_RDONLY)

    seq = time.strftime('%y%m%d%H', time.gmtime((time.time()//(6*3600))*6*3600))
    for path in ['stdout', 'stderr', 'stdlog']:
        os.open('{0}.{1}.{2}'.format(id, path, seq),
                os.O_CREAT|os.O_WRONLY|os.O_APPEND,
                0644)

    if id.isdigit():
        return log
    else:
        lock = os.open('{0}.lock'.format(id), os.O_CREAT|os.O_RDONLY, 0444)
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        with open('{0}.json'.format(id)) as fd:
            return log, json.load(fd), lock

def remove_old_logs():
    seq = time.strftime('%y%m%d%H', time.gmtime((time.time()//(6*3600))*6*3600))
    for path in os.listdir('.'):
        fields = path.split('.')
        if (3 == len(fields)) and (fields[1] in ['stdout', 'stderr', 'stdlog']):
            if int(fields[2]) < (int(seq)-12):
               try: 
                   os.remove(path)
               except:
                   pass

def release_lock(fd):
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)
