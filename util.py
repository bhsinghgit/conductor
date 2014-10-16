import os
import time
import fcntl
import json
import base64
import hashlib

def log_writer(worker_id, ip, tag, msg):
    utc  = time.time()

    os.write(3, '\n[{0}.{1} {2} {3} {4} {5}] {6}'.format(
        time.strftime("%y%m%d.%H%M%S", time.gmtime(utc)),
        int((utc - int(utc)) * 1000000),
        os.getpid(),
        ip,
        worker_id,
        tag,
        msg
    ))

def log(worker_id, ip, tag, msg):
    index = msg.find('\n')
    if index > -1:
        log_writer(worker_id, ip, tag, msg[:index])
    else:
        log_writer(worker_id, ip, tag, msg)

def blob(worker_id, ip, msg):
    b64enc  = base64.b64encode(msg)
    md5hash = hashlib.md5(msg).hexdigest()

    log_writer(worker_id, ip, 'blob.{0}.{1}'.format(len(b64enc),md5hash),b64enc)
    return md5hash

def initialize(id):
    map(os.close, range(3))

    os.umask(0)
    os.open('/dev/null', os.O_RDONLY)

    seq = time.strftime('%y%m%d%H', time.gmtime((time.time()//(6*3600))*6*3600))
    for path in ['stdout', 'stderr']:
        os.open('{0}.{1}.{2}'.format(id, path, seq),
                os.O_CREAT|os.O_WRONLY|os.O_APPEND,
                0644)

    os.open('log.{0}'.format(seq), os.O_CREAT|os.O_WRONLY|os.O_APPEND, 0644)

    lock = os.open('{0}.lock'.format(id), os.O_CREAT|os.O_RDONLY, 0444)
    fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
    with open('conductor.json'.format(id)) as fd:
        return json.load(fd), lock

def remove_old_logs():
    seq = time.strftime('%y%m%d%H', time.gmtime((time.time()//(6*3600))*6*3600))
    for path in os.listdir('.'):
        fields = path.split('.')
        if (3 == len(fields)) and (fields[1] in ['stdout', 'stderr']):
            if int(fields[2]) < (int(seq)-12):
               try: 
                   os.remove(path)
               except:
                   pass

def release_lock(fd):
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)
