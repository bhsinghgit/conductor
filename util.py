import os
import time
import fcntl
import json
import base64
import hashlib

class Logger(object):
    def __init__(self, thread, session=None):
        if not session:
            session = int(time.time())

        self.session  = "%s.%010d.%06d" % (thread, session, os.getpid())
        self.sequence = 0

    def append(self, tag, msg):
        self.sequence += 1

        utc = time.time()

        os.write(3, '\n{0}.{1}.{2}.{3} {4} : {5}'.format(
            self.session,
            "%010d" % (self.sequence),
            time.strftime("%y%m%d.%H%M%S", time.gmtime(utc)),
            int((utc - int(utc)) * 1000000),
            tag,
            msg
        ))

    def log(self, tag, msg=None):
        if msg is None:
            msg = tag
            tag = '-'

        index = msg.find('\n')
        if index > -1:
            self.append(tag, msg[:index])
        else:
            self.append(tag, msg)

    def blob(self, msg):
        b64enc = base64.b64encode(msg)

        self.append(len(b64enc), b64enc)

        return self.sequence

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
        return json.load(fd), lock, Logger(id)

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
