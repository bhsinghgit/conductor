import os
import time
import base64

class Logger(object):
    def __init__(self, thread, session=None):
        if not session:
            session = int(time.time())

        self.session  = "%s.%010d.%06d" % (thread, session, os.getpid())
        self.sequence = 0

    def append(self, tag, msg):
        self.sequence += 1

        utc = time.time()

        os.write(3, '\n{0}.{1}.{2}.{3}.{4} {5}'.format(
            self.session,
            '%010d' % (self.sequence),
            time.strftime("%y%m%d.%H%M%S", time.gmtime(utc)),
            '%06d' % (int((utc - int(utc)) * 1000000)),
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
