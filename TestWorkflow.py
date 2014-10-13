import json
import os

def init(input, state):
    return ('ok', 'initialized', dict(seq=0))

def update(input, state, event):
    return ('ok', 'message received', dict(seq=0))

def get_lock(input, state):
    state['seq'] += 1

    index = ('%010d' % (input['worker']))[9]

    return ('lock', 'waiting for lock', state, 'locktest-{0}'.format(index))

def modify_file(input, state):
    state['seq'] += 1

    index = ('%010d' % (input['worker']))[9]

    fd = os.open('/tmp/locktest.' + index, os.O_CREAT|os.O_RDWR)

    raw  = os.read(fd, 1000000)

    if (raw is None) or ('' == raw):
        obj = dict(total=0, workers=list(), guid=input['guid'])
        os.ftruncate(fd, 0)
    else:
        obj = json.loads(raw)
        if ('guid' not in obj) or (input['guid'] != obj['guid']):
            obj = dict(total=0, workers=list(), guid=input['guid'])
            os.ftruncate(fd, 0)

    obj['total'] += 1
    obj['workers'].append(input['worker'])

    os.lseek(fd, 0, os.SEEK_SET)
    os.write(fd, json.dumps(obj, indent=4, sort_keys=True))

    return ('unlock', 'file modified', state, 'locktest-{0}'.format(index))

def loop(input, state):
    state['seq'] += 1

    if state['seq'] < 5:
        return ('retry', 'waiting in the loop', state, 1)

    return ('all done')

workflow = {
    ('init',        'ok'):     'get_lock',
    ('get_lock',    'lock'):   'modify_file',
    ('modify_file', 'unlock'): 'loop'
}
