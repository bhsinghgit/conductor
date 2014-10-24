import json
import os
import random

def modify(guid, worker, filename, key):
    obj = dict(guid=guid, total=0, workers=[], extraTotal=0, extraWorkers=[])

    fd = os.open(filename, os.O_CREAT|os.O_RDWR)

    raw  = os.read(fd, 1000000)

    if (raw is None) or ('' == raw):
        os.ftruncate(fd, 0)
    else:
        tmp = json.loads(raw)
        if ('guid' not in tmp) or (guid != tmp['guid']):
            os.ftruncate(fd, 0)
        else:
            obj = tmp

    if 'main' == key:
        obj['total'] += 1
        obj['workers'].append(worker)
    else:
        obj['extraTotal'] += 1
        obj['extraWorkers'].append(worker)

    os.lseek(fd, 0, os.SEEK_SET)
    os.write(fd, json.dumps(obj, indent=4, sort_keys=True))

def init(input, state, util):
    index = ('%010d' % (input['worker']))[9]

    state = dict(seq=0, index=str(index), random=list())

    num = set()
    while True:
        num.add(random.randrange(input['count']))
        if 5 == len(num):
            break

    for i in num:
        state['random'].append(str(i))

    return ('ok', 'initialized', state)

def handler(input, state, event, util):
    return ('ok', 'got signal', state)

def get_lock(input, state, util):
    state['seq'] += 1

    locks = list(state['index'])
    locks.extend(state['random'])

    return ('lock', 'waiting for lock', state,
            ['locktest-{0}'.format(l) for l in locks])

def locktest(input, state, util):
    state['seq'] += 1

    modify(input['guid'], input['worker'],
           '/tmp/locktest.' + state['index'], 'main')

    locks = list(state['index'])
    locks.extend(state['random'])

    for l in state['random']:
        modify(input['guid'], input['worker'], '/tmp/locktest.' + l, '')

    return ('unlock', 'file modified', state,
            ['locktest-{0}'.format(l) for l in locks])

def loop(input, state, util):
    state['seq'] += 1

    if state['seq'] < 5:
        return ('retry', 'waiting in the loop', state, 1)

    message = dict(workername='sheepdog', code='inform')
    return ('message', 'informing sheepdog', state, [message])

def send_file(input, state, util):
    state['seq'] += 1

    filename = '/tmp/locktest.' + str(input['worker'])

    if os.path.isfile(filename):
        message = dict(workername='sheepdog',
                       code='file',
                       data=json.loads(open(filename).read()))
    else:
        message = dict(workername='sheepdog',
                       code='file',
                       data=dict(guid=input['guid'], total=0,
                                  workers=[], extraTotal=0, extraWorkers=[]))

    return ('message', 'sending message', state, [message])

def done(input, state, util):
    return ('ok', 'done')

workflow = {
    ('init',      'ok'):      'get_lock',
    ('get_lock',  'lock'):    'locktest',
    ('locktest',  'unlock'):  'loop',
    ('handler',   'ok'):      'send_file',
    ('send_file', 'message'): 'done'
}
