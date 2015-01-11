import json
import os
import random

log = shepherd.log
blob = shepherd.blob

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

def init(input, state):
    log('Starting sheep with input<{0}>'.format(blob(input)))

    index = ('%010d' % (input['worker']))[9]

    state = dict(seq=0, index=str(index), random=list())

    num = set()
    while True:
        num.add(random.randrange(input['count']))
        if 5 == len(num):
            break

    for i in num:
        state['random'].append(str(i))

    log('Moving to centralbox')
    return ('moveto', 'initialized', state, 'centralbox')

def handler(input, state, event):
    log('Handler invokded with input<{0}>, state<{1}> and event<{2}>'.format(
        blob(input), blob(state), blob(event)))
    return ('ok', 'got signal', state)

def get_lock(input, state):
    state['seq'] += 1

    locks = list(state['index'])
    locks.extend(state['random'])

    locks = ['locktest-{0}'.format(l) for l in locks]

    log('Trying to acquire locks<{0}>'.format(blob(locks)))
    return ('lock', 'waiting for lock', state, locks)

def locktest(input, state):
    state['seq'] += 1

    modify(input['guid'], input['worker'], 'locktest.' + state['index'], 'main')

    locks = list(state['index'])
    locks.extend(state['random'])

    for l in state['random']:
        modify(input['guid'], input['worker'], 'locktest.' + l, '')

    locks = ['locktest-{0}'.format(l) for l in locks]

    log('Releasing locks<{0}>'.format(blob(locks)))
    return ('unlock', 'file modified', state, locks)

def loop(input, state):
    state['seq'] += 1

    if state['seq'] < 5:
        log('Wating in the loop({0})'.format(state['seq']))
        return ('retry', 'waiting in the loop', state, 1)

    message = dict(appname=input['appname'],
                   workername='sheepdog',
                   code='inform')

    log('Sending message<{0}> to sheepdog'.format(blob(message)))
    return ('message', 'informing sheepdog', state, [message])

def send_file(input, state):
    state['seq'] += 1

    filename = 'locktest.' + str(input['worker'])

    if os.path.isfile(filename):
        message = dict(appname=input['appname'],
                       workername='sheepdog',
                       code='file',
                       data=json.loads(open(filename).read()))
    else:
        message = dict(appname=input['appname'],
                       workername='sheepdog',
                       code='file',
                       data=dict(guid=input['guid'],
                                 total=0,
                                 workers=[],
                                 extraTotal=0,
                                 extraWorkers=[]))

    log('Sending message<{0}> to sheepdog'.format(blob(message)))
    return ('message', 'sending message', state, [message])

def done(input, state):
    log('Done with input<{0}> and state<{1}>'.format(blob(input), blob(state)))
    return ('ok', 'done')

workflow = {
    ('init',      'moveto'):  'get_lock',
    ('get_lock',  'lock'):    'locktest',
    ('locktest',  'unlock'):  'loop',
    ('handler',   'ok'):      'send_file',
    ('send_file', 'message'): 'done'
}
