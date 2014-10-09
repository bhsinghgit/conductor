import json
import os

def locktest():
    fd = os.open('/tmp/conductor.locktest', os.O_CREAT|os.O_RDWR)

    raw  = os.read(fd, 1000000)

    if (raw is None) or ('' == raw):
        obj = dict(count=0)
    else:
        obj = json.loads(raw)

    obj['count'] += 1

    os.lseek(fd, 0, os.SEEK_SET)
    os.write(fd, json.dumps(obj))

def worker(input, state, event):
    if 'init' == event['code']:
        return dict(status='received init',
                    state=json.dumps(dict(seq=0)),
                    alarm=-1)

    state = json.loads(state)
    state['seq'] += 1

    if 1 == state['seq']:
        return dict(status='alarm',
                    state=json.dumps(state),
                    lock='locktest')

    if (2 == state['seq']) and ('locked' == event['code']):
        locktest()
        return dict(status='alarm',
                    state=json.dumps(state),
                    alarm=3)

    if 3 == state['seq']:
        return dict(status='alarm',
                    state=json.dumps(state),
                    unlock='locktest',
                    alarm=0)
    
    if 'alarm' == event['code']:
        if state['seq'] < 5:
            return dict(status='alarm',
                        state=json.dumps(state),
                        alarm=1)
        else:
            return dict(status='done')
