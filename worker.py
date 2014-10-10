import json
import os

def locktest(input):
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

def worker(input, state, event):
    input = json.loads(input)
    index = ('%010d' % (input['worker']))[9]

    if 'init' == event['code']:
        return dict(status='received init',
                    state=json.dumps(dict(seq=0)),
                    alarm=-1)

    state = json.loads(state)
    state['seq'] += 1

    if 1 == state['seq']:
        return dict(status='alarm',
                    state=json.dumps(state),
                    lock='locktest-' + index)

    if (2 == state['seq']) and ('locked' == event['code']):
        locktest(input)
        return dict(status='alarm',
                    state=json.dumps(state),
                    alarm=3)

    if 3 == state['seq']:
        return dict(status='alarm',
                    state=json.dumps(state),
                    unlock='locktest-' + index,
                    alarm=0)
    
    if 'alarm' == event['code']:
        if state['seq'] < 5:
            return dict(status='alarm',
                        state=json.dumps(state),
                        alarm=1)
        else:
            return dict(status='done')
