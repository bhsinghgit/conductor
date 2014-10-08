import json

def worker(input, state, event):
    if 'init' == event['code']:
        return dict(status='received init',
                    state=json.dumps(dict(seq=1)),
                    alarm=5)

    if 'alarm' == event['code']:
        state = json.loads(state)
        if 10 == state['seq']:
            return dict(status='done')

        state['seq'] += 1
        return dict(status='received init',
                    state=json.dumps(state),
                    alarm=5)
