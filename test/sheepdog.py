import json
import os
import random

def init(input, state):
    track = dict(workers=dict(), extras=dict())
    for i in range(input['expected']):
        track['workers'][i] = 0
        track['extras'][i]  = 0

    state = dict(messages=0, signals=0, track=track)

    return ('ok', 'initialized', state)

def handler(input, state, event):
    if 'inform' == event['code']:
        state['signals'] += 1

        if state['signals'] < input['expected']:
            return ('ok', 'received signal', state)

        messages = list()
        for i in range(input['expected']):
            messages.append(dict(appname='testapp',
                                 workername='sheep-' + str(i),
                                 code='signal'))

        return ('message', 'sent signal', state, messages)

    if 'file' == event['code']:
        data = event['data']

        state['messages'] += 1

        if input['guid'] == data['guid']:
            if len(data['workers']) == data['total']:
                for w in data['workers']:
                    state['track']['workers'][str(w)] += 1

            if len(data['extraWorkers']) == data['extraTotal']:
                for w in data['extraWorkers']:
                    state['track']['extras'][str(w)] += 1

        if input['expected'] == state['messages']:
            for i in range(input['expected']):
                if state['track']['workers'][str(i)] != 1:
                    return ('done', 'FAIL')

            for w, value in state['track']['extras'].iteritems():
                if value != 5:
                    return ('done', 'FAIL')

            return ('done', 'WAITING', state)

        return ('continue', 'message processed', state)

    if 'report' == event['code']:
        state['result'] = 'PASS'
        return ('done', 'REPORTING', state)

def done(input, state):
    if 'result' in state:
        return ('ok', state['result'])

    if 'sleep_count' not in state:
        state['sleep_count'] = 0

    state['sleep_count'] += 1
    return ('retry', 'SLEEPING_FOR_SIGNAL_'+str(state['sleep_count']), state, 5)

workflow = {
    ('handler', 'done'): 'done'
}
