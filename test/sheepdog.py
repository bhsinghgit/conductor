import json
import os
import random

log = shepherd.log
blob = shepherd.blob

def init(input, state):
    log('Starting sheepdog with input<{0}>'.format(blob(input)))

    track = dict(workers=dict(), extras=dict())
    for i in range(input['expected']):
        track['workers'][i] = 0
        track['extras'][i]  = 0

    state = dict(messages=0, signals=0, track=track)

    return ('ok', 'initialized', state)

def handler(input, state, event):
    log('Handler invokded with input<{0}>, state<{1}> and event<{2}>'.format(
        blob(input), blob(state), blob(event)))

    if 'inform' == event['code']:
        state['signals'] += 1

        if state['signals'] < input['expected']:
            return ('ok', 'received signal', state)

        messages = list()
        for i in range(input['expected']):
            messages.append(dict(appname=input['appname'],
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
        log('Done with input<{0}> and state<{1}>'.format(
            blob(input), blob(state)))
        return ('ok', state['result'])

    if 'sleep_count' not in state:
        state['sleep_count'] = 0

    state['sleep_count'] += 1
    log('Wating for signal..{0}'.format(state['sleep_count']))
    return ('retry', 'SLEEPING_FOR_SIGNAL_'+str(state['sleep_count']), state, 5)

workflow = {
    ('handler', 'done'): 'done'
}
