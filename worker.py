import json

def worker(input, state, event):
    input = json.loads(input)

    if 'init' == event['code']:
        control_info = dict(state='init', seq=0)
        continuation  = None
    else:
        state = json.loads(state)
        control_info = state['control_info']
        continuation = state['continuation']
        control_info['seq'] += 1

    try:
        module = __import__(input['workflow'])
        method = getattr(module, control_info['state'])
        result = method(input['input'], continuation)
    except Exception as e:
        return dict(status=json.dumps('exception: ' + str(e)))

    if 1 == len(result):
        return dict(status=json.dumps(result[0]))

    state_tuple = (control_info['state'], result[0])
    if state_tuple in module.workflow:
        control_info['state'] = module.workflow[state_tuple]
    else:
        return dict(status=json.dumps('next state not found'))

    commit_status = json.dumps(result[1])
    commit_state  = json.dumps(dict(control_info=control_info,
                                    continuation=result[2]))

    if 'lock' == result[0]:
        return dict(status=commit_status,
                    state=commit_state,
                    lock=result[3])

    if 'unlock' == result[0]:
        return dict(status=commit_status,
                    state=commit_state,
                    unlock=result[3],
                    alarm=0)

    if 'retry' == result[0]:
        return dict(status=commit_status,
                    state=commit_state,
                    alarm=result[3])

    return dict(status=commit_status,
                state=commit_state,
                alarm=0)
