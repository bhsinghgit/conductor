import json
import workflow

def worker(input, state, event):
    print('here')
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
        method = getattr(workflow, control_info['state'])
        result = method(input, continuation, event)
    except Exception as e:
        result = ('done', 'exception: ' + str(e), dict())

    state_tuple = (control_info['state'], result[0])
    if state_tuple in workflow.workflow:
        control_info['state'] = workflow.workflow[state_tuple]
    else:
        result = ('done', 'next state not found', dict())

    commit_status = json.dumps(result[1])
    commit_state  = json.dumps(dict(control_info=control_info,
                                    continuation=result[2]))

    if 'ok' == result[0]:
        return dict(status=commit_status,
                    state=commit_state,
                    alarm=0)

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

    if 'done' == result[0]:
        return dict(status=commit_status)
