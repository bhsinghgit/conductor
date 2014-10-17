import json

def worker(input, state, event, util):
    def exception(msg):
        util.log(
            'state transition. current({0}) return({1}) exception({2})'.format(
                current_state, result[0], msg))
        return dict(exception=msg)

    def commit(next, obj):
        util.log('state transition. current({0}) return({1}) next({2})'.format(
            current_state, result[0], next))
        return obj

    input = json.loads(input)

    if 'data' in event:
        event['data'] = json.loads(event['data'])

    if 'init' == event['code']:
        control_info = dict(state='init', seq=0)
        continuation  = None
    else:
        state = json.loads(state)
        control_info = state['control_info']
        continuation = state['continuation']
        control_info['seq'] += 1

    current_state = control_info['state']

    try:
        result = (None,)
        module = __import__(input['workflow'])
        method = getattr(module, current_state)
        if 'handler' == current_state:
            result = method(input['input'], continuation, event, util)
        else:
            result = method(input['input'], continuation, util)
    except Exception as e:
        return exception(str(e))

    if len(result) < 2:
        return exception('invalid return value')

    if 2 == len(result):
        return commit('done', dict(status=json.dumps(result[1])))

    if 'retry' != result[0]:
        control_info['state'] = module.workflow.get((control_info['state'],
                                                     result[0]), 'handler')

    next_state    = control_info['state']
    commit_status = json.dumps(result[1])
    commit_state  = json.dumps(dict(control_info=control_info,
                                    continuation=result[2]))


    if 'lock' == result[0]:
        return commit(next_state, dict(status=commit_status,
                                       state=commit_state,
                                       lock=result[3]))

    if 'unlock' == result[0]:
        return commit(next_state, dict(status=commit_status,
                                       state=commit_state,
                                       unlock=result[3],
                                       alarm=0))

    if 'retry' == result[0]:
        return commit(next_state, dict(status=commit_status,
                                       state=commit_state,
                                       alarm=result[3]))

    if 'message' == result[0]:
        for m in result[3]:
            if 'data' in m:
                m['data'] = json.dumps(m['data'])

        return commit(next_state, dict(status=commit_status,
                                       state=commit_state,
                                       message=result[3]))

    if ('handler' == current_state) and ('handler' == control_info['state']):
        return commit(next_state, dict(status=commit_status,
                                       state=commit_state))

    return commit(next_state, dict(status=commit_status,
                                   state=commit_state,
                                   alarm=0))
