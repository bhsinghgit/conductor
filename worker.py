import json

def worker(input, state, event, util):
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

        if len(result) < 2:
            raise Exception('invalid return value')
    except Exception as e:
        util.log(
            'state transition. current({0}) return({1}) exception({2})'.format(
                current_state, result[0], str(e)))
        return dict(exception=str(e))

    if 2 == len(result):
        util.log('state transition. finished. current({0}) return({1})'.format(
            current_state, result[0]))
        return dict(status=json.dumps(result[1]))

    if 'retry' != result[0]:
        control_info['state'] = module.workflow.get((control_info['state'],
                                                     result[0]), 'handler')

    next_state  = control_info['state']
    commit_dict = dict(status=json.dumps(result[1]),
                       state=json.dumps(dict(control_info=control_info,
                                             continuation=result[2])))

    if 'lock' == result[0]:
        commit_dict['lock']   = result[3]

    if 'unlock' == result[0]:
        commit_dict['unlock'] = result[3]

    if 'message' == result[0]:
        for m in result[3]:
            if 'data' in m:
                m['data'] = json.dumps(m['data'])

        commit_dict['message'] = result[3]

    if ('handler' != next_state) and ('lock' != result[0]):
        commit_dict['alarm'] = 0

    if 'retry' == result[0]:
        commit_dict['alarm'] = result[3]

    util.log('state transition. current({0}) return({1}) next({2})'.format(
            current_state, result[0], next_state))
    return commit_dict
