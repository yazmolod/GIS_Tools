from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import logging
logger = logging.getLogger(__name__)

def pool_execute(func, inputs=None, workers=100, pool_type='thread', unpack_input=True):
    logger.debug('Start pool_execute...')
    if not inputs:
        return []
    if pool_type == 'thread':
        p_executor = ThreadPoolExecutor
    elif pool_type == 'process':
        p_executor = ProcessPoolExecutor
    else:
        raise TypeError(f'Неверный параметр pool_type={pool_type}')
    with p_executor(max_workers=workers) as executor: 
        inputs = list(inputs)
        example = inputs[0]
        if (isinstance(example, list) or isinstance(example, tuple)) and unpack_input:
            futures = {executor.submit(func, *args):args for args in inputs}
        elif isinstance(example, dict) and unpack_input:
            futures = {executor.submit(func, **args):args for args in inputs}
        else:
            futures = {executor.submit(func, args):args for args in inputs}
        result = []
        for f in as_completed(futures):
            try:    
                result.append(f.result())
            except:
                logger.exception(f'FATAL ERROR on args {futures[f]}')
                return result
            else:
                logger.info(f'Done {len(result)} out {len(futures)}')
    return result


def loop_execute(func, inputs=None, unpack_input=True, **kwargs):
    if not inputs:
        return []
    results = []
    for i, item in enumerate(inputs):
        try:
            if (isinstance(item, list) or isinstance(item, tuple)) and unpack_input:
                result = func(*item)
            elif isinstance(item, dict) and unpack_input:
                result = func(**item)
            else:
                result = func(item)
            results.append(result)
            logger.info(f'Done {i+1} out {len(inputs)}')
        except:
            logger.exception(f'FATAL ERROR on args {item}')
            return results
    return results