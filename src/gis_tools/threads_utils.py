import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


def chunker(iterator, size):
    """Разбить исходный итерируемый объект на группы списков.
    Для случаев, когда большой список (или итератор неизвестной длины)
    нужно запустить в pool_execute частями
    """
    chunk = []
    for i in iterator:
        chunk.append(i)
        if len(chunk) == size:
            yield chunk[:]
            chunk.clear()
    if chunk:
        yield chunk


def pool_execute(func, inputs=None, workers=100, pool_type="thread", unpack_input=True):
    """Simple use of python threading pool

    Args:
        func (function): pool function
        inputs (None, optional): args for function
        workers (int, optional): count of pool workers
        pool_type (str, optional): pool type - thread or process
        unpack_input (bool, optional): if true, will unpack list-like inputs in args. Else use input as is

    Returns:
        list: func results

    Raises:
        TypeError: when use unknown type in pool_type
    """
    logger.debug("Start pool_execute...")
    if not inputs:
        return []
    if pool_type == "thread":
        p_executor = ThreadPoolExecutor
    elif pool_type == "process":
        p_executor = ProcessPoolExecutor
    else:
        raise TypeError(f"Неверный параметр pool_type={pool_type}")
    with p_executor(max_workers=workers) as executor:
        inputs = list(inputs)
        example = inputs[0]
        if (isinstance(example, list) or isinstance(example, tuple)) and unpack_input:
            futures = {executor.submit(func, *args): args for args in inputs}
        elif isinstance(example, dict) and unpack_input:
            futures = {executor.submit(func, **args): args for args in inputs}
        else:
            futures = {executor.submit(func, args): args for args in inputs}
        result = []
        for f in as_completed(futures):
            try:
                result.append(f.result())
            except Exception as e:
                raise e
            else:
                logger.info(f"Done {len(result)} out {len(futures)}")
    return result


def loop_execute(func, inputs=None, unpack_input=True, **kwargs):
    """Same as pool_execute, but it doesn't use threading or multiprocessing - just iterate inputs"""
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
            logger.info(f"Done {i+1} out {len(inputs)}")
        except:
            logger.exception(f"FATAL ERROR on args {item}")
            return results
    return results
