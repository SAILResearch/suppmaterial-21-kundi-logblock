from functools import wraps
from datetime import datetime
import logging
from threading import Thread
from functools import wraps
from multiprocessing import Process, Pool
from queue import Queue
import threading
import os
import signal
from contextlib import contextmanager

logging.basicConfig(filename='runtime.log', level=logging.INFO)
log = logging.getLogger("Util")


def elpased_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        end = datetime.now()

        log.info('Funtion: {} finished, Elapsed time: {!s}'.format(func.__name__, end - start))
        return result

    return wrapper


def _getAllFiles(folder, ext='.log', removeList= [], start=None):
    """
    Get all files with extension under a folder
    :param folder:
    :param ext:
    :return:
    """
    files_list = []
    for root, dirs, files in os.walk(folder):
        for file in files:
            if start is None:
                if file.endswith(ext):
                    files_list.append(os.path.join(root, file))
            else:
                if file.startswith(start):
                    files_list.append(os.path.join(root, file))
        return [x for x in files_list if x not in removeList]


def run_async(func):
    """
    Run function in parallel
    :param func:
    :return:
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # TODO: If queue needed, just uncomment the following code,
        #  and get by [job.get() for job in [func(x,y...) for i in ITERATION]]
        # queue = Queue()
        # thread = Thread(target=func, args=(queue,) + args, kwargs=kwargs)
        # thread.start()
        # return queue
        thread = Thread(target=func, args=args, kwargs=kwargs)
        thread.start()
        #thread.join()
        return thread
    return wrapper


def run_async_multiprocessing(func):
    """
    Run function in parallel
    :param func:
    :return:
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        proc = Process(target=func, args=args, kwargs=kwargs)
        proc.start()
        return proc
    return wrapper

@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise TimeoutError("Timed out!")
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)