import asyncio
import argparse
import logging


class RabbitLogQueueTimeout(Exception):
    pass

class RabbitMQHandler(logging.Handler):
    def __init__(self, log_queue, marbl_name, *args, **kwargs):
        super().__init__(*args,**kwargs)
        self.log_queue = log_queue
        self.marbl_name = marbl_name


    def emit(self,record):

        msg = self.create_msg(record)
        topic = self.create_topic(record)

        log_q_msg = (msg, topic)
        #queue is an asyncio queue
        #because the listener is a coroutine
        #and not run in a separate thread.
        #because this function can be run in a coroutine
        #it needs to run sequentially and not be scheduled,
        #because that might cause the msg not to be logged
        #or be logged out of sequence.
        #it needs to not block for excessive amount of time
        #if you cant put an item on the queue.
        #will give up after five attempts (0.63s) (exponential backoff)
        cnt=0
        while cnt <= 5:
            try:
                self.log_queue.put_nowait(log_q_msg)
                # print("emitted: {}".format(log_q_msg))
                return
            except asyncio.QueueFull:
                time.sleep((2**cnt)*0.01)
                cnt=cnt+1
        
        raise RabbitLogQueueTimeout("tried to put msg in log_queue but kept getting told queue was full")

    def create_msg(self,record):
        record.marbl_name=self.marbl_name
        msg = self.format(record)
        return msg

    def create_topic(self,record):
        logger_name = record.name #need to create a new LogRecord Attribute
        pid = record.process
        severity = record.levelname.lower()
        #marbl_name.logger,pid,severity
        topic = "{}.{}.{}.{}".format(self.marbl_name,logger_name.replace('.','__'),pid,severity)

        return topic


class MarblLogger(object):
    def __init__(self, logger, log_queue):
        self._logger = logger
        self.log_queue = log_queue

    def info(self, msg):
        self._logger.info(msg)

    def error(self, msg):
        self._logger.error(msg)

    def warning(self, msg):
        self._logger.warning(msg)

    def debug(self, msg):
        self._logger.debug(msg)

    def critical(self, msg):
        self._logger.critical(msg)


def create_logger(*, app_name, marbl_name):
    logger = logging.getLogger(app_name)
    logger.setLevel(logging.DEBUG)
    logger.propagate=True

    log_queue = asyncio.Queue()
    hdlr = _create_rabbit_handler(
                level=logging.DEBUG, 
                marbl_name=marbl_name,
                log_queue=log_queue)

    logger.addHandler(hdlr)

    return MarblLogger(logger, log_queue)

def _create_rabbit_handler(*, level, marbl_name, log_queue):
    hdlr = RabbitMQHandler(log_queue=log_queue, 
                           marbl_name=marbl_name)
    hdlr.setLevel(level)

    #%(marbl_name) is a record attribute that gets added before emitting
    form = logging.Formatter(fmt="%(asctime)s: %(levelname)s: %(marbl_name)s: %(name)s (%(process)d): %(message)s")
    hdlr.setFormatter(form)
    return hdlr


def create_task(coro_obj):
    '''
    wrapper for creating a task that can be used for waiting
    until a task has started.

    :param coro_obj: coroutine object to schedule
    :returns: a two element tuple where the first element
        is the task object. Awaiting on this will return when
        the coroutine object is done executing. The second element
        is a future that becomes done when the coroutine object is started.

    .. note:: must only be called from within the thread
        where the event loop resides
    '''

    loop = asyncio.get_event_loop()

    async def task_wrapper(coro_obj, launched):
        launched.set_result(True)
        await coro_obj
    
    launched = loop.create_future()
    t = loop.create_task(task_wrapper(coro_obj, launched))
    t.task_name = coro_obj
    return t, launched
    # return loop.create_task(task_wrapper(coro_obj, launched)), launched
    # return loop.create_task(task_wrapper(coro_obj, launched)), launched

async def create_multiple_tasks(coro_obj_list):
    for coro_obj in coro_obj_list:
        _, launched = create_task(coro_obj)
        await launched