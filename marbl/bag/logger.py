import asyncio
import logging
import time

from ..base import Marbl

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



#this should only be added to the root marbl
#TODO investigate logger as class attribute set here and available in all Marbls
class Logger(Marbl):
    def __init__(self, *, conn, marbl_name, app_name="marbl"):
        self._conn = conn
        self._marbl_name = marbl_name
        self._app_name = app_name
        self._log_queue = asyncio.Queue()
        self.logger = self._create_logger()


    async def setup(self):
        self._chan = await self._conn.create_channel()
        await self._chan.register_producer(exchange_name="log",
                exchange_type="topic")


    async def main(self):
        try:
            msg, topic = await asyncio.wait_for(self._log_queue.get(),0.1)
        except asyncio.TimeoutError:
            pass
        else:
            await self._chan.publish(
                    exchange_name="log",
                    msg=msg,
                    routing_key=topic
                  )

    async def pre_run(self):
        pass

    async def post_run(self):
        pass

    def _create_logger(self):
        logger = logging.getLogger(self._app_name)
        logger.setLevel(logging.DEBUG)
        logger.propagate=True

        hdlr = self._create_rabbit_handler(logging.DEBUG)
        logger.addHandler(hdlr)

        return logger

    def _create_rabbit_handler(self, level):
        hdlr = RabbitMQHandler(log_queue=self._log_queue, 
                               marbl_name=self._marbl_name)
        hdlr.setLevel(level)

        #%(marbl_name) is a record attribute that gets added before emitting
        form = logging.Formatter(fmt="%(asctime)s: %(levelname)s: %(marbl_name)s: %(name)s (%(process)d): %(message)s")
        hdlr.setFormatter(form)
        return hdlr

