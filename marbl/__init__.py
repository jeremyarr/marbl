'''
marbl is an asyncio microservices framework.

'''

from .__version__ import __version__

from .utils import create_task
from .base import Marbl, StopTimeout
from . import bag




# import time
# import threading

# g = threading.local()

# import mooq
# import os


# class MutableBool(object):
#     def __init__(self, init_val):
#         self._x = init_val

#     def set_(self):
#         self._x = True

#     def clear(self):
#         self._x = False

#     def __bool__(self):
#         return self._x == True

#     def __eq__(self, other):
#         return self._x == other

#     def __repr__(self):
#         return ("<{} ({}) at {}>"
#                "").format( self.__class__.__name__,
#                            self._x,
#                            hex(id(self))
#                    )




















#     def __init__(self,*,logger=None, name="no_name", version="no_version"):
#         self.pid = os.getpid()
#         self.logger = logger
#         self.trigger = trigger
#         self.name = name
#         self.version = version
#         self.tick = 0

#     async def setup(self,*, conn):
#         self.chan = await conn.create_channel()
#         await self.chan.register_producer(exchange_name="heartbeat",
#                                      exchange_type="direct")

#     def heartbeat_msg(self):
#         return (self.name, self.version, self.tick, self.pid)

#     async def publish_heartbeat(self):
#         self.tick = self.tick + 1
#         msg = self.heartbeat_msg()

#         await self.chan.publish(
#                 exchange_name="heartbeat",
#                 msg=msg,
#                 routing_key=""
#         )
#         print("tick {} {} {} ({})".format(msg.name, msg.version, msg.tick, msg.pid))
#         self.logger.debug(msg)

#     async def run(self):
#         await run_helper(coro_obj=self.publish_heartbeat(),
#                          trigger=self.trigger,

#                          interval=2)

#     async def run_once(self):
#         await run_once_helper(coro_obj=self.publish_heartbeat())






# class LogMessages(object):
#     def __init__(self,*,logger, log_q, trigger, conn):
#         self.trigger = False
#         self.logger = logger
#         self.log_q = log_q
#         self.conn = conn

#     async def setup(self):
#         self.chan = await self.conn.create_channel()
#         await self.chan.register_producer(exchange_name="log",
#                                      exchange_type="topic")

#     async def _process_log_queue(self):
#         try:
#             msg, topic = await asyncio.wait_for(self.log_q.get(),0.1)
#         except asyncio.TimeoutError:
#             pass
#         else:
#             await self._publish_log(msg,topic)

#     async def _publish_log(self,msg,topic):
#         await self.chan.publish(
#                 exchange_name="log",
#                 msg=msg,
#                 routing_key=topic
#         )

#     async def run(self):
#         await run(self._process_log_queue(),0)



# async def run_once_helper(coro_obj,trigger=None, initial_tasks=[]):
#     await run_helper(coro_obj=coro_obj,
#               interval=0, 
#               trigger=trigger,
#               num_cycles=1,
#               initial_tasks=[])

# async def run_helper(coro_obj=None, interval=1, trigger=None, 
#               num_cycles=None, initial_tasks=[]):

#     for t in initial_tasks:
#         _, launched = create_task(t)
#         await launched

#     if coro_obj is None:
#         return

#     if num_cycles is not None:
#         cnt = 0

#     while True:
#         brk = await coro_obj
#         if brk:
#             break

#         if interval != 0:
#             await sleep_lightly(interval, trigger)

#         if trigger == True:
#             break

#         if num_cycles is not None:
#             cnt = cnt + 1
#             if cnt == num_cycles:
#                 break


# class MyMicro(object):
#     def __init__(self):
#         self.logger = create_logger()
#         self.trigger = create_trigger()

#         self.log_messages = LogMessages(trigger=self.trigger, logger=self.logger)
#         self.heartbeat = Heartbeat(trigger=self.trigger, logger=self.logger)
#         self.consume_messages = ConsumeMessages(trigger=self.trigger, logger=self.logger)


#     async def setup(self, host="localhost", port=5672, broker="rabbit"):
#         conn = await mooq.connect(host, port, broker)

#         await self.log_messages.setup(conn)
#         await self.heartbeat.setup(conn)
#         await self.consume_messages.setup(conn)

#     async def run(self):
#         await run(
#                     coro_obj = None,
#                     inital_tasks = [
#                                     self.log_messages.run(),
#                                     self.heartbeat.run(),
#                                     self.consume_messages.run(),
#                                     ]
#                   )
















