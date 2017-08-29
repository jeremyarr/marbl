from abc import ABCMeta, abstractmethod
import asyncio
import traceback
import functools
import os
import time
import datetime

# from .utils import create_multiple_tasks

class StopTimeout(Exception):
    pass

class AlreadyRunning(Exception):
    pass

class MultipleMarblError(Exception):
    pass

class MutableBool(object):
    def __init__(self, init_val):
        self._x = init_val

    def set_(self):
        self._x = True

    def clear(self):
        self._x = False

    def __bool__(self):
        return self._x == True

    def __eq__(self, other):
        return self._x == other

    def __repr__(self):
        return ("<{} ({}) at {}>"
               "").format( self.__class__.__name__,
                           self._x,
                           hex(id(self))
                   )

class Trigger(MutableBool):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,**kwargs)
        self._clear_trigger_metadata()

    def set_as_error(self, exc_obj, tb_str):
        self.set_()
        self.cause = "error"
        self.error_type = type(exc_obj)
        self.error_traceback = tb_str

    def set_as_stop(self):
        self.set_()
        self.cause = "stop"

    def set_as_cascade(self):
        self.set_()
        self.cause = "cascade"

    def set_as_remote_stop(self):
        self.set_()
        self.cause = "remote_stop"

    def clear(self):
        self._x = False
        self._clear_trigger_metadata()

    def _clear_trigger_metadata(self):
        self.cause = None
        self.error_type = None
        self.error_traceback = None


def runner(func):
    @functools.wraps(func)
    async def inner(self, *args, **kwargs):
        try:
            show_errors=kwargs['show_errors']
        except KeyError:
            show_errors=True


        if self.is_running():
            raise AlreadyRunning

        self._running.set_()
        self.has_stopped = asyncio.get_event_loop().create_future()
        try:
            await self._pre_run()
            await self.pre_run()
            await func(self, *args, **kwargs)
            await self.post_run()
        except Exception as e:
            tb_str = traceback.format_exc()
            if show_errors:
                err_msg = "AN ERRROR OCCURRED\n {}".format(tb_str)
                print(err_msg)
                try:
                    self._logger.error(err_msg)
                    await asyncio.sleep(1)
                except AttributeError:
                    pass
            self.trigger.set_as_error(e, tb_str)
        finally:
            self._running.clear()
            self.has_stopped.set_result(None)


    return inner


class Marbl(metaclass=ABCMeta):

    version = "not_set"
    trigger = Trigger(False)


    @abstractmethod
    async def setup(self, *args, **kwargs):
        pass

    def register(self, *args, **kwargs):
        return []

    def register_standard_marbls(self):
        return [
            self.register_receiver(),
            self.register_logger(),
            self.register_ticker(),
            self.register_remote_control(),
            ]


    def register_receiver(self):
        try: 
            receiver_obj = Receiver(conn=self._conn)
        except MultipleMarblError:
            return None
        except AttributeError:
            return None
        else:
            return {
                     "marbl_obj":receiver_obj,
                     "run_method":"run",
                     "interval":0.1,
                    }

    def register_logger(self):
        try: 
            logger_obj = Logger(conn=self._conn,logger=self._logger)
        except MultipleMarblError:
            return None
        except AttributeError:
            return None
        else:
            return {
                     "marbl_obj":logger_obj,
                     "run_method":"run",
                     "interval":0.1,
                    }

    def register_ticker(self):
        try: 
            ticker_obj = Ticker(conn=self._conn,logger=self._logger, 
                            marbl_name=self.__class__.__name__.lower(), 
                            marbl_version=self.version)
        except MultipleMarblError:
            return None
        except AttributeError:
            return None
        else:
            return {
                     "marbl_obj":ticker_obj,
                     "run_method":"run",
                     "interval":2,
                    }

    def register_remote_control(self):
        try: 
            remote_control_obj = RemoteControl(conn=self._conn,logger=self._logger, 
                                    marbl_name=self.__class__.__name__.lower())
        except MultipleMarblError:
            return None
        except AttributeError:
            return None
        else:
            return {
                     "marbl_obj":remote_control_obj,
                     "run_method":"do_not_run",
                    }


    async def _pre_run(self):
        registry = self.register()
        registry.extend(self.register_standard_marbls())

        to_schedule = []

        for r in registry:
            if not r:
                continue

            await r['marbl_obj'].setup()

            if r['run_method'] == "run":
                coro_obj = r['marbl_obj'].run(interval=r['interval'])
                to_schedule.append(coro_obj)
            elif r['run_method'] == "do_not_run":
                pass
            else:
                raise NotImplementedError


        for coro_obj in to_schedule:
            _, launched = self.create_task(coro_obj)
            await launched



    async def pre_run(self, *args, **kwargs):
        pass

    @abstractmethod
    async def main(self, *args, **kwargs):
        pass

    async def post_run(self, *args, **kwargs):
        pass

    # @consume_exceptions
    @runner
    async def run_once(self,*, main_args=(), main_kwargs={}, show_errors=True):
        await self.main(*main_args,**main_kwargs)

    # @consume_exceptions
    @runner
    async def run(self, *, num_cycles=None, interval=1, main_args=(), main_kwargs={}, show_errors=True):
        cnt = 0

        while True:
            brk = await self.main(*main_args,**main_kwargs)

            if interval != 0:
                await self.sleep_lightly(interval)

            if self.trigger:
                break

            if num_cycles is not None:
                cnt = cnt + 1

                if cnt == num_cycles:
                    break



    async def sleep_lightly(self,interval):
        num_cycles, frac_secs = divmod(interval, 0.1)

        for x in range(int(num_cycles)):
            if self.trigger:
                return
            else:
                await asyncio.sleep(0.1)

        await asyncio.sleep(frac_secs)

    def is_running(self):
        return self._running == True


    @property
    def _running(self):
        try:
            return self._running_internal
        except AttributeError:
            self._running_internal = MutableBool(False)
            return self._running_internal

    def create_task(self, coro_obj, show_errors=True):
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
            try:
                launched.set_result(True)
                await coro_obj
            except Exception as e:
                tb_str = traceback.format_exc()
                if show_errors:
                    err_msg = "AN ERRROR OCCURRED\n {}".format(tb_str)
                    print(err_msg)
                    try:
                        self._logger.error(err_msg)
                        await asyncio.sleep(1)
                    except AttributeError:
                        pass


                self.trigger.set_as_error(e, tb_str)



        launched = loop.create_future()

        return loop.create_task(task_wrapper(coro_obj, launched)), launched




class Receiver(Marbl):

    already_exists = MutableBool(False)

    def __init__(self, *, conn):
        self._conn = conn

        if self.already_exists:
            raise MultipleMarblError("tried to instantiate another Receiver")
        else:
            self.already_exists.set_()

    async def setup(self):
        pass

    def register_standard_marbls(self):
        return []

    async def main(self):
        await self._conn.process_events(num_cycles=1)


class Logger(Marbl):

    already_exists = MutableBool(False)

    def __init__(self, *, conn, logger):
        self._conn = conn
        self.logger = logger

        if self.already_exists:
            raise MultipleMarblError("tried to instantiate another Logger")
        else:
            self.already_exists.set_()

    async def setup(self):
        self._chan = await self._conn.create_channel()
        await self._chan.register_producer(exchange_name="log",
                exchange_type="topic")

    def register_standard_marbls(self):
        return []

    async def main(self):
        try:
            msg, topic = await asyncio.wait_for(self.logger.log_queue.get(),0.1)
        except asyncio.TimeoutError:
            pass
        else:
            await self._chan.publish(
                    exchange_name="log",
                    msg=msg,
                    routing_key=topic
                  )


class Ticker(Marbl):

    already_exists = MutableBool(False)

    def __init__(self, *, conn, marbl_name, marbl_version, logger, show=True, publish=True):
        self._conn = conn
        self._tick = 0
        self._marbl_version = marbl_version
        self._marbl_name = marbl_name
        self._pid = os.getpid()
        self._show = show
        self._publish = publish
        self._started = time.time()
        self._logger = logger

        if self.already_exists:
            raise MultipleMarblError("tried to instantiate another Logger")
        else:
            self.already_exists.set_()

    async def setup(self):
        self._chan = await self._conn.create_channel()
        await self._chan.register_producer(exchange_name="heartbeat",
                exchange_type="direct")

    def register_standard_marbls(self):
        return []

    def _construct_msg(self):
        return {
                "tick":self._tick,
                "version":self._marbl_version,
                "name": self._marbl_name,
                "pid": self._pid,
                "started": self._started
              }

    async def main(self):
        self._tick = self._tick + 1

        if self._publish:
            await self._chan.publish(
                    exchange_name="heartbeat",
                    msg=self._construct_msg(),
                    routing_key=""
                  )

        iso_str = datetime.datetime.utcfromtimestamp(self._started).isoformat()
        tick_msg = "tick {} {} {} ({}) started {}".format(
                    self._tick, self._marbl_name, 
                    self._marbl_version, self._pid, iso_str)

        if self._show:
            print(tick_msg)

        self._logger.debug(tick_msg)

class RemoteControl(Marbl):
    already_exists = MutableBool(False)

    def __init__(self, *, conn, logger, marbl_name):
        self._conn = conn
        self._logger = logger
        self._marbl_name = marbl_name

        if self.already_exists:
            raise MultipleMarblError("tried to instantiate another RemoteControl")
        else:
            self.already_exists.set_()


    def register_standard_marbls(self):
        return []


    async def setup(self):
        self._chan = await self._conn.create_channel()
        routing_keys =[ 
                        "{}.all.all".format(self._marbl_name), #all <marbl_name> micros (any version)
                        "{}.all.{}".format(self._marbl_name,self.version), #all <marbl_name> micros (with version <version>)
                        "all.all.all", #all micros (any version)
                        "all.{}.all".format(os.getpid()),#any micro with pid <pid>
                      ]

        await self._chan.register_consumer(exchange_name="supervisor",
                exchange_type="topic", routing_keys=routing_keys,
                callback=self.take_action,
                create_task_meth=self.create_task)
        
    async def main(self):
        pass

    async def take_action(self, resp):
        self.trigger.set_as_remote_stop()





def add_std_non_logging_options(parser):
    parser.add_argument("-interval", type=float, default=1, help="delay between cycles")
    parser.add_argument("-num_cycles",type=int, default=None, help="number of cycles to run for")

    parser.add_argument("-port", default=5672, type=int, help="broker port")
    parser.add_argument("-host", default="localhost", type=str, help="broker host")
    parser.add_argument("-broker", default="rabbit", type=str, help="broker type")
    parser.add_argument("-virtual_host", type=str, help="broker virtual host")

def add_standard_options(parser, default_marbl_name="marbl_name", default_app_name="marbl"):
    add_std_non_logging_options(parser)

    parser.add_argument("-marbl_name", default=default_marbl_name, help="name of the marbl for logging purposes")
    parser.add_argument("-app_name", default=default_app_name, help="name of the app for logging purposes")