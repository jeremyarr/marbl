from abc import ABCMeta, abstractmethod
import asyncio
import traceback

class StopTimeout(Exception):
    pass

class AlreadyRunning(Exception):
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

    def clear(self):
        self._x = False
        self._clear_trigger_metadata()

    def _clear_trigger_metadata(self):
        self.cause = None
        self.error_type = None
        self.error_traceback = None


def runner(func):
    async def inner(self, *args, **kwargs):

        if self.is_running():
            raise AlreadyRunning

        self.trigger.clear()
        self._running.set_()
        self.has_stopped = asyncio.get_event_loop().create_future()
        try:
            await self.pre_run()
            await func(self, *args, **kwargs)
            await self.post_run()
        except Exception as e:
            tb_str = traceback.format_exc()
            # print("AN ERRRORRRRRRRRRRR")
            # print(tb_str)
            self.trigger.set_as_error(e, tb_str)
        else:
            self.trigger.set_as_stop()
        finally:
            self._running.clear()
            self.has_stopped.set_result(None)


    return inner


class Marbl(metaclass=ABCMeta):

    version = "not_set"

    @abstractmethod
    async def setup(self, *args, **kwargs):
        pass

    @abstractmethod
    async def pre_run(self, *args, **kwargs):
        pass

    @abstractmethod
    async def main(self, *args, **kwargs):
        pass

    @abstractmethod
    async def post_run(self, *args, **kwargs):
        pass

    # @consume_exceptions
    @runner
    async def run_once(self,*, main_args=(), main_kwargs={}):
        await self.main(*main_args,**main_kwargs)

    # @consume_exceptions
    @runner
    async def run(self, *, num_cycles=None, interval=1, main_args=(), main_kwargs={}):
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


    async def stop(self, timeout=1):
        self.trigger.set_as_stop()

        try:
            done, pending = await asyncio.wait([self.has_stopped], timeout=timeout)
        except AttributeError:
            pass
        else:
            if pending:
                raise StopTimeout

    async def sleep_lightly(self,interval):
        num_cycles,frac_secs = divmod(interval, 0.1)

        for x in range(int(num_cycles)):
            if self.trigger:
                return
            else:
                await asyncio.sleep(0.1)

        await asyncio.sleep(frac_secs)


    def is_running(self):
        return self._running == True

    def is_triggered(self):
        return self.trigger == True

    @property
    def trigger(self):
        try:
            return self._trigger
        except AttributeError:
            self._trigger = Trigger(False)
            return self._trigger

    @property
    def _running(self):
        try:
            return self._running_internal
        except AttributeError:
            self._running_internal = MutableBool(False)
            return self._running_internal

    def create_task(self, coro_obj):
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
                # print("AN ERRRORRRRRRRRRRR")
                # print(tb_str)
                self.trigger.set_as_error(e, tb_str)


        launched = loop.create_future()
        return loop.create_task(task_wrapper(coro_obj, launched)), launched