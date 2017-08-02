from abc import ABCMeta, abstractmethod
import asyncio

class StopTimeout(Exception):
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


def runner(func):
    async def inner(self, *args, **kwargs):
        self.running.set_()
        self.has_stopped = asyncio.get_event_loop().create_future()
        try:
            await self.pre_run()
            await func(self, *args, **kwargs)
            await self.post_run()
        finally:
            self.running.clear()
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


    @runner
    async def run_once(self,*, main_args=(), main_kwargs={}):
        await self.main(*main_args,**main_kwargs)


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
        self.trigger.set_()
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
        return self.running == True

    def is_triggered(self):
        return self.trigger == True

    @property
    def trigger(self):
        try:
            return self._trigger
        except AttributeError:
            self._trigger = MutableBool(False)
            return self._trigger

    @trigger.setter
    def trigger(self, val):
        if type(val) != bool:
            raise TypeError("trigger must be set to True or False")

        if not hasattr(self,"_trigger"):
            self._trigger = MutableBool(False)

        if val:
            self._trigger.set_()
        else:
            self._trigger.clear()


    @property
    def running(self):
        try:
            return self._running
        except AttributeError:
            self._running = MutableBool(False)
            return self._running




