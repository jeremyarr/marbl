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


def mark_as_running(func):
    async def inner(self, *args, **kwargs):
        self.running.set_()
        self.has_stopped = asyncio.get_event_loop().create_future()
        try:
            await func(self, *args, **kwargs)
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
    async def main(self, *args, **kwargs):
        pass


    @mark_as_running
    async def run_once(self):
        await self.main()


    @mark_as_running
    async def run(self, *, num_cycles=None, interval=1):
        cnt = 0

        while True:
            brk = await self.main()

            if interval != 0:
                await self.sleep_lightly(interval)

            if self.triggered:
                break

            if num_cycles is not None:
                cnt = cnt + 1

                if cnt == num_cycles:
                    break


    async def stop(self, timeout=1):
        self.triggered.set_()
        try:
            done, pending = await asyncio.wait([self.has_stopped], timeout=timeout)
        except AttributeError:
            pass
        else:
            if pending:
                raise StopTimeout

    async def sleep_lightly(self,interval):
        num_cycles,frac_secs = divmod(interval,0.1)

        for x in range(int(num_cycles)):
            if self.triggered:
                return
            else:
                await asyncio.sleep(0.1)

        await asyncio.sleep(frac_secs)


    def is_running(self):
        return self.running == True

    @property
    def triggered(self):
        try:
            return self._triggered
        except AttributeError:
            self._triggered = MutableBool(False)
            return self._triggered

    @property
    def running(self):
        try:
            return self._running
        except AttributeError:
            self._running = MutableBool(False)
            return self._running




