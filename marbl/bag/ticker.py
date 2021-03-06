import os

from marbl import Marbl

class Ticker(Marbl):
    def __init__(self, *, conn, parent_name, parent_version, show=True):
        self._conn = conn
        self._tick = 0
        self._parent_version = parent_version
        self._parent_name = parent_name
        self._pid = os.getpid()
        self._show = show

    async def setup(self):
        self._chan = await self._conn.create_channel()
        await self._chan.register_producer(exchange_name="heartbeat",
                exchange_type="direct")


    def _construct_msg(self):
        return {
                "tick":self._tick,
                "version":self._parent_version,
                "name": self._parent_name,
                "pid": self._pid,
              }

    async def main(self):
        self._tick = self._tick + 1

        await self._chan.publish(
                exchange_name="heartbeat",
                msg=self._construct_msg(),
                routing_key=""
              )
        if self._show:
            print("tick {} {} {} ({})".format(
                    self._tick, self._parent_name, 
                    self._parent_version, self._pid))

