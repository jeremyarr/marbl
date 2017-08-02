from ..base import Marbl

class MessageProcessor(Marbl):
    def __init__(self, *, conn):
        self._conn = conn

    async def setup(self):
        pass

    async def main(self):
        await self._conn.process_events(num_cycles=1)

    async def pre_run(self):
        pass

    async def post_run(self):
        pass