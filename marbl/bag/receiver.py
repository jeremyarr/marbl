from marbl import Marbl

class Receiver(Marbl):
    def __init__(self, *, conn):
        self._conn = conn

    async def setup(self):
        pass

    async def main(self):
        await self._conn.process_events(num_cycles=1)


