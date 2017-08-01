from ..base import Marbl
import asyncio

class Dummy(Marbl):
    def __init__(self, raise_error=None, 
            sleep_for=0.01, sleep_lightly_for=0,
            ):
        self.raise_error = raise_error
        self.sleep_for = sleep_for
        self.sleep_lightly_for = sleep_lightly_for

    async def setup(self):
        pass

    async def main(self):
        if self.sleep_for:
            await asyncio.sleep(self.sleep_for)
        if self.sleep_lightly_for:
            await self.sleep_lightly(self.sleep_lightly_for)
        if self.raise_error:
            raise self.raise_error

