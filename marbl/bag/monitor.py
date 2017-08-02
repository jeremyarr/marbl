from ..base import Marbl
import asyncio

class Monitor(Marbl):
    def __init__(self, *, marbl_list, action="stop"):
        self._marbl_list = marbl_list
        assert action in ["stop","trigger"]
        self._action = action

    async def setup(self):
        pass

    async def main(self):
        take_action = False
        for m in self._marbl_list:
            if m.is_triggered():
                take_action = True
                break

        if take_action:
            if self._action=="stop":
                await asyncio.gather(*[m.stop() for m in self._marbl_list])
            else:
                [setattr(m, "trigger", True) for m in self._marbl_list]


    async def pre_run(self):
        pass

    async def post_run(self):
        pass
