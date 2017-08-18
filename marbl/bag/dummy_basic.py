import asyncio
import argparse

from marbl import Marbl


class DummyBasic(Marbl):
    def __init__(self, raise_error=None, 
            sleep_for=0.01, sleep_lightly_for=0,
            show=False):

        self._raise_error = raise_error
        self._sleep_for = sleep_for
        self._sleep_lightly_for = sleep_lightly_for
        self._show =show

    async def setup(self):
        pass

    def register_standard_marbls(self):
        return []

    async def main(self):
        if self._show:
            print("dummy cycle")
        if self._sleep_for:
            await asyncio.sleep(self._sleep_for)
        if self._sleep_lightly_for:
            await self.sleep_lightly(self._sleep_lightly_for)

        if self._raise_error:
            raise self._raise_error


async def main(args):
    if args.raise_error:
        e = eval(args.raise_error)
    else:
        e = None

    marbl_obj = DummyBasic(
                    sleep_for=args.sleep_for,
                    sleep_lightly_for=args.sleep_lightly_for,
                    raise_error=e,
                    show=(not args.no_show),
                )

    await marbl_obj.setup()
    await marbl_obj.run(interval=args.interval, num_cycles=args.num_cycles)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="a placeholder marbl")
    parser.add_argument("-sleep_for", default=0.01, type=float, help="number of seconds to asyncio.sleep for")
    parser.add_argument("-sleep_lightly_for",default=0,type=float, help="number of seconds to asyncio.sleep_lightly for")
    parser.add_argument("-raise_error", help="error to raise after sleeping completed")

    parser.add_argument("-interval", type=float, default=1, help="delay between cycles")
    parser.add_argument("-num_cycles",type=int, default=None, help="number of cycles to run for")

    parser.add_argument("--no_show", action="store_true", help="do not print to stdout")

    args = parser.parse_args()

    print(args)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
