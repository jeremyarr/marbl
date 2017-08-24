import asyncio
import argparse

import mooq

from marbl import Marbl, add_standard_options, create_logger

class Dummy(Marbl):
    def __init__(self, *, conn, logger, raise_error=None, 
            sleep_for=0.01, sleep_lightly_for=0,
            show=False):

        self._conn = conn
        self._logger = logger
        self._raise_error = raise_error
        self._sleep_for = sleep_for
        self._sleep_lightly_for = sleep_lightly_for
        self._show =show

    async def setup(self):
        pass

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
    conn = await mooq.connect(host=args.host, port=args.port, broker=args.broker)
    logger = create_logger(app_name=args.app_name, marbl_name=args.marbl_name)
    if args.raise_error:
        e = eval(args.raise_error)
    else:
        e = None

    marbl_obj = Dummy(
                    conn=conn,
                    logger=logger,
                    sleep_for=args.sleep_for,
                    sleep_lightly_for=args.sleep_lightly_for,
                    raise_error=e,
                    show=args.show
                )

    await marbl_obj.setup()

    await marbl_obj.run(interval=args.interval, num_cycles=args.num_cycles)

def cli():
    parser = argparse.ArgumentParser(description="a marbl that comes with all the standard marbls")
    parser.add_argument("-sleep_for", default=0.01, type=float, help="number of seconds to asyncio.sleep for")
    parser.add_argument("-sleep_lightly_for",default=0,type=float, help="number of seconds to asyncio.sleep_lightly for")
    parser.add_argument("-raise_error", help="error to raise after sleeping completed")
    parser.add_argument("--show", action="store_true", help="do not print to stdout")

    add_standard_options(parser, default_marbl_name="dummy")

    args = parser.parse_args()
    print(args)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))



if __name__ == "__main__":
    cli()

