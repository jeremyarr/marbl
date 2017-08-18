import asyncio
import argparse

import mooq

from marbl import Marbl, add_standard_options, create_logger


class Dummy(Marbl):
    def __init__(self, *, conn, logger):
        self._conn = conn
        self._logger = logger

    async def setup(self):
        pass

    async def main(self):
        await self.sleep_lightly(2)
        raise ValueError


async def main(args):
    conn = await mooq.connect(host=args.host, port=args.port, broker=args.broker)
    logger = create_logger(app_name=args.app_name, marbl_name=args.marbl_name)
    marbl_obj = Dummy(
                    conn=conn,
                    logger=logger,
                )

    await marbl_obj.setup()

    await marbl_obj.run(interval=args.interval, num_cycles=args.num_cycles)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="a marbl that comes with all the standard marbls")

    add_standard_options(parser, default_marbl_name="dummy")

    args = parser.parse_args()
    print(args)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
