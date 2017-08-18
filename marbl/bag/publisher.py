import os
import argparse
import asyncio

from marbl import Marbl, add_standard_options

import mooq

class Publisher(Marbl):
    def __init__(self, *, conn, exchange_name, exchange_type, msg, routing_key, show=True):
        self._conn = conn
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._msg = msg
        self._routing_key = routing_key
        self._show = show


    async def setup(self):
        self._chan = await self._conn.create_channel()
        await self._chan.register_producer(exchange_name=self._exchange_name,
                exchange_type=self._exchange_type)

    async def main(self):
        await self._chan.publish(
                exchange_name=self._exchange_name,
                msg=self._msg,
                routing_key=self._routing_key
        )
        if self._show:
            print( ("published '{}' to {} exchange"
                    " '{}' with routing key '{}'"
                    "").format(self._msg, self._exchange_type,
                    self._msg, self._routing_key))

async def main(args):
    conn = await mooq.connect(host=args.host, port=args.port, broker=args.broker)

    marbl_obj = Publisher(
                    conn=conn,
                    exchange_name=args.exchange_name,
                    exchange_type=args.exchange_type,
                    msg=args.msg,
                    routing_key=args.routing_key,
                )

    await marbl_obj.setup()
    await marbl_obj.run(interval=args.interval, num_cycles=args.num_cycles)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="a flexible message publisher")

    parser.add_argument("exchange_name", help="name of the exchange to publish to")
    parser.add_argument("msg", help="message to publish")
    parser.add_argument("routing_key", help="routing key of message")
    parser.add_argument("exchange_type",choices=["direct", "topic", "fanout"], help="exchange type")

    add_standard_options(parser)

    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))