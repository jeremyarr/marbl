import argparse
import asyncio
import os

from marbl import Marbl, add_standard_options, create_logger
import mooq


class EchoConsumer(Marbl):
    def __init__(self, *, conn, logger, exchange_name, exchange_type, routing_keys):

        self._conn = conn
        self._logger = logger
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._routing_keys = routing_keys


    async def setup(self):
        self._chan = await self._conn.create_channel()

        await self._chan.register_consumer(exchange_name=self._exchange_name,
                exchange_type=self._exchange_type, routing_keys=self._routing_keys,
                callback=self.echo,
                create_task_meth=self.create_task)
        
    async def main(self):
        pass

    async def echo(self, resp):
        msg = resp['msg']
        print("echo: {}".format(msg))
        self._logger.debug("echo: {}".format(msg))





async def main(args):
    conn = await mooq.connect(host=args.host, port=args.port, broker=args.broker)
    logger = create_logger(app_name=args.app_name, marbl_name=args.marbl_name)

    routing_keys = [args.routing_key]

    marbl_obj = EchoConsumer(
                    conn=conn,
                    logger=logger,
                    exchange_name=args.exchange_name, 
                    exchange_type=args.exchange_type, 
                    routing_keys=routing_keys
                )

    await marbl_obj.setup()
    await marbl_obj.run(interval=args.interval, num_cycles=args.num_cycles)

def cli():
    parser = argparse.ArgumentParser(description="echo messages consumed")
    parser.add_argument("exchange_name", help="name of the exchange to consume")
    parser.add_argument("routing_key", help="routing key to consume (only one allowed at the moment)")
    parser.add_argument("exchange_type",choices=["direct", "topic", "fanout"], help="exchange type")
    add_standard_options(parser, default_marbl_name="echoconsumer")

    args = parser.parse_args()

    print(args)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))




if __name__ == "__main__":
    cli()