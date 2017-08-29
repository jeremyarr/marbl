import asyncio
import argparse

import mooq

from marbl import Marbl, add_standard_options

class MarblCtl(Marbl):
    def __init__(self, *, conn, msg):
        self._conn = conn
        self.msg = msg
        self.last_response = []

    async def setup(self):
        self._chan = await self._conn.create_channel()
        await self._chan.register_producer(exchange_name="supervisor",
                exchange_type="topic")

        await self._chan.register_consumer( exchange_name="supervisor",
            exchange_type="topic",
            routing_keys=["supervisor_response"],
            callback = self.process_response,
            create_task_meth=self.create_task
          )

    async def main(self):
        await self._chan.publish(
                exchange_name="supervisor",
                msg=self.msg,
                routing_key="supervisor_request" )
        print("published supervisor request!")

        await self.sleep_lightly(2)

        if not self.trigger:
            print("timeout error raised")
            raise TimeoutError

    async def process_response(self,resp):
        print("processing response")
        self.last_response = resp['msg']
        print(resp['msg'])

        self.trigger.set_as_remote_stop()

async def main(args):
    conn = await mooq.connect(host=args.host, port=args.port, broker=args.broker, virtual_host=args.virtual_host)

    if args.stop_supervisor:
        msg = ["stop_supervisor"]
    elif args.stop_all:
        msg = ["stop_all"]
    elif args.list:
        msg = ["list"]
    else:
        print("valid command not received")
        return

    marbl_obj = MarblCtl(
                    conn=conn,
                    msg=msg,
                )

    await marbl_obj.setup()
    await marbl_obj.run(interval=args.interval, num_cycles=args.num_cycles)

def cli():
    parser = argparse.ArgumentParser(description="control and monitor marbls via the supervisor")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--stop_supervisor",action="store_true",help="stop the supervisor")
    group.add_argument("--stop_all",action="store_true",help="stop all marbls except the supervisor")
    group.add_argument("--list",action="store_true",help="list all running marbls")

    add_standard_options(parser)

    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))


if __name__ == "__main__":
    cli()
