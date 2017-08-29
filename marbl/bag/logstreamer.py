import argparse
import asyncio
import os

from marbl import Marbl, add_std_non_logging_options
import mooq

VALID_SEVERITIES = ["debug","info","warning","error","critical"]

class LogStreamer(Marbl):
    def __init__(self, *, conn, marbl_name="*", app_name="*", pid="*", lowest_severity="*", 
        hide_date=False, hide_severity=False, hide_marbl=False, hide_app=False):

        self._conn = conn
        self._marbl_name = marbl_name
        self._app_name = app_name
        self._pid = pid
        self._lowest_severity = lowest_severity
        self._hide_date = hide_date
        self._hide_severity = hide_severity
        self._hide_marbl = hide_marbl
        self._hide_app = hide_app

    async def setup(self):
        #marbl_name.app_name.pid.severity
        self._chan = await self._conn.create_channel()
        routing_keys = self.generate_topics()
        print(routing_keys)

        await self._chan.register_consumer(exchange_name="log",
                exchange_type="topic", routing_keys=routing_keys,
                callback=self.take_action,
                create_task_meth=self.create_task)
        
    async def main(self):
        pass

    async def take_action(self, resp):
        msg = resp['msg']
        splitted_msg = msg.split(":")
        fields_to_remove = []

        if self._hide_date:
            fields_to_remove.append(0)
            fields_to_remove.append(1)
            fields_to_remove.append(2)
        if self._hide_severity:
            fields_to_remove.append(3)
        if self._hide_marbl:
            fields_to_remove.append(4)
        if self._hide_app:
            fields_to_remove.append(5)

        for i in sorted(fields_to_remove,reverse=True):
            del splitted_msg[i]

        out = ":".join(splitted_msg)
        print("{}".format(out))
        # print(resp['msg'])


    def generate_topics(self):
        #in order of level
        topic_without_severity = "{}.{}.{}".format(self._marbl_name, self._app_name, self._pid)
        if self._lowest_severity == "*":
            topic = "{}.*".format(topic_without_severity)
            return [topic]


        i = VALID_SEVERITIES.index(self._lowest_severity)

        filtered_severities = VALID_SEVERITIES[i:]

        topic_list = []
        for s in filtered_severities:
            topic = "{}.{}".format(topic_without_severity,s)
            topic_list.append(topic)

        return topic_list



async def main(args):
    conn = await mooq.connect(host=args.host, port=args.port, broker=args.broker, virtual_host=args.virtual_host)
    if args.hide_all:
        args.hide_date = True
        args.hide_severity = True
        args.hide_marbl = True
        args.hide_app = True


    marbl_obj = LogStreamer(
                    conn=conn,
                    marbl_name=args.marbl_name,
                    app_name=args.app_name, 
                    pid=args.pid, 
                    lowest_severity=args.severity, 
                    hide_date=args.hide_date, 
                    hide_severity=args.hide_severity, 
                    hide_marbl=args.hide_marbl, 
                    hide_app=args.hide_app
                )

    await marbl_obj.setup()
    await marbl_obj.run(interval=args.interval, num_cycles=args.num_cycles)

def cli():
    parser = argparse.ArgumentParser(description="responds to remote control commands")
    parser.add_argument("--marbl_name", default="*", help="name of the marbl to view logs of")
    parser.add_argument("--app_name",default="*",
                        help="name of the app to view logs of")
    parser.add_argument("--pid",default="*",
                        help="process id to view logs of")
    parser.add_argument("--severity",
                        default="*",choices=VALID_SEVERITIES,
                        help="minimum severity level to view logs of")
    parser.add_argument("--hide_date", action="store_true",
                        help="hide date field")
    parser.add_argument("--hide_severity", action="store_true",
                        help="hide severity field")
    parser.add_argument("--hide_marbl", action="store_true",
                        help="hide marbl field")
    parser.add_argument("--hide_app", action="store_true",
                        help="hide app field")
    parser.add_argument("--hide_all", action="store_true",
                        help="hide all fields")
    add_std_non_logging_options(parser)

    args = parser.parse_args()

    print(args)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))




if __name__ == "__main__":
    cli()