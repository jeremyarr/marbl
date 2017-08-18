import asyncio
import argparse
import datetime
import os

import mooq

from marbl import Marbl, add_standard_options, create_logger
from marbl.bag import Dummy

class Alive(object):
    def __init__(self, hb):
        self.tick = hb['tick']
        self.version = hb['version']
        self.name = hb['name']
        self.pid = hb['pid']
        self.started = hb['started']
        self.last_heartbeat = datetime.datetime.utcnow()

    def update(self):
        self.last_heartbeat = datetime.datetime.utcnow()

    def serialise(self):
        return {
                    "tick":self.tick,
                    "version":self.version,
                    "name":self.name,
                    "pid":self.pid,
                    "started":self.started,
                    "last_heartbeat":self.last_heartbeat.isoformat(),
                }

class Supervisor(Marbl):
    def __init__(self, *, conn, logger, app_name="marbl"):
        self._conn = conn
        self.marbl_name = "supervisor"
        self._app_name = app_name
        self.alive = []
        self._logger = logger


    def register_standard_marbls(self):
        return [
            self.register_receiver(),
            self.register_logger(),
            self.register_ticker(),
            ]



    async def setup(self):
        self._chan = await self._conn.create_channel()

        await self._chan.register_consumer(exchange_name="heartbeat",
                exchange_type="direct", routing_keys=[""],
                callback=self.process_heartbeats,
                create_task_meth=self.create_task)

        await self._chan.register_producer(exchange_name="supervisor",
                exchange_type="topic")

        await self._chan.register_consumer(exchange_name="supervisor",
                exchange_type="topic", routing_keys=["supervisor_request"],
                callback=self.process_supervisor_msg,
                create_task_meth=self.create_task)

    async def pre_run(self):
        self.create_task(self.clean_heartbeat_list())

    async def main(self):
        pass

    async def process_heartbeats(self, resp):
        a = self.get_by_pid(resp['msg']['pid'])
        my_pid = os.getpid()
        if a and a.name == "supervisor" and a.pid != my_pid:
            me = self.get_by_pid(my_pid)
            if me.started < a.started:
                self._logger.error("another more recent supervisor detected as running")
                await asyncio.sleep(0.5)
                self.trigger.set_()
                return

        if a:
            a.update()
        else:
            new=Alive(resp['msg'])
            self._logger.info("new marbl detected ({})".format(new.pid))
            self.alive.append(new)

    async def clean_heartbeat_list(self):
        while True:
            now = datetime.datetime.utcnow()
            for a in self.alive:
                if now - a.last_heartbeat > datetime.timedelta(seconds=5):
                    self._logger.info("marbl with pid {} no longer alive".format(a.pid))
                    self.alive.remove(a)

            await self.sleep_lightly(0.5)
            if self.trigger:
                break

    def get_by_pid(self, pid):
        for a in self.alive:
            if a.pid == pid:
                return a

        return None

    async def process_supervisor_msg(self, resp):
        self._logger.debug("request {}".format(resp))
        if resp['msg'][0]=="stop_supervisor":
            msg = {"status_code:":200, "data":"stopping supervisor"}
            await self.publish_response(msg)
            self.trigger.set_as_remote_stop()
        elif resp['msg'][0]=="stop_all":
            msg = {"status_code:":200, "data":"stopping all"}
            await self.publish_response(msg)
            await self.publish_to_marbls(["stop"], "all.all.all")
        elif resp['msg'][0]=="list":
            msg = {"status_code:":200, "data":[a.serialise() for a in self.alive]}
            await self.publish_response(msg)
        else:
            msg = {"status_code:":400, "data":"invalid_command"}
            await self.publish_response(msg)

    async def publish_response(self,msg):
        await self._chan.publish(
                exchange_name="supervisor",
                msg=msg,
                routing_key="supervisor_response" )

    async def publish_to_marbls(self, msg, routing_key):
        await self._chan.publish(
                exchange_name="supervisor",
                msg=msg,
                routing_key=routing_key)


async def main(args):
    conn = await mooq.connect(host=args.host, port=args.port, broker=args.broker)
    logger = create_logger(app_name=args.app_name, marbl_name=args.marbl_name)
    marbl_obj = Supervisor(
                    conn=conn,
                    app_name=args.app_name,
                    logger=logger
                )

    await marbl_obj.setup()
    await marbl_obj.run(interval=args.interval, num_cycles=args.num_cycles)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="supervisor of marbls")
    add_standard_options(parser, default_marbl_name="supervisor")

    args = parser.parse_args()

    print(args)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))















