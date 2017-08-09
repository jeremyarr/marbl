from marbl import Marbl
import os

class RemoteControl(Marbl):
    def __init__(self, *, conn, marbl_name):
        self._conn = conn
        self._marbl_name = marbl_name

    async def setup(self):
        self._chan = await self._conn.create_channel()
        routing_keys =[ 
                        "{}.all.all".format(self._marbl_name), #all marbl_name micros (any version)
                        "{}.all.{}".format(self._marbl_name,self.version), #all marbl_name micros (with version version)
                        "all.all.all", #all micros (any version)
                        "all.{}.all".format(os.getpid()),#any micro with pid pid
                      ]

        await self._chan.register_consumer(exchange_name="supervisor",
                exchange_type="topic", queue_name="blah", routing_keys=routing_keys,
                callback=self.take_action)

    async def main(self):
        raise NotImplementedError

    async def take_action(self, resp):
        self.trigger.set_as_remote_stop()


