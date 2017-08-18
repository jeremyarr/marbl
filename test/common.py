import unittest
import re
import asyncio

import marbl
from marbl import g
import mooq

# @unittest.skip("skipped")
class MarblTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    async def async_setUp(self):
        await self.GIVEN_InMemoryBrokerStarted("localhost",1234)
        await self.GIVEN_ConnectionToBroker("localhost",1234,"in_memory")

    async def async_tearDown(self):
        await self.StopMarbl()
        await self.CloseBroker()
        await self.StopMarblsInList()



    def setUp(self):
        self.callback_count = 0
        self.callback_msg = ''
        self.callback_routing_key = ''
        self.marbl_list = []
        g.trigger.clear()

    def tearDown(self):
        pass

    def GIVEN_this(self):
        pass

    def WHEN_that(self):
        pass

    def THEN_verify(self):
        pass

    async def GIVEN_InMemoryBrokerStarted(self,host,port):
        self.broker = mooq.InMemoryBroker(host=host,port=port)
        _, launched = marbl.create_task(self.broker.run())
        await launched

    async def CloseBroker(self):
        await self.broker.close()

    async def GIVEN_ConnectionToBroker(self,host, port, broker):
        self.conn = await mooq.connect(broker=broker,
                                       host=host,
                                       port=port)

    async def GIVEN_ConsumerRegisteredOnNewChannel(self,*,queue_name,exchange_name,
                exchange_type, routing_keys, callback, chan_name="chan"):


        chan = await self.conn.create_channel()
        setattr(self,chan_name,chan)

        await chan.register_consumer( queue_name=queue_name,
                exchange_name=exchange_name,
                exchange_type=exchange_type,
                routing_keys=routing_keys,
                callback = callback
              )

    async def GIVEN_MarblSetup(self, m, name="marbl_obj"):

        setattr(self, name, m)
        marbl_obj = getattr(self,name)
        await marbl_obj.setup()

    async def GIVEN_MarblRunOnceNTimes(self,n, name="marbl_obj"):
        marbl_obj = getattr(self, name)
        for i in range(n):
            await marbl_obj.run_once(show_errors=False)

    async def WHEN_MarblRunOnceNTimes(self,*args,**kwargs):
        await self.GIVEN_MarblRunOnceNTimes(*args, **kwargs)

    async def WHEN_ProcessEventsNTimes(self,n):
        await self.conn.process_events(num_cycles=n)

    def THEN_CallbackCalledNTimes(self,n):
        self.assertEqual(n, self.callback_count)

    def THEN_LastCallbackMessageIs(self,expected):
        self.assertEqual(expected,self.callback_msg)

    def THEN_LastCallbackMessageRegexIs(self,regexp):
        self.assertRegex(self.callback_msg, regexp )

    def THEN_LastCallbackRoutingKeyIs(self, expected):
        self.assertEqual(expected, self.callback_routing_key)

    async def callback_spy(self,resp):
        self.callback_count = self.callback_count + 1
        self.callback_msg = resp['msg']
        self.callback_routing_key = resp['routing_key']

    async def GIVEN_ProducerRegisteredOnNewChannel(self, 
                exchange_name, exchange_type, chan_name="chan"):

        chan = await self.conn.create_channel()
        setattr(self, chan_name, chan)

        await chan.register_producer( exchange_name=exchange_name,
                                      exchange_type=exchange_type)

    async def GIVEN_PublishMessage(self, *, 
                exchange_name, msg, routing_key, chan_name="chan"):

        chan = getattr(self, chan_name)
        await chan.publish(msg=msg,exchange_name=exchange_name, 
                routing_key=routing_key)

    async def WHEN_MarblRunInBackground(self,*, num_cycles, interval, name="marbl_obj"):
        marbl_obj = getattr(self, name)
        _, launched = marbl_obj.create_task(
                        marbl_obj.run(num_cycles=num_cycles,interval=interval),
                        show_errors=False
                      )
        await launched

    async def GIVEN_MarblRunInBackground(self,*args,**kwargs):
        await self.WHEN_MarblRunInBackground(*args, **kwargs)

    async def WHEN_MarblRunInForeground(self,*, num_cycles, interval, name="marbl_obj"):
        marbl_obj = getattr(self, name)
        await marbl_obj.run(num_cycles=num_cycles,interval=interval, show_errors=False)

    async def StopMarbl(self, timeout=1, name="marbl_obj"):
        
        try:
            marbl_obj = getattr(self, name)
            await marbl_obj.stop(timeout=timeout)
        except AttributeError:
            print("WARNINGG object has no attribute {}".format(name))

    async def GIVEN_NMarblsSetup(self,*, n, marbl_cls,  **kwargs):
        for i in range(n):
            m = marbl_cls(**kwargs)
            self.marbl_list.append(m)



    async def GIVEN_AllMarblsAreRunningInBackground(self,*,num_cycles, interval):
        for m in self.marbl_list:
            _, launched = m.create_task(
                            m.run(num_cycles=num_cycles,interval=interval),
                            show_errors=False
                          )
            await launched

    def THEN_AllMarblsAreNotRunning(self):
        [self.assertFalse(m.is_running()) for m in self.marbl_list]

    def THEN_AllMarblsAreRunning(self):
        [self.assertTrue(m.is_running()) for m in self.marbl_list]

    def THEN_AllMarblsAreRunningExcept(self,n):
        for i, m in enumerate(self.marbl_list):
            if i != n:
                self.assertTrue(m.is_running())



    async def StopMarblsInList(self, timeout=1):
        for m in self.marbl_list:
            await m.stop(timeout=timeout)

    def THEN_Triggered(self):
        self.assertTrue(g.trigger)

    def THEN_NotTriggered(self):
        self.assertFalse(g.trigger)







    def THEN_MarblIsNotRunning(self, name="marbl_obj"):
        marbl_obj = getattr(self, name)
        self.assertFalse(marbl_obj.is_running())

    def THEN_MarblIsRunning(self, name="marbl_obj"):
        marbl_obj = getattr(self, name)
        self.assertTrue(marbl_obj.is_running())

    def GIVEN_TriggerAsStop(self):
        g.trigger.set_as_stop()

    def GIVEN_TriggerAsRemoteStop(self):
        g.trigger.set_as_remote_stop()

    def GIVEN_TriggerAsError(self, exc_obj=ValueError(), tb_str="fake_tb_str"):
        g.trigger.set_as_error(exc_obj, tb_str)

    def WHEN_TriggerAsError(self,*args,**kwargs):
        self.GIVEN_TriggerAsError(*args, **kwargs)

    async def WHEN_MarblRunOnceInBackground(self, name="marbl_obj"):
        marbl_obj = getattr(self, name)
        _, launched = marbl_obj.create_task(
                        marbl_obj.run_once(), show_errors=False
                      )
        await launched

    def THEN_TriggeredWithCause(self,cause,*, error_type=None):
        self.assertTrue(g.trigger)
        self.assertEqual(cause, g.trigger.cause)
        self.assertEqual(error_type, g.trigger.error_type)
        if cause=="error":
            self.assertIsNotNone(g.trigger.error_traceback)
        else:
            self.assertIsNone(g.trigger.error_traceback)


    async def GIVEN_StopIthMarbl(self,i,timeout=1):
        await self.marbl_list[i].stop(timeout=timeout)

    async def GIVEN_SendRemoteStopCommandWithRoutingKey(self, routing_key):
        await self.GIVEN_ProducerRegisteredOnNewChannel( 
                        exchange_name="supervisor", exchange_type="topic")

        await self.GIVEN_PublishMessage(exchange_name="supervisor", msg=["stop"], 
                routing_key=routing_key)

    def THEN_LastMarblCtlResponseCode(self, expected, name="marbl_obj"):
        marbl_obj = getattr(self, name)
        self.assertEqual(expected, marbl_obj.last_response[0])