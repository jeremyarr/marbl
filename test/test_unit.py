import unittest
from unittest.mock import Mock, patch
import asyncio

import xmlrunner

from test.context import marbl
from test import common
import younit
import time
import mooq
import os
import time

# @unittest.skip("skipped") 
class DummyTest(common.MarblTestCase):

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_running(self):
        await self.GIVEN_MarblSetup(
                marbl.bag.Dummy()
              )

        await self.WHEN_MarblRunInBackground(num_cycles=2, interval=1)
        self.THEN_MarblIsRunning()

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_not_running_after_finished_running(self):
        await self.GIVEN_MarblSetup(
                marbl.bag.Dummy()
              )

        await self.WHEN_MarblRunOnceNTimes(1)

        self.THEN_MarblIsNotRunning()

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_not_running(self):
        await self.GIVEN_MarblSetup(
                marbl.bag.Dummy()
              )

        self.THEN_MarblIsNotRunning()

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_not_running_if_error_occurs_when_running_once(self):
        await self.GIVEN_MarblSetup(
                marbl.bag.Dummy(raise_error=ValueError)
              )

        with self.assertRaises(ValueError):
            await self.WHEN_MarblRunOnceNTimes(1)

        self.THEN_MarblIsNotRunning()

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_not_running_if_error_occurs_when_running(self):
        await self.GIVEN_MarblSetup(
                marbl.bag.Dummy(raise_error=ValueError)
              )

        with self.assertRaises(ValueError):
            await self.WHEN_MarblRunInForeground(num_cycles=2, interval=0.2)

        self.THEN_MarblIsNotRunning()

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_version(self):
        await self.GIVEN_MarblSetup(
                marbl.bag.Dummy()
              )

        self.THEN_Version("not_set")

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_stop_timeout(self):
        await self.GIVEN_MarblSetup(
                marbl.bag.Dummy(sleep_for=0.1)
              )

        await self.WHEN_MarblRunInBackground(num_cycles=1, interval=0)

        with self.assertRaises(marbl.StopTimeout):
            await self.StopMarbl(timeout=0.02)


    def THEN_MarblIsNotRunning(self):
        self.assertFalse(self.marbl_obj.is_running())

    def THEN_MarblIsRunning(self):
        self.assertTrue(self.marbl_obj.is_running())

    def THEN_Version(self,x):
        self.assertEqual(x, self.marbl_obj.version)




# @unittest.skip("skipped") 
class SleepLightlyTest(common.MarblTestCase):

    async def async_setUp(self):
        await super().async_setUp()
        self.total = 0

        def count_total_sleep(interval):
            self.total = self.total + interval

        self.m = younit.AsyncMock(side_effect=count_total_sleep)

        self.sleep_patcher = patch('asyncio.sleep',new=self.m)
        self.mock_sleep = self.sleep_patcher.start()

    async def async_tearDown(self):
        await super().async_tearDown()
        self.sleep_patcher.stop()

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_sleep_lightly(self):
        await self.GIVEN_MarblSetup(
                marbl.bag.Dummy()
              )

        await self.WHEN_SleepLightly(13.235)

        self.THEN_AsyncioSleepCalledNTimes(133)
        self.THEN_AsyncioSleepTotalTime(13.235)


    async def WHEN_SleepLightly(self, interval):
        await self.marbl_obj.sleep_lightly(interval)

    def THEN_AsyncioSleepCalledNTimes(self,n):
        self.assertEqual(n, self.m.mock.call_count)

    def THEN_AsyncioSleepTotalTime(self,n):
        self.assertAlmostEqual(n, self.total,places=3)








# @unittest.skip("skipped") 
class HeartbeatTest(common.MarblTestCase):

    async def async_setUp(self):
        await super().async_setUp()

        await self.GIVEN_ConsumerRegisteredOnNewChannel(
                queue_name="heartbeat_consumer_queue",
                exchange_name="heartbeat",
                exchange_type="direct",
                routing_keys=[""],
                callback = self.callback_spy
              )
        await self.GIVEN_MarblSetup(
                marbl.bag.Heartbeat(
                    conn=self.conn,
                    parent_name="fake_parent",
                    parent_version="fake_version"
                )
              )


    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_publishes_heartbeat(self):
        await self.GIVEN_MarblRunOnceNTimes(1)

        await self.WHEN_ProcessEventsNTimes(20)

        self.THEN_CallbackCalledNTimes(1)


    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_publishes_heartbeat_twice_in_correct_format(self):
        await self.GIVEN_MarblRunOnceNTimes(2)

        await self.WHEN_ProcessEventsNTimes(20)

        self.THEN_CallbackCalledNTimes(2)
        self.THEN_LastCallbackMessageIs(
            {"tick":2,
            "version":"fake_version",
            "name":"fake_parent",
            "pid":os.getpid(),
            }
        )

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_heartbeat_correct_format(self):
        await self.GIVEN_MarblRunOnceNTimes(1)

        await self.WHEN_ProcessEventsNTimes(20)

        self.THEN_LastCallbackMessageIs(
            {"tick":1,
            "version":"fake_version",
            "name":"fake_parent",
            "pid":os.getpid(),
            }
        )


# @unittest.skip("skipped") 
class MessageProcessorTest(common.MarblTestCase):

    async def async_setUp(self):
        await super().async_setUp()
        await self.GIVEN_ProducerRegisteredOnNewChannel(
            exchange_name="fake_exch",
            exchange_type="direct",
            chan_name="chan1"
            )
        await self.GIVEN_ConsumerRegisteredOnNewChannel(
                queue_name="fake_queue",
                exchange_name="fake_exch",
                exchange_type="direct",
                routing_keys=["fake_routing_key"],
                callback = self.callback_spy,
                chan_name="chan2"
              )
        await self.GIVEN_MarblSetup(
                marbl.bag.MessageProcessor(conn=self.conn)
              )




    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_processes_a_message(self):
        await self.GIVEN_PublishMessage(
                exchange_name="fake_exch",
                msg="fake message",
                routing_key="fake_routing_key",
                chan_name="chan1"
              )

        await self.WHEN_MarblRunOnceNTimes(1)

        self.THEN_CallbackCalledNTimes(1)
        self.THEN_LastCallbackMessageIs("fake message")

# @unittest.skip("skipped") 
class LogProcessorTest(common.MarblTestCase):

    async def async_setUp(self):
        await super().async_setUp()
        await self.GIVEN_ConsumerRegisteredOnNewChannel(
                queue_name="fake_queue",
                exchange_name="log",
                exchange_type="topic",
                routing_keys=["*.*.*.*"],
                callback = self.callback_spy,
              )
        await self.GIVEN_MarblSetup(
                marbl.bag.LogProcessor(conn=self.conn, marbl_name="fake_micro")
              )

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_logs_an_error_message(self):
        await self.GWT_LogsMessage("error")

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_logs_a_debug_message(self):
        await self.GWT_LogsMessage("debug")

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_logs_an_info_message(self):
        await self.GWT_LogsMessage("info")

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_logs_a_critical_message(self):
        await self.GWT_LogsMessage("critical")

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_logs_a_warning_message(self):
        await self.GWT_LogsMessage("warning")


    def GIVEN_Log(self, severity, msg):
        l = getattr(self.marbl_obj.logger,severity)
        l(msg)

    async def GWT_LogsMessage(self,severity):
        self.GIVEN_Log(severity,"fake_msg")
        await self.GIVEN_MarblRunOnceNTimes(1)

        await self.WHEN_ProcessEventsNTimes(20)

        self.THEN_CallbackCalledNTimes(1)
        self.THEN_LastCallbackMessageRegexIs(".*: {}: fake_micro: marbl \({}\): fake_msg".format(severity.upper(), os.getpid()))
        self.THEN_LastCallbackRoutingKeyIs("fake_micro.marbl.{}.{}".format(os.getpid(), severity))


# @unittest.skip("skipped") 
class MonitorTriggererTest(common.MarblTestCase):

    async def async_setUp(self):
        await super().async_setUp()

        await self.GIVEN_NMarblsSetup(
            n=10, marbl_cls=marbl.bag.Dummy)

        await self.GIVEN_MarblSetup(
                marbl.bag.Monitor(marbl_list=self.marbl_list, action="trigger")
              )

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_trigger_one_triggers_all(self):
        self.GIVEN_TriggerIthMarblInList(4)
        await self.WHEN_MarblRunOnceNTimes(1)
        self.THEN_AllMarblsAreTriggered()

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_no_triggers(self):
        await self.WHEN_MarblRunOnceNTimes(1)
        self.THEN_AllMarblsAreNotTriggered()



# @unittest.skip("skipped") 
class MonitorStopperTest(common.MarblTestCase):

    async def async_setUp(self):
        await super().async_setUp()

        await self.GIVEN_NMarblsSetup(
            n=10, marbl_cls=marbl.bag.Dummy, 
            sleep_for=0,
            sleep_lightly_for=5)

        await self.GIVEN_MarblSetup(
                marbl.bag.Monitor(marbl_list=self.marbl_list)
              )
        await self.GIVEN_AllMarblsAreRunningInBackground(num_cycles=2, interval=2)


    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_trigger_one_stops_all(self):
        self.GIVEN_TriggerIthMarblInList(4)
        await self.WHEN_MarblRunOnceNTimes(1)
        self.THEN_AllMarblsAreNotRunning()

    # @unittest.skip("skipped")
    @younit.asyncio_test
    async def test_no_triggers_no_stops(self):
        await self.WHEN_MarblRunOnceNTimes(1)
        self.THEN_AllMarblsAreRunning()







if __name__ == '__main__':
    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output='test-reports'),
        # these make sure that some options that are not applicable
        # remain hidden from the help menu.
        failfast=False, buffer=False, catchbreak=False)