import unittest
from unittest.mock import Mock, patch
import asyncio

import xmlrunner

from test.context import marbl
from test import common
import younit






if __name__ == '__main__':
    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output='test-reports'),
        # these make sure that some options that are not applicable
        # remain hidden from the help menu.
        failfast=False, buffer=False, catchbreak=False)