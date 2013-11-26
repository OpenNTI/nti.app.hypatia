#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

import uuid

from nti.appserver.tests.test_application import SharedApplicationTestBase
from nti.appserver.tests.test_application import WithSharedApplicationMockDS
from nti.appserver.tests.test_application import WithSharedApplicationMockDSWithChanges

from nti.dataserver.tests.mock_dataserver import SharedConfiguringTestBase as \
                                                 DSSharedConfiguringTestBase

class ConfiguringTestBase(DSSharedConfiguringTestBase):
    set_up_packages = ('nti.dataserver', 'nti.hypatia', 'nti.contentsearch')

class ApplicationTestBase(SharedApplicationTestBase):
    pass
