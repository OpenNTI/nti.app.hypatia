#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

import uuid

import nti.dataserver as dataserver

import nti.hypatia as hypatia

from nti.appserver.tests.test_application import SharedApplicationTestBase
from nti.appserver.tests.test_application import WithSharedApplicationMockDS
from nti.appserver.tests.test_application import WithSharedApplicationMockDSWithChanges

from nti.dataserver.tests.mock_dataserver import SharedConfiguringTestBase as \
                                                 DSSharedConfiguringTestBase

class ConfiguringTestBase(DSSharedConfiguringTestBase):
    set_up_packages = (dataserver, hypatia,)

class ApplicationTestBase(SharedApplicationTestBase):
    pass
