#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

import uuid

from zope import component

from nti.appserver.tests.test_application import SharedApplicationTestBase
from nti.appserver.tests.test_application import WithSharedApplicationMockDS
from nti.appserver.tests.test_application import WithSharedApplicationMockDSWithChanges
from nti.appserver.tests.test_application import WithSharedApplicationMockDSHandleChanges

from nti.dataserver import interfaces as nti_interfaces

from nti.hypatia import interfaces as hypatia_interfaces
from nti.hypatia.adapters import _HypatiaUserIndexController

from nti.dataserver.tests.mock_dataserver import SharedConfiguringTestBase as \
                                                 DSSharedConfiguringTestBase

zanpakuto_commands = (
    "Shoot To Kill",
    "Bloom, Split and Deviate",
    "Rankle the Seas and the Skies",
    "Lightning Flash Flame Shell",
    "Flower Wind Rage and Flower God Roar, Heavenly Wind Rage and Heavenly Demon Sneer",
    "All Waves, Rise now and Become my Shield, Lightning, Strike now and Become my Blade",
    "Cry, Raise Your Head, Rain Without end",
    "Sting All Enemies To Death",
    "Reduce All Creation to Ash",
    "Sit Upon the Frozen Heavens",
    "Call forth the Twilight",
    "Multiplication and subtraction of fire and ice, show your might")

def register():
    try:
        # temp hack to register adapter while the zcml condition zopyx_index is
        # deprecated
        component.provideAdapter(_HypatiaUserIndexController, (nti_interfaces.IUser,),
                                 hypatia_interfaces.IHypatiaUserIndexController)
    except:
        pass

class ConfiguringTestBase(DSSharedConfiguringTestBase):
    set_up_packages = ('nti.dataserver', 'nti.hypatia', 'nti.contentsearch')

    def setUp(self):
        super(ConfiguringTestBase, self).setUp()
        register()
   
class ApplicationTestBase(SharedApplicationTestBase):
    features = SharedApplicationTestBase.features + ('forums',)

    def setUp(self):
        super(ApplicationTestBase, self).setUp()
        register()
