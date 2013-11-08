#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
graphdb module

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component
from zope.component.interfaces import ComponentLookupError

from pyramid.threadlocal import get_current_request

from . import interfaces as pio_interfaces

def get_possible_site_names(request=None, include_default=True):
	request = request or get_current_request()
	if not request:
		return () if not include_default else ('',)
	__traceback_info__ = request

	site_names = getattr(request, 'possible_site_names', ())
	if include_default:
		site_names += ('',)
	return site_names

def get_predictionio_app(sites=(), request=None):
	sites = sites or get_possible_site_names(request=request)
	for site in sites:
		app = component.queryUtility(pio_interfaces.IPredictionIOApp, name=site)
		if app is not None:
			return app
	raise ComponentLookupError()
