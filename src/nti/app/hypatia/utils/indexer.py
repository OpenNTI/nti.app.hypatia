#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from nti.monkey import relstorage_patch_all_except_gevent_on_import
relstorage_patch_all_except_gevent_on_import.patch()

import os
import sys
import time
import signal
import argparse

import zope.exceptions
from zope.configuration import xmlconfig, config
from zope.dottedname import resolve as dottedname

from nti.dataserver.utils import run_with_dataserver

from nti.hypatia import reactor
from nti.hypatia import interfaces as hypatia_interfaces

MIN_INTERVAL = reactor.MIN_INTERVAL
MAX_INTERVAL = reactor.MAX_INTERVAL
DEFAULT_INTERVAL = reactor.DEFAULT_INTERVAL

def sigint_handler(*args):
	logger.info("Shutting down %s", os.getpid())
	sys.exit(0)

def handler(*args):
	raise SystemExit()

signal.signal(signal.SIGINT, sigint_handler)
signal.signal(signal.SIGTERM, handler)

def main():
	arg_parser = argparse.ArgumentParser(description="Index processor")
	arg_parser.add_argument('-v', '--verbose', help="Be verbose", action='store_true',
							 dest='verbose')
	arg_parser.add_argument('-d', '--devmode', help="Dev mode", action='store_true',
							 dest='devmode')
	arg_parser.add_argument('-m', '--mintime',
							 dest='mintime',
							 help="Min poll time interval (secs)",
							 type=int,
							 default=DEFAULT_INTERVAL)
	arg_parser.add_argument('-x', '--maxtime',
							 dest='maxtime',
							 help="Max poll time interval (secs)",
							 type=int,
							 default=DEFAULT_INTERVAL)
	arg_parser.add_argument('-l', '--limit',
							 dest='limit',
							 help="Queue limit",
							 type=int,
							 default=hypatia_interfaces.DEFAULT_QUEUE_LIMIT)
	arg_parser.add_argument('--redis', help="Use redis lock", action='store_true',
							 dest='redis')

	args = arg_parser.parse_args()
	env_dir = os.getenv('DATASERVER_DIR')
	if not env_dir or not os.path.exists(env_dir) and not os.path.isdir(env_dir):
		raise IOError("Invalid dataserver environment root directory")

	context = _create_context(env_dir, args.devmode)
	conf_packages = ('nti.appserver', 'nti.hypatia')
	run_with_dataserver(environment_dir=env_dir,
						xmlconfig_packages=conf_packages,
						verbose=args.verbose,
						context=context,
						minimal_ds=True,
						config_features=("devmode",),
						function=lambda: _process_args(args))

def _create_context(env_dir, devmode):
	env_dir = os.path.expanduser(env_dir)

	etc = os.getenv('DATASERVER_ETC_DIR') or os.path.join(env_dir, 'etc')
	etc = os.path.expanduser(etc)

	context = config.ConfigurationMachine()
	xmlconfig.registerCommonDirectives(context)

	if devmode:
		context.provideFeature("devmode")

	slugs = os.path.join(etc, 'package-includes')
	if os.path.exists(slugs) and os.path.isdir(slugs):
		package = dottedname.resolve('nti.dataserver')
		context = xmlconfig.file('configure.zcml', package=package, context=context)
		xmlconfig.include(context, files=os.path.join(slugs, '*.zcml'),
						  package='nti.appserver')

	return context

def _process_args(args):
	import logging

	mintime = args.mintime
	maxtime = args.maxtime
	assert mintime <= maxtime and mintime > 0

	if args.devmode:
		logging.root.setLevel(logging.DEBUG)

	limit = args.limit
	assert limit > 0

	mintime = max(min(mintime, MAX_INTERVAL), MIN_INTERVAL)
	maxtime = max(min(maxtime, MAX_INTERVAL), MIN_INTERVAL)

	ei = '%(asctime)s %(levelname)-5.5s [%(name)s][%(thread)d][%(threadName)s] %(message)s'
	logging.root.handlers[0].setFormatter(zope.exceptions.log.Formatter(ei))

	target = reactor.IndexReactor(mintime, maxtime, limit, args.redis)
	result = target(time.sleep)
	sys.exit(result)

if __name__ == '__main__':
	main()
