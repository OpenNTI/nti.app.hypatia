#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import os
import sys
import time
import signal
import logging
import argparse

import zope.exceptions

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
	arg_parser.add_argument('env_dir', help="Dataserver environment root directory")
	arg_parser.add_argument('-n', '--mintime',
							 dest='mintime',
							 help="Min poll time interval (secs)",
							 type=int,
							 default=DEFAULT_INTERVAL)
	arg_parser.add_argument('-m', '--maxtime',
							 dest='maxtime',
							 help="Max poll time interval (secs)",
							 type=int,
							 default=DEFAULT_INTERVAL)
	arg_parser.add_argument('-l', '--limit',
							 dest='limit',
							 help="Queue limit",
							 type=int,
							 default=hypatia_interfaces.DEFAULT_QUEUE_LIMIT)

	args = arg_parser.parse_args()
	env_dir = args.env_dir

	conf_packages = ('nti.appserver', 'nti.hypatia')
	run_with_dataserver(environment_dir=env_dir,
						xmlconfig_packages=conf_packages,
						verbose=args.verbose,
						function=lambda: _process_args(args))

def _process_args(args):
	mintime = args.mintime
	maxtime = args.maxtime
	assert mintime <= maxtime and mintime > 0

	limit = args.limit
	assert limit > 0

	mintime = max(min(mintime, MAX_INTERVAL), MIN_INTERVAL)
	maxtime = max(min(maxtime, MAX_INTERVAL), MIN_INTERVAL)

	ei = '%(asctime)s %(levelname)-5.5s [%(name)s][%(thread)d][%(threadName)s] %(message)s'
	logging.root.handlers[0].setFormatter(zope.exceptions.log.Formatter(ei))

	target = reactor.IndexReactor(mintime, maxtime, limit)
	target(time.sleep)

if __name__ == '__main__':
	main()
