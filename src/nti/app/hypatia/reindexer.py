#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time
from collections import defaultdict

from nti.contentsearch.constants import type_
from nti.contentsearch.interfaces import ITypeResolver

from nti.externalization.interfaces import LocatedExternalDict

from nti.hypatia import search_queue
from nti.hypatia import search_catalog
from nti.hypatia.reactor import process_queue
from nti.hypatia.utils import all_cataloged_objects
from nti.hypatia.utils import all_indexable_objects_iids

def get_type(obj):
	resolver = ITypeResolver(obj, None)
	return resolver.type if resolver else None

def reindex(usernames=(), accept=(), cataloged=True, sharedWith=True,
			missing=False, queue_limit=None):
	total = 0
	resolve = bool(queue_limit is not None)
	type_index = search_catalog()[type_] 
	if cataloged:
		generator = all_cataloged_objects(usernames, sharedWith=sharedWith, 
										  resolve=resolve)
	else:
		generator = all_indexable_objects_iids(usernames, resolve=resolve)

	now = time.time()
	queue = search_queue()
	type_count = defaultdict(int)
	for iid, obj in generator:
		try:
			obj_type = get_type(obj)
			if 	(not missing or not type_index.has_doc(iid)) and \
				(not accept or obj_type in accept):
				queue.add(iid)
				total += 1
				type_count[obj_type] = type_count[obj_type] + 1 
		except TypeError:
			pass

	if queue_limit is not None:
		process_queue(limit=queue_limit)
		
	elapsed = time.time() - now
	result = LocatedExternalDict()
	result['Total'] = total
	result['Elapsed'] = elapsed
	result['TypeCount'] = dict(type_count)
	
	logger.info("%s object(s) processed in %s(s)", total, elapsed)
	return result

# script methods

import os
import pprint
import argparse

from nti.dataserver.utils import run_with_dataserver
from nti.dataserver.utils.base_script import create_context

def _process_args(args):
	result = reindex(missing=args.missing,
					 queue_limit=args.limit,
					 cataloged=not args.all,
					 accept=args.types or (),
					 sharedWith=args.sharedWith,
					 usernames=args.usernames or ())
		
	if args.verbose:
		pprint.pprint(result)
	return result
	
def main():
	arg_parser = argparse.ArgumentParser(description="Hypatia content reindexer")
	arg_parser.add_argument('-v', '--verbose', help="Be verbose", action='store_true',
							 dest='verbose')

	arg_parser.add_argument('-m', '--missing', 
							 help="Reindex only missing objects", 
							 action='store_true',
							 dest='missing')

	arg_parser.add_argument('-t', '--types',
							dest='types',
							nargs="+",
							help="The object types to index")

	arg_parser.add_argument('-u', '--usernames',
							dest='usernames',
							nargs="+",
							help="The object creator user names")

	arg_parser.add_argument('-l', '--limit',
							 dest='limit',
							 help="Queue limit",
							 type=int)
	
	site_group = arg_parser.add_mutually_exclusive_group()
	site_group.add_argument('-a', '--all', 
							 help="Reindex all intid objects", 
							 action='store_true',
							 dest='all')

	site_group.add_argument('-s', '--shared',
							 dest='sharedWith',
							 action='store_true',
							 help="Inclued sharedWith objects (if users specified).")

	args = arg_parser.parse_args()
	env_dir = os.getenv('DATASERVER_DIR')
	if not env_dir or not os.path.exists(env_dir) and not os.path.isdir(env_dir):
		raise IOError("Invalid dataserver environment root directory")

	context = create_context(env_dir)
	conf_packages = ('nti.appserver', 'nti.app.hypatia')

	run_with_dataserver(environment_dir=env_dir,
						xmlconfig_packages=conf_packages,
						verbose=args.verbose,
						context=context,
						minimal_ds=True,
						function=lambda: _process_args(args))

if __name__ == '__main__':
	main()
