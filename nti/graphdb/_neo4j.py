#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
Neo4J graphdb

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import six
import numbers
import urlparse

from zope import component
from zope import interface

from py2neo import neo4j
from py2neo import rel as rel4j
from py2neo import node as node4j
from py2neo.exceptions import ClientError

from nti.utils.schema import SchemaConfigured
from nti.utils.schema import createDirectFieldProperties

from . import interfaces as graph_interfaces
from ._neo4j_provider import Neo4jQueryProvider

def _isolate(self, node):
	query = "START a=node(%s) MATCH a-[r]-b DELETE r" % node._id
	self.append_cypher(query, {})

neo4j.WriteBatch.isolate = _isolate

def _is_404(ex):
	cause = getattr(ex, '__cause__', None)
	return getattr(cause, 'status_code', None) == 404

_marker = object()

@interface.implementer(graph_interfaces.IGraphNode)
class Neo4jNode(SchemaConfigured):
	createDirectFieldProperties(graph_interfaces.IGraphNode)

	_neo = None

	def __str__(self):
		return self.id

	def __repr__(self):
		return "%s(%s,%s)" % (self.__class__.__name__, self.id, self.properties)

	def __eq__(self, other):
		try:
			return self is other or (self.id == other.id)
		except AttributeError:
			return NotImplemented

	def __hash__(self):
		xhash = 47
		xhash ^= hash(self.id)

	@classmethod
	def create(cls, node):
		if isinstance(node, Neo4jNode):
			result = node
		elif graph_interfaces.IGraphNode.providedBy(node):
			result = Neo4jNode(id=node.id, uri=node.uri,
							   labels=set(node.labels), properties=dict(node.properties))
		elif node is not None:
			result = Neo4jNode(id=unicode(node._id), uri=unicode(node.__uri__))
			result.labels = set(getattr(node, '_labels', ()))
			result.properties = dict(node._properties)
			result._neo = node
		else:
			result = None
		return result

@interface.implementer(graph_interfaces.IGraphRelationship)
class Neo4jRelationship(SchemaConfigured):
	createDirectFieldProperties(graph_interfaces.IGraphRelationship)

	_neo = None

	def __str__(self):
		return self.id

	def __repr__(self):
		return "%s(%s,%s)" % (self.__class__.__name__, self.id, self.properties)

	def __eq__(self, other):
		try:
			return self is other or (self.id == other.id)
		except AttributeError:
			return NotImplemented

	def __hash__(self):
		xhash = 47
		xhash ^= hash(self.id)
		return xhash

	@classmethod
	def create(cls, rel):
		if isinstance(rel, Neo4jRelationship):
			result = rel
		elif graph_interfaces.IGraphRelationship.providedBy(rel):
			result = Neo4jRelationship(id=rel.id, uri=rel.uri, type=rel.type, start=rel.start, end=rel.end,
									   properties=dict(rel.properties))
		elif rel is not None:
			result = Neo4jRelationship(id=unicode(rel._id),
									   uri=unicode(rel.__uri__),
									   type=rel.type,
									   start=Neo4jNode.create(rel.start_node),
									   end=Neo4jNode.create(rel.end_node),
									   properties=dict(rel._properties))
			result._neo = rel
		else:
			result = None
		return result

@interface.implementer(graph_interfaces.IGraphDB)
class Neo4jDB(object):

	__db__ = None

	def __init__(self, url, username=None, password=None):
		self.url = url
		self.username = username
		self.password = password
		self.provider = Neo4jQueryProvider(self)

	@classmethod
	def authenticate(cls, url, username, password):
		o = urlparse.urlparse(url)
		neo4j.authenticate(o.netloc, username, password)

	@classmethod
	def create_db(cls, url, username=None, password=None):
		if username and password:
			cls.authenticate(url, username, password)
		graphdb = neo4j.GraphDatabaseService(url)
		graphdb.clear()
		graphdb.get_or_create_index(neo4j.Node, "PKIndex")
		graphdb.get_or_create_index(neo4j.Relationship, "PKIndex")
		result = Neo4jDB(url, username=username, password=password)
		return result

	@property
	def db(self):
		if self.__db__ is None:
			if self.username and self.password:
				self.authenticate(self.url, self.username, self.password)
			self.__db__ = neo4j.GraphDatabaseService(self.url)
		return self.__db__

	def _safe_index_remove(self, index, entity):
		try:
			index.remove(entity=entity)
		except ClientError, e:
			if not _is_404(e):
				raise e

	def _do_create_node(self, obj, key=None, value=None, labels=None, properties=None, props=True):
		labels = labels or ()
		properties = properties or dict()

		# create node
		properties.update(graph_interfaces.IPropertyAdapter(obj).properties())
		labels = list(set(labels).union(graph_interfaces.ILabelAdapter(obj).labels()))
		node = node4j(**properties)

		index = self.db.get_or_create_index(neo4j.Node, "PKIndex")
		adapted = graph_interfaces.IUniqueAttributeAdapter(obj)
		key = adapted.key if not key else key
		value = adapted.value if value is None else value

		if key and value is not None:
			result = index.get_or_create(key, value, properties)
			if props:
				result.get_properties()
		else:
			result = self.db.create(node)[0]

		if labels:
			result.set_labels(*labels)

		return result

	def create_node(self, obj, labels=None, properties=None, key=None, value=None, raw=False, props=True):
		result = self._do_create_node(obj, labels, properties, key, value, props=props)
		return Neo4jNode.create(result) if not raw else result

	def create_nodes(self, *objs):
		label_set = []
		wb = neo4j.WriteBatch(self.db)
		for o in objs:
			labels = list(graph_interfaces.ILabelAdapter(o).labels())
			label_set.append(labels)
			properties = graph_interfaces.IPropertyAdapter(o).properties()
			abstract = node4j(**properties)
			adapted = graph_interfaces.IUniqueAttributeAdapter(o)
			if adapted.key and adapted.value:
				wb.get_or_create_in_index(neo4j.Node, "PKIndex", adapted.key, adapted.value, abstract)
			else:
				wb.create(abstract)
		created = wb.submit()
		
		result = []
		wb = neo4j.WriteBatch(self.db)
		for i, n in enumerate(created):
			labels = label_set[i]
			if isinstance(n, neo4j.Node) and labels:
				wb.set_labels(n, *labels)
				result.append(Neo4jNode.create(n))
		wb.submit()
		return result
				
	def _get_labels_and_properties(self, node, props=True):
		if node is not None and props:
			node.get_properties()
			setattr(node, '_labels', node.get_labels())
		return node

	def _do_get_node(self, obj, props=True):
		result = None
		try:
			if isinstance(obj, neo4j.Node):
				result = obj
			elif isinstance(obj, (six.string_types, numbers.Number)):
				result = self.db.node(str(obj))
			elif isinstance(obj, Neo4jNode) and obj._neo is not None:
				result = obj._neo
			elif graph_interfaces.IGraphNode.providedBy(obj):
				result = self.db.node(obj.id)
			elif obj is not None:
				adapted = graph_interfaces.IUniqueAttributeAdapter(obj, None)
				if adapted is not None:
					result = self.db.get_indexed_node("PKIndex", adapted.key, adapted.value)
			self._get_labels_and_properties(result, props)
		except ClientError, e:
			if not _is_404(e):
				raise e
			result = None
		return result

	def get_node(self, obj, raw=False, props=True):
		result = self._do_get_node(obj, props=props)
		return Neo4jNode.create(result) if result is not None and not raw else result

	node = get_node

	def get_nodes(self, *objs):
		nodes = []
		rb = neo4j.ReadBatch(self.db)
		for o in objs:
			adapted = graph_interfaces.IUniqueAttributeAdapter(o)
			rb.get_indexed_nodes("PKIndex", adapted.key, adapted.value)

		for result in rb.submit():
			if result:
				node = result[0]
				nodes.append(Neo4jNode.create(node))
			else:
				nodes.append(None)
		return nodes

	def get_or_create_node(self, obj, raw=False, props=True):
		result = self.get_node(obj, raw=raw, props=props) or self.create_node(obj, raw=raw, props=props)
		return result

	def get_indexed_node(self, key, value, raw=False, props=True):
		result = self.db.get_indexed_node("PKIndex", key, value)
		self._get_labels_and_properties(result, props)
		return Neo4jNode.create(result) if result is not None and not raw else result

	def get_node_properties(self, obj):
		node = self.get_node(obj)
		return node.properties if node is not None else None

	def get_node_labels(self, obj):
		node = self._do_get_node(obj)
		return node._labels if node is not None else None

	def update_node(self, obj, labels=_marker, properties=_marker):
		node = self._do_get_node(obj, props=False)
		if node is not None:
			if labels != _marker:
				node.set_labels(*labels)
			if properties != _marker:
				node.set_properties(properties)
			return True
		return False

	def _do_delete_node(self, obj):
		node = self._do_get_node(obj, props=False)
		if node is not None:
			wb = neo4j.WriteBatch(self.db)
			wb.remove_from_index(neo4j.Node, "PKIndex", entity=node)
			wb.isolate(node)
			wb.delete(node)
			responses = wb.submit()
			return responses[2] is None
		return False

	def delete_node(self, obj):
		result = self._do_delete_node(obj)
		return result

	def delete_nodes(self, *objs):
		nodes = []

		# get all the nodes at once
		rb = neo4j.ReadBatch(self.db)
		for o in objs:
			adapted = graph_interfaces.IUniqueAttributeAdapter(o)
			if adapted.key and adapted.value:
				rb.get_indexed_nodes("PKIndex", adapted.key, adapted.value)
			else:
				node = self._do_get_node(o, props=False)
				if node is not None:
					nodes.append(node)

		for lst in rb.submit():
			if lst:
				nodes.append(lst[0])

		# process all deletions at once
		wb = neo4j.WriteBatch(self.db)
		for node in nodes:
			wb.remove_from_index(neo4j.Node, "PKIndex", entity=node)
			wb.isolate(node)
			wb.delete(node)
			
		result = 0
		responses = wb.submit()
		for idx in range(2, len(responses), 3):
			if responses[idx] is None:
				result += 1
		return result

	# relationships

	def _get_rel_properties(self, start, end, rel_type):
		adapted = component.queryMultiAdapter((start, end, rel_type), graph_interfaces.IPropertyAdapter)
		result = adapted.properties() if adapted is not None else None
		return result or {}
	
	def _get_rel_keyvalue(self, start, end, rel_type, key=None, value=None):
		adapted = component.queryMultiAdapter((start, end, rel_type), graph_interfaces.IUniqueAttributeAdapter)
		if adapted is not None:
			key = adapted.key if not key else key
			value = adapted.value if value is None else value
		return (key, value)

	def _do_create_relationship(self, start, end, rel_type, properties=None, key=None, value=None):
		properties = properties or dict()

		# get neo4j nodes
		n4j_end = self.get_or_create_node(end, raw=True, props=False)
		n4j_start = self.get_or_create_node(start, raw=True, props=False)
		properties.update(self._get_rel_properties(start, end, rel_type))
		
		index = self.db.get_or_create_index(neo4j.Relationship, "PKIndex")
		key, value = self._get_rel_keyvalue(start, end, rel_type, key, value)
		if key and value is not None:
			abstract = [n4j_start, str(rel_type), n4j_end, properties]
			result = index.get_or_create(key, value, abstract)
			if properties:
				result.get_properties()
		else:
			# create neo4j relationship
			rel = rel4j(n4j_start, str(rel_type), n4j_end, **properties)
			result = self.db.create(rel)[0]

		return result

	def create_relationship(self, start, end, rel_type, properties=None, key=None, value=None, raw=False):
		result = self._do_create_relationship(start, end, rel_type, properties, key, value)
		return Neo4jRelationship.create(result) if not raw else result

	def _do_get_relationship(self, obj, props=True):
		result = None
		try:
			if isinstance(obj, neo4j.Relationship):
				result = obj
			elif isinstance(obj, (six.string_types, numbers.Number)):
				result = self.db.relationship(str(obj))
			elif isinstance(obj, Neo4jRelationship) and obj._neo is not None:
				result = obj._neo
			elif graph_interfaces.IGraphRelationship.providedBy(obj):
				result = self.db.relationship(obj.id)
			if result is not None and props:
				result.get_properties()
		except ClientError, e:
			if not _is_404(e):
				raise e
			result = None
		return result

	def get_relationship(self, obj, raw=False):
		result = self._do_get_relationship(obj)
		return Neo4jRelationship.create(result) if result is not None and not raw else result

	relationship = get_relationship

	def get_indexed_relationship(self, key, value, raw=False, props=True):
		result = self.db.get_indexed_relationship("PKIndex", key, value)
		if result is not None and props:
			result.get_properties()
		return Neo4jRelationship.create(result) if result is not None and not raw else result
	
	def _do_match(self, start_node=None, end_node=None, rel_type=None, bidirectional=False, limit=None):
		n4j_end = self._do_get_node(end_node) if end_node is not None else None
		n4j_start = self._do_get_node(start_node) if start_node is not None else None
		n4j_type = str(rel_type) if rel_type is not None else None
		result = self.db.match(n4j_start, n4j_type, n4j_end, bidirectional, limit)
		return result

	def match(self, start=None, end=None, rel_type=None, bidirectional=False, limit=None, raw=False):
		result = self._do_match(start, end, rel_type, bidirectional, limit)
		result = [Neo4jRelationship.create(x) for x in result or ()] if not raw else result
		return result or ()
		
	def delete_relationships(self, *objs):
		# collect node4j rels
		rels = set([self._do_get_relationship(x, False) for x in objs])
		rels.discard(None)

		wb = neo4j.WriteBatch(self.db)
		for rel in rels:
			wb.remove_from_index(neo4j.Relationship, "PKIndex", entity=rel)
			wb.delete(rel)
		wb.submit()

		return True if rels else False

	delete_relationship = delete_relationships

	def delete_indexed_relationship(self, key, value):
		rel = self.db.get_indexed_relationship("PKIndex", key, value)
		if rel is not None:
			wb = neo4j.WriteBatch(self.db)
			wb.remove_from_index(neo4j.Relationship, "PKIndex", entity=rel)
			wb.delete(rel)
			wb.submit()
		return rel

	def update_relationship(self, obj, properties=_marker):
		rel = self._do_get_relationship(obj)
		if rel is not None:
			if properties != _marker:
				rel.set_properties(properties)
			return True
		return False

	# cypher

	def execute(self, query, **params):
		result = neo4j.CypherQuery(self.db, query).execute(**params)
		return result
