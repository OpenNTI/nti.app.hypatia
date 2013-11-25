#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
graphdb interfaces

$Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

from zope import schema
from zope import interface

from dolmen.builtins import IDict
from dolmen.builtins import ITuple

from nti.utils import schema as nti_schema

NEO4J = u"neo4j"
DATABASE_TYPES = (NEO4J,)

ADD_EVENT = 0
MODIFY_EVENT = 1
REMOVE_EVENT = 2

class IGraphQueryProvider(interface.Interface):

	def suggest_friends_to(user, max_depth=2, limit=None):
		pass

class IGraphDB(interface.Interface):

	provider = nti_schema.Object(IGraphQueryProvider, title='query provider')

	def execute(query, **params):
		pass

	def create_node(obj, labels=None, properties=None, key=None, value=None):
		pass

	def create_nodes(*objs):
		pass

	def get_or_create_node(obj):
		pass

	def get_node(obj):
		pass

	def get_nodes(*objs):
		pass

	def get_node_properties(obj):
		pass

	def get_node_labels(obj):
		pass

	def get_indexed_node(key, value):
		pass

	def delete_node(obj):
		pass

	def delete_nodes(*objs):
		pass

	def update_node(obj, properties=None):
		pass

	def create_relationship(start, end, rel_type, properties=None, key=None, value=None):
		pass

	def get_relationship(obj):
		pass

	def match(start_node=None, end_node=None, rel_type=None, bidirectional=False, limit=None):
		pass

	def delete_indexed_relationship(key, value):
		pass

	def delete_relationships(*rels):
		pass

	def update_relationship(obj, properties=None):
		pass

	def get_indexed_relationship(key, value):
		pass


class IGraphNode(interface.Interface):
	id = nti_schema.ValidTextLine(title="node id")
	uri = nti_schema.ValidTextLine(title="uri identifier", required=False)
	labels = schema.Tuple(value_type=nti_schema.ValidTextLine(title="label"), required=False)
	properties = schema.Dict(nti_schema.ValidTextLine(title="key"),
							 nti_schema.ValidTextLine(title="value"),
							 required=False)

class IRelationshipType(interface.Interface):
	"""
	Marker interface for a relationship
	"""

	def __str__():
		pass

class IGraphRelationship(interface.Interface):

	id = nti_schema.ValidTextLine(title="relationship id")

	uri = nti_schema.ValidTextLine(title="uri identifier", required=False)

	type = nti_schema.Variant((nti_schema.Object(IRelationshipType, description="A :class:`.Interface`"),
							   nti_schema.ValidTextLine(title='relationship type')),
							  title="The relationship type")

	start = nti_schema.Variant((nti_schema.Object(IGraphNode, description="A :class:`.IGraphNode`"),
								nti_schema.Object(interface.Interface, description="A :class:`.Interface`")),
							  title="The start node",
							  required=False)

	end = nti_schema.Variant((nti_schema.Object(IGraphNode, description="A :class:`.IGraphNode`"),
							  nti_schema.Object(interface.Interface, description="A :class:`.Interface`")),
							 title="The end node",
							 required=False)

	properties = schema.Dict(nti_schema.ValidTextLine(title="key"), 
							 nti_schema.Variant((nti_schema.ValidTextLine(title="value string"),
												 nti_schema.Number(title="value number"),
												 schema.Bool(title="value bool"),
												 schema.List(title="value list"))),
							 required=False)

class IPropertyAdapter(IDict):
	"""
	return a dict of properties
	"""

class ILabelAdapter(ITuple):
	"""
	returns a set with labels
	"""

class IUniqueAttributeAdapter(interface.Interface):
	"""
	Interface to specify the attribute name/value that uniquely identifies an object
	"""
	key = interface.Attribute("Attribute key")
	value = interface.Attribute("Attribute value")

class IFriendOf(IRelationshipType):
	pass

class IMemberOf(IRelationshipType):
	pass

class IFollow(IRelationshipType):
	pass

class ICommentOn(IRelationshipType):
	pass

class ITakeAssessment(IRelationshipType):
	pass

class ILike(IRelationshipType):
	pass

class IRate(IRelationshipType):
	pass

class IReply(IRelationshipType):
	pass

class IAuthor(IRelationshipType):
	pass
