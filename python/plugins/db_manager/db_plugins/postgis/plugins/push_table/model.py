# -*- coding: utf-8 -*-

"""
/***************************************************************************
Name                 : Push table plugin
Description          : Support for diff/push table by using pg_comparator 
Date                 : Apr 24, 2014
copyright            : (C) 2014 by Peter Kolenic
email                : peter.kolenic@gmail.com

 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

class DBs(object):
	"""Class representing database structure (databases/schemas/tables/columns/primary keys)

	Basic usage is:
	- use add_and_scan(db_connection) to scan database information and add the structure here
	- use get_compatible_tables_by_ref(input_table) to filter tables with structure compatible
	  to input_table

	The real work is done in get_schema_table_field_information.
	"""
	def __init__(self, print_message_callback = None, tr = None):
		"""print_message_callback: if supplied, callback is used for printing progress messages
		tr: translate function
		"""
		self.dbs = {}
		self.print_message_callback = print_message_callback
		self._tr = tr

	def tr(self, msg):
		if self._tr:
			return self._tr(msg)
		else:
			return msg

	def print_message(self, msg):
		if self.print_message_callback:
			self.print_message_callback(self.tr(msg))

	def add_and_scan(self, connection):
		self.dbs[connection.connectionName()] = self.get_schema_table_field_information(connection.database().connector)

	def is_empty(self):
		for con in self.dbs.values():
			for schema in con.schemas().values():
				for table in schema.tables().values():
					return False
		return True

	def tables_count(self):
		count = 0
		for con in self.dbs.values():
			for schema in con.schemas().values():
				for table in schema.tables().values():
					count += 1
		return count

	def add_table(self, connection_name, table):
		connector = table._schema._db._connector
		schema_name = table._schema.schema_name
		table_name = table.table_name

		if not self.dbs.has_key(connection_name):
			self.dbs[connection_name] = DB(connector)
		new_table = self.dbs[connection_name].get_or_create_schema(schema_name).get_or_create_table(table_name)
		new_table.copy_info_from(table)

	def get_compatible_tables(self, connection_name, schema_name, table_name):
		try:
			input_table = self.dbs[connection_name].schemas()[schema_name].tables()[table_name]
			dbs = self.get_compatible_tables_by_ref(input_table)
			return (dbs, input_table)
		except KeyError, e:
			raise Exception, 'Error: %s/%s.%s not found in DBs' % (connection_name, schema_name, table_name)

	def get_compatible_tables_by_ref(self, input_table):
		"""Filter tables with structure compatible to input_table.

		Constructs new DBs with only the compatibles, and returns it.
		"""
		dbs = DBs(print_message_callback = self.print_message_callback, tr = self._tr)
		input_table_fields = input_table.fields()
		input_table_pks = input_table.pks()

		for connection_name, db in self.dbs.iteritems():
			# Searching for compatible table in DB: connection_name
			# we need to have database in output, even if no compatible tables are found
			dbs.dbs[connection_name] = DB(db._connector)

			for schema in db.schemas().values():
				# Checking schema schema.schema_name in connection connection_name
				for table_name, table in schema.tables().iteritems():
					if table is input_table:
						# Table is source table - skipping
						continue # skip source
					if table.is_view():
						# Push into views is not supported - skipping
						continue
					if input_table_fields != table.fields():
						# Table is not column compatible - skipping
						continue
					# PKs compatibility is checked only for regular tables
					if not input_table.is_view() and input_table_pks != table.pks():
						# Table is column compatible, but has not the same primary keys - skipping
						continue
					# Compatible table found
					dbs.add_table(connection_name, table)

		return dbs

	def connections(self):
		return self.dbs.keys()

	def get_schema_names_for_db_connection(self, db_connection):
		if self.dbs.has_key(db_connection):
			return [ s.schema_name for s in self.dbs[db_connection].schemas().values() ]
		else:
			return None

	def get_table_names_for_db_schema(self, db_connection, schema_name):
		if self.dbs.has_key(db_connection) and self.dbs[db_connection].schemas().has_key(schema_name) and self.dbs[db_connection].schemas()[schema_name]:
			return [ t for t in self.dbs[db_connection].schemas()[schema_name].tables().keys() ]
		else:
			return None

	def get_table(self, db_connection, schema_name, table_name):
		try:
			db = self.dbs[db_connection]
			schema = db.schemas()[schema_name]
			table = schema.tables()[table_name]
			return table
		except KeyError, e:
			return None

	def get_schema_table_field_information(self, connector):
		"""DB scanning function, adds scanned information.

		DB structure is gained by 3 selects from in information_schema.
		Then, corresponding model structures are constructed.
		"""
		ignored_tables = ",".join(
			[ "'" + t + "'" for t in
				[ "spatial_ref_sys", "geography_columns", "geometry_columns", "raster_columns", "raster_overviews", "topology" ]
			])

		db = DB(connector = connector)

		# get all tables: (schema, name, isRegular) - we need this only for check whether entry is view or regular table
		# views and materialized views can be source.
		# TODO: it could be possible for views and materialized views to be compared against (i.e. target of Check, with no Push).
		sql = u"""
			SELECT	table_schema,
					table_name,
					table_type = 'BASE TABLE'
			FROM information_schema.tables
			WHERE table_schema != 'information_schema'
				AND table_schema !~ '^pg_'
				AND table_name NOT IN (	""" + ignored_tables + """ )
		"""

		c = connector._execute(None, sql)
		tables = connector._fetchall(c)
		connector._close_cursor(c)

		for table in tables:
			db.get_or_create_schema(table[0]).get_or_create_table(table[1], not table[2])

		# get columns: (schema, table, position, name, type)
		sql = u"""
			SELECT
				table_schema,
				table_name,
				ordinal_position,
				quote_ident(column_name),
				data_type
			FROM information_schema.columns
			WHERE	table_schema NOT IN (	'pg_catalog',
											'information_schema')
				AND table_name NOT IN ( """ + ignored_tables + """ )
				AND data_type NOT IN ('USER-DEFINED',
									'ARRAY')
			UNION
			SELECT
				table_schema,
				table_name,
				ordinal_position,
				quote_ident(column_name),
				data_type ||'|'|| udt_name
			FROM information_schema.columns
			WHERE	table_schema NOT IN (	'pg_catalog',
											'information_schema')
				AND table_name NOT IN ( """ + ignored_tables + """ )
				AND data_type = 'USER-DEFINED'
			UNION
			SELECT
				c.table_schema,
				c.table_name,
				c.ordinal_position,
				quote_ident(c.column_name),
				c.data_type ||'|'|| c.udt_name ||'|'|| e.data_type
			FROM information_schema.columns c
			LEFT JOIN information_schema.element_types e
			ON ((c.table_catalog,
				c.table_schema,
				c.table_name,
				'TABLE',
				c.dtd_identifier)
				=
				(e.object_catalog,
				e.object_schema,
				e.object_name,
				e.object_type,
				e.collection_type_identifier))
			WHERE	c.table_schema NOT IN (	'pg_catalog',
											'information_schema')
				AND table_name NOT IN ( """ + ignored_tables + """ )
				AND c.data_type = 'ARRAY'
		"""
		c = connector._execute(None, sql)
		fields = connector._fetchall(c)
		connector._close_cursor(c)

		for field in fields:
			db.get_or_create_schema(field[0]).get_or_create_table(field[1]).add_field(field[2], field[3], field[4])

		# get primary keys: ( schema, table, PKname, column_name(+) )
		sql = u"""
			SELECT	con.table_schema,
					con.table_name,
					con.constraint_name,
					kus.column_name
			FROM information_schema.table_constraints con
			LEFT JOIN information_schema.key_column_usage kus
			ON ((	con.constraint_catalog,
					con.constraint_schema,
					con.constraint_name)
				=
				(	kus.constraint_catalog,
					kus.constraint_schema,
					kus.constraint_name))
			WHERE	con.constraint_type = 'PRIMARY KEY'
					AND con.constraint_schema != 'information_schema'
					AND con.constraint_schema !~ '^pg_'
					AND con.table_name NOT IN ( """ + ignored_tables + """ )
		"""
		c = connector._execute(None, sql)
		primaryKeys = connector._fetchall(c)
		connector._close_cursor(c)

		for pk in primaryKeys:
			db.get_or_create_schema(pk[0]).get_or_create_table(pk[1]).add_pk(pk[3])

		return db

	def get_connection_name_when_onlyone(self):
		cname = self.dbs.keys()
		if len(cname) != 1:
			raise Exception("This DBs should have exactly one connection defined, and it has %d." % len(cname))
		return cname[0]

class DB(object):
	"""Represents DB. Should not be constructed directly outside of model subpackage."""
	def __init__(self, connector):
		self._connector = connector
		self._schemas = {}

	def get_or_create_schema(self, schema_name):
		"""Lazy schema adding.
		"""
		if not self._schemas.has_key(schema_name):
			self._schemas[schema_name] = Schema(schema_name, db = self)
		return self._schemas[schema_name]

	def schemas(self):
		return self._schemas

	def get_connect_params(self):
		uri = self._connector.uri()
		return (uri.username(), uri.password(), uri.host(), uri.port(), uri.database())

class Schema(object):
	"""Represents DB Schema. Should not be constructed directly outside of model subpackage."""
	def __init__(self, schema_name, db = None):
		self._db = db
		self.schema_name = schema_name
		self._tables = {}

	def get_or_create_table(self, table_name, is_view = False):
		"""Lazy table adding.
		"""
		if not self._tables.has_key(table_name):
			self._tables[table_name] = Table(table_name, schema = self, is_view = is_view)
		return self._tables[table_name]

	def tables(self):
		return self._tables

class Table(object):
	"""Represents DB Table. Should not be constructed directly outside of model subpackage."""
	def __init__(self, table_name, schema = None, is_view = False):
		self.table_name = table_name
		self._schema = schema
		self._field_map = {}
		self._is_view = is_view
		self._primary_keys = set()

	def is_view(self):
		return self._is_view

	def pg_comparator_connect_string(self, force_pk = None):
		(username, password, host, port, database) = self._schema._db.get_connect_params()
		pk = ",".join(force_pk if force_pk else list(self._primary_keys))
		# INFO escaping of special chars in password:
		#   (pg_comparator = 2.2.2) it looks like @:? doesn't need escaping, and
		#   there is no way to escape /
		# No fear of shell code injection, since Popen(shell=False) (unless pg_comparator)
		s = 'pgsql://%(username)s%(pass)s%(host)s%(port)s/%(base)s/%(schema)s"%(table)s"?%(pk)s' % {
			"username": username if username else '',
			"pass": ':%s' % password if  username and password else '',
			"host":	'@%s' % host if host else '',
			"port":	':%s' % port if host and port else '',
			"base":	database,
			"schema": '"%s".' % self._schema.schema_name if self._schema.schema_name else '',
			"table": self.table_name,
			"pk": pk,
		}
		return s

	def add_field(self, field_num, field_name, field_type):
		assert not self._field_map.has_key(field_num)
		self._field_map[field_num] = Field(field_name, field_type)

	def add_pk(self, column_name):
		self._primary_keys.add(column_name)

	# def has_all_table_privileges_(self):
	# 	sql = u"""SELECT
	# 				has_table_privilege(%(t)s, 'SELECT'),
	# 				has_table_privilege(%(t)s, 'INSERT'),
	# 				has_table_privilege(%(t)s, 'UPDATE'),
	# 				has_table_privilege(%(t)s, 'DELETE')"""
	# 				% { 't': '"%s"."%s"' % (self.table_name, self.schema.schema_name) }
	# 	c = self.schema.db.connector._execute(None, sql)
	# 	res = self._fetchone(c)
	# 	self._close_cursor(c)
	# 	return all(res)
	def has_all_table_privileges(self):
		privs = self._schema._db._connector.getTablePrivileges( (self._schema.schema_name, self.table_name) )
		return all(privs)

	def fields(self):
		return frozenset(self._field_map.values())

	def pks(self):
		return frozenset(self._primary_keys)

	def copy_info_from(self, fr):
		self._primary_keys = fr._primary_keys
		self._field_map = fr._field_map
		self.table_name = fr.table_name
		# do not copy schema object reference

class Field(object):
	"""Represents DB Field. Should not be constructed directly outside of model subpackage."""
	def __init__(self, field_name, field_type):
		self.field_name = field_name
		self.field_type = field_type
	def __repr__(self):
		return 'Field(%s,%s)' % (repr(self.field_type), repr(self.field_name))
	def __key(self):
		return (self.field_name, self.field_type)
	def __eq__(self, field):
		if not isinstance(field, Field):
			return False
		return self.__key() == field.__key()
	def __ne__(self, field):
		return not self.__eq__(field)
	def __hash__(self):
		return hash(self.__key())
