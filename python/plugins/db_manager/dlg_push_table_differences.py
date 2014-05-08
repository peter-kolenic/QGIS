# -*- coding: utf-8 -*-

"""
/***************************************************************************
Name                 : DB Manager
Description          : Database manager plugin for QGIS
Date                 : Apr 24, 2014
copyright            : (C) 2014 by Peter Kolenic
email                : peter.kolenic@gmail.com

The content of this file is based on
- DB_Manager by Giuseppe Sucameli (GPLv2 license)
  which is based on: PG_Manager by Martin Dobias (GPLv2 license)
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

from PyQt4.QtCore import *
from PyQt4.QtGui import *
from subprocess import Popen, PIPE, STDOUT, call
import os

from .ui.ui_DlgPushTableDifferences import Ui_DbManagerDlgPushTableDifferences as Ui_Dialog
from .ui.ui_DlgPushTableDifferences import _fromUtf8
from .db_plugins.plugin import BaseError
from .db_plugins import createDbPlugin

PG_COMPARE_MAX_RATIO = 2.0
class DlgPushTableDifferences(QDialog, Ui_Dialog):

	def __init__(self, inputTable, parent=None):
		QDialog.__init__(self, parent)
		self.inputTable = inputTable

		# Views don't have primary columns
		# if not [ f for f in self.inputTable.fields() if f.primaryKey ]:
		# 	QMessageBox.warning( None,
		# 		self.tr("Table error"),
		# 		self.tr("unable to push differences - table doesn't have primary key column"))
		# 	QMetaObject.invokeMethod(self, "close", Qt.QueuedConnection)
		# 	return

		self.setupUi(self)
		self.checkButton = QPushButton(_fromUtf8("&Check differences"))
		self.buttonBox.addButton(self.checkButton, QDialogButtonBox.ActionRole)
		self.checkButton.setText(QApplication.translate("DbManagerDlgPushTableDifferences",
			"&Check differences", None, QApplication.UnicodeUTF8))
		self.connect(self.checkButton, SIGNAL("clicked()"), self.startCheck)
		self.syncButton = QPushButton(_fromUtf8("&Push differences"))
		self.buttonBox.addButton(self.syncButton, QDialogButtonBox.ActionRole)
		self.syncButton.setText(QApplication.translate("DbManagerDlgPushTableDifferences",
			"&Push differences", None, QApplication.UnicodeUTF8))
		self.connect(self.syncButton, SIGNAL("clicked()"), self.startSync)


		# *PKField is hidden for regular tables, only show when view is source
		self.labelPKField.hide()
		self.cboPKField.hide()

		self.populateData()

		# updates of UI
		self.connect(self.cboDatabase, SIGNAL("currentIndexChanged(int)"), self.populateSchemas)
		self.connect(self.cboSchema, SIGNAL("currentIndexChanged(int)"), self.populateTables)

		self.connect(self.cboDatabase, SIGNAL("currentIndexChanged(int)"), self.disableSyncButton)
		self.connect(self.cboSchema, SIGNAL("currentIndexChanged(int)"), self.disableSyncButton)
		self.connect(self.cboTable, SIGNAL("currentIndexChanged(int)"), self.disableSyncButton)
		self.disableSyncButton()

	def disableSyncButton(self):
		self.syncButton.setEnabled(False)
		self.chboxLockTables.setChecked(False)
		self.chboxLockTables.setEnabled(False)

	@pyqtSlot('QString')
	def printMessage(self, text):
		self.plainTextEdit.appendPlainText(text)
	def clearMessages(self):
		self.plainTextEdit.clear()

	def enableControls(self, enable=True, resetMouseCursor=False):
		self.cboDatabase.setEnabled(enable)
		self.cboSchema.setEnabled(enable)
		self.cboTable.setEnabled(enable)
		self.checkButton.setEnabled(enable)
		if enable or resetMouseCursor:
			QApplication.restoreOverrideCursor()
		else:
			QApplication.setOverrideCursor(QCursor(Qt.WaitCursor))

	def populateData(self):
		# disable all controls
		self.enableControls(False)
		self.scanner = DBScanForPushCompatibleTables(self.inputTable, self.tr)
		self.scanThread = QThread()
		self.scanner.moveToThread(self.scanThread)
		self.scanner.printMessage.connect(self.printMessage)
		self.scanner.dbDataCreated.connect(self.dataReady)
		self.scanThread.started.connect(self.scanner.process)

		self.scanner.finished.connect(self.scanThread.quit)
		self.scanner.finished.connect(self.scanner.deleteLater)
		self.scanThread.finished.connect(self.scanThread.deleteLater)

		self.scanThread.start()

	def dataReady(self, data):
		self.dbs, self.input_table = data
		if not self.dbs or self.dbs.is_empty():
			self.printMessage(self.tr("Table error - no compatible table to push to found"))
			self.enableControls(enable=False, resetMouseCursor=True)
		else:
			# enable all controls
			self.enableControls(True)
			self.populateDatabases()
			self.populateSchemas()
			self.populateTables()
			if self.input_table.is_view():
				self.labelPKField.show()
				self.cboPKField.show()
				self.cboPKField.clear()
				for f in  list(self.input_table.fields()):
					self.cboPKField.addItem(f.field_name)
				# FIXME: candidates can be pre-computed
				# do not allow any misunderstanding on user side that we have any idea
				# what should be used as a key - no field is pre-selected
				self.cboPKField.setCurrentIndex(-1)
				# disable "Check" button until any field is selected
				self.checkButton.setEnabled(False)
				self.checkButton.connect(self.cboPKField, SIGNAL("currentIndexChanged(int)"), lambda: self.checkButton.setEnabled(True))
				# disable "Sync" button on every change of PK field
				self.checkButton.connect(self.cboPKField, SIGNAL("currentIndexChanged(int)"), lambda: self.syncButton.setEnabled(False))

	def populateDatabases(self):
		self.cboDatabase.clear()
		for connection in self.dbs.connections():
			self.cboDatabase.addItem(connection)
		self.cboDatabase.setCurrentIndex(0 if self.dbs else -1)

	def populateSchemas(self):
		self.cboSchema.clear()
		schemas = self.dbs.get_schema_names_for_db_connection(self.cboDatabase.currentText())
		if schemas:
			for schema in schemas:
				self.cboSchema.addItem(schema)
		self.cboSchema.setEnabled(bool(schemas))
		self.cboSchema.setCurrentIndex(0 if schemas else -1)

	def populateTables(self):
		self.cboTable.clear()
		tables = self.dbs.get_table_names_for_db_schema(self.cboDatabase.currentText(), self.cboSchema.currentText())
		if tables:
			for table in tables:
				self.cboTable.addItem(table)
		self.cboTable.setEnabled(bool(tables))
		self.cboTable.setCurrentIndex(0 if tables else -1)

	# return (inputUri,outputUri,lockTables)
	def get_pg_arguments(self):
		db = self.cboDatabase.currentText()
		pushDiffSchemaName = self.cboSchema.currentText()
		pushDiffTableName = self.cboTable.currentText()
		if not db or not pushDiffSchemaName or not pushDiffTableName:
			# should never happen. valid selection is invariant
			QMessageBox.warning( None,
				self.tr("Push differences"),
				self.tr("Nowhere to push differences to - select table"))
			return (None, None)

		output_table = self.dbs.get_table(db, pushDiffSchemaName, pushDiffTableName)
		force_pk = [ self.cboPKField.currentText() ] if self.input_table.is_view() else None
		return (self.input_table, output_table, self.chboxLockTables.isChecked(), force_pk)

	def startCheck(self):
		(input_table, output_table, lock_tables, force_pk) = self.get_pg_arguments()
		if not (input_table and output_table):
			return
		self.enableControls(False)

		self.checkThread = QThread()
		self.checkWorker = PGComparatorWorker(input_table, output_table, lock_tables, force_pk, self.tr)

		self.checkWorker.moveToThread(self.checkThread)
		self.checkWorker.printMessage.connect(self.printMessage)
		self.checkWorker.clearMessages.connect(self.clearMessages)
		self.checkWorker.synced.connect(self.checkFinished)
		self.checkThread.started.connect(self.checkWorker.check)

		self.checkWorker.finished.connect(self.checkThread.quit)
		self.checkWorker.finished.connect(self.checkWorker.deleteLater)
		self.checkThread.finished.connect(self.checkThread.deleteLater)

		self.checkThread.finished.connect(self.enableControls)

		self.checkThread.start()

	def checkFinished(self, success, inserts, updates, deletes, has_privileges):
		self.enableControls(True)
		if success:
			self.printMessage(self.tr("Summary: inserts :%d  updates: %d  deletes: %d") % (inserts, updates, deletes))
			if has_privileges:
				self.syncButton.setEnabled(True)
				self.chboxLockTables.setEnabled(True)
				self.chboxLockTables.setChecked(False)
			else:
				self.printMessage(self.tr("Can't Push - missing privileges"))
		else:
			self.printMessage(self.tr("ERROR during Check"))

	def startSync(self):
		(input_table, output_table, lock_tables, force_pk) = self.get_pg_arguments()
		if not (input_table and output_table):
			return
		self.enableControls(False)

		self.syncThread = QThread()
		self.syncWorker = PGComparatorWorker(input_table, output_table, lock_tables, force_pk, self.tr)

		self.syncWorker.moveToThread(self.syncThread)
		self.syncWorker.printMessage.connect(self.printMessage)
		self.syncWorker.clearMessages.connect(self.clearMessages)
		self.syncWorker.synced.connect(self.syncFinished)
		self.syncThread.started.connect(self.syncWorker.sync)

		self.syncWorker.finished.connect(self.syncThread.quit)
		self.syncWorker.finished.connect(self.syncWorker.deleteLater)
		self.syncThread.finished.connect(self.syncThread.deleteLater)

		self.syncThread.finished.connect(self.enableControls)

		self.syncThread.start()

	def syncFinished(self, success, inserts, updates, deletes, has_privileges):	# has_privileges is ignored here
		self.enableControls(True)
		self.syncButton.setEnabled(False)
		QMessageBox.information(self, self.tr("Push differences"), self.tr("%s while pushing differences: inserts :%d  updates: %d  deletes: %d") %
			("No error" if success else "Error", inserts, updates, deletes))

class PGComparatorWorker(QObject):
	finished = pyqtSignal()
	printMessage = pyqtSignal('QString')
	clearMessages = pyqtSignal()
	synced = pyqtSignal(bool, int, int, int, bool)	# success, INSERTs, UPDATEs, DELETEs, has SELECT;INSERT;UPDATE;DELETE privileges

	def __init__(self, inputTable, outputTable, lock, force_pk, tr):
		QObject.__init__(self)
		self.inputTable = inputTable
		self.outputTable = outputTable
		self.lock = lock
		self.force_pk = force_pk
		self.tr = tr	# TODO: i hope it doesn't alter any state, so is threadsafe - check this 
						# (can be caching on-demand translating)

	@pyqtSlot()
	def check(self):
		self.process(False)

	@pyqtSlot()
	def sync(self):
		self.process(True)

	@pyqtSlot(bool)
	def process(self, do_it=False):
		pg_call = ["pg_comparator", "--no-lock", "--debug", "--verbose", "--verbose", "--max-ratio",
					str(PG_COMPARE_MAX_RATIO),
					self.inputTable.pg_comparator_connect_string(force_pk = self.force_pk),
					self.outputTable.pg_comparator_connect_string(force_pk = self.force_pk)]
		if do_it:
			pg_call[7:7] = ["-S", "-D"]
		if self.lock:
			pg_call[1] = "--lock"
		self.clearMessages.emit()
		self.printMessage.emit(" ".join(pg_call))
		retcode = 0
		rest_output = ""
		rest_error = ""
		error_message = None
		(inserts, updates, deletes) = ([0], [0], [0])
		try:
			# TODO: try something to read stdout and stderr in separately, and emit different signals 
			#	so can be differentiated in message box by i.e. color
			# http://stackoverflow.com/a/4896288/794081
			# http://stackoverflow.com/a/12270885/794081
			# http://stackoverflow.com/a/1810703/794081
			p = Popen(pg_call, bufsize=1, shell=False, stdout=PIPE, stderr=STDOUT, universal_newlines=True)
			for l in iter(p.stdout.readline, ''):
				self.printMessage.emit(l.rstrip())
				for o in [ ("INSERT", inserts), ("UPDATE", updates), ("DELETE", deletes) ]:
					if l.startswith(o[0]):
						o[1][0] += 1
			(rest_output, rest_error) = p.communicate()
			retcode = p.returncode
		except OSError as e:
			retcode = -1
			error_message = unicode( e )

		text = self.tr("pg_comparator check finished succesfully") if retcode == 0 else self.tr("ERROR: pg_comparator check returned errnum: %d") % retcode
		if error_message:
			text += "\n" + self.tr("Exception") + ": " + error_message
		if rest_output:
			text += "\n" + self.tr("Final messages") + ":\n" + rest_output
		if rest_error:
			text += "\n" + self.tr("Final error messages") + ":\n" + rest_error
		self.printMessage.emit(text)
		try:
			has_privileges = self.outputTable.has_all_table_privileges()
		except Exception, e:
			self.printMessage.emit(self.tr("ERROR while retrieving privileges: %s") + unicode(e))
			has_privileges = False
		self.synced.emit(retcode == 0, inserts[0], updates[0], deletes[0], has_privileges)
		self.finished.emit()

class DBScanForPushCompatibleTables(QObject):
	finished = pyqtSignal()
	# dbDataCreated = pyqtSignal(list)
	dbDataCreated = pyqtSignal(tuple)
	printMessage = pyqtSignal('QString')
	clearMessages = pyqtSignal()
	def __init__(self, inputTable, tr):
		QObject.__init__(self)
		self.input_table_names = ( inputTable.database().connection().connectionName(), inputTable.schemaName(), inputTable.name )
		self.tr = tr	# TODO: i hope it doesn't alter any state, so is threadsafe - check this 
						# (can be caching on-demand translating)

	@pyqtSlot()
	def process(self):
		self.clearMessages.emit()
		self.compatible_connections = []
		self.input_table = None
		# enclose all code in try/except, so as to finish thread in the case of exception
		try:
			dbpluginclass = createDbPlugin( "postgis" )
			connections = DBs(print_message_signal = self.printMessage, tr = self.tr)
			for connection in dbpluginclass.connections(): # TODO: might not be threadsafe
				self.printMessage.emit(self.tr("Checking DB connection %s") % connection.connectionName())
				if connection.database() == None:
					# connect to database
					try:
						if not connection.connect():
							self.printMessage.emit(self.tr("Database connection error ") + self.tr("Unable to connect to ") + connection.connectionName() )
							continue
					except BaseError, e:
						self.printMessage.emit(self.tr("Unable to connect to ") + connection.connectionName() + " " + unicode(e) )
						continue
				if connection.database().connector.hasComparatorSupport():
					self.printMessage.emit(self.tr("Getting DB information from: %s") % connection.connectionName())
					connections.add_and_scan(connection)
				else:
					self.printMessage.emit(self.tr("Skipping connection %s, no pg_comparator support") % connection.connectionName())

			self.compatible_connections, self.input_table = connections.get_compatible_tables(*self.input_table_names)

			if not self.compatible_connections or self.compatible_connections.is_empty():
				self.printMessage.emit(self.tr("No compatible tables found in any database"))
		except Exception, e:
			self.printMessage.emit(self.tr("ERROR while scanning DB: ") + unicode(e))
			# self.printMessage.emit(traceback.format_exc(e))
			self.compatible_connections = []
		finally:
			self.printMessage.emit(self.tr("Scanning for tables finished."))
			self.printMessage.emit("")
			self.dbDataCreated.emit( (self.compatible_connections, self.input_table) )
			self.finished.emit()


class DBs(object):
	def __init__(self, print_message_signal = None, tr = None):
		self.dbs = {}
		self.print_message_signal = print_message_signal
		self._tr = tr

	def tr(self, msg):
		if self._tr:
			return self._tr(msg)
		else:
			return msg

	def print_message(self, msg):
		if self.print_message_signal:
			self.print_message_signal.emit(msg)

	def add_and_scan(self, connection):
		self.dbs[connection.connectionName()] = self.get_schema_table_field_information(connection.database().connector)


	def is_empty(self):
		for con in self.dbs.values():
			for schema in con.schemas().values():
				for table in schema.tables().values():
					return False
		return True

	def add_table(self, connection_name, table):
		connector = table._schema._db._connector
		schema_name = table._schema.schema_name
		table_name = table.table_name

		if not self.dbs.has_key(connection_name):
			self.dbs[connection_name] = DB(connector)
		new_table = self.dbs[connection_name].get_or_create_schema(schema_name).get_or_create_table(table_name)
		new_table.copy_info_from(table)

	def get_compatible_tables(self, connection_name, schema_name, table_name):
		dbs = DBs(print_message_signal = self.print_message_signal, tr = self._tr)
		try:
			input_table = self.dbs[connection_name].schemas()[schema_name].tables()[table_name]
		except KeyError, e:
			raise Exception, 'Error: %s/%s.%s not found in DBs' % (connection_name, schema_name, table_name)
		input_table_fields = input_table.fields()
		input_table_pks = input_table.pks()

		for connection_name, db in self.dbs.iteritems():
			self.print_message(self.tr("Searching for compatible table in DB: %s") % connection_name)

			for schema in db.schemas().values():
				self.print_message(self.tr("Checking schema %s in connection %s") % (schema.schema_name, connection_name))
				for table_name, table in schema.tables().iteritems():
					if table is input_table:
						self.print_message(self.tr("Table %s is source table - skipping") % table_name)
						continue # skip source
					if table.is_view():
						self.print_message(self.tr("Push into views is not supported: %s - skipping") % table_name)
						continue
					if input_table_fields != table.fields():
						self.print_message(self.tr("Table %s is not column compatible - skipping") % table_name)
						continue
					# PKs compatibility is checked only for regular tables
					if not input_table.is_view() and input_table_pks != table.pks():
						self.print_message(self.tr("Table %s is column compatible, but has not the same primary keys - skipping") % table_name)
						continue
					dbs.add_table(connection_name, table)
					self.print_message(self.tr("Compatible table %s found in schema %s in connection %s") % (table.table_name, schema.schema_name, connection_name))

		return (dbs, input_table)

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
		ignored_tables = ",".join(
			[ "'" + t + "'" for t in
				[ "spatial_ref_sys", "geography_columns", "geometry_columns", "raster_columns", "raster_overviews" ]
			]);

		db = DB(connector = connector)

		# get all tables: (schema, name, isRegular) - we need this only for check whether entry is view or regular table
		# views and materialized views can be source.
		# FIXME: it could be possible for views and materialized views to be compared against.
		sql = u"""
			SELECT
				nsp.nspname,
				cla.relname,
				cla.relkind = 'r' isregulartable
			--	,
			--	pg_get_userbyid(cla.relowner) relowner
			FROM pg_class AS cla
			JOIN pg_namespace AS nsp ON nsp.oid = cla.relnamespace
			WHERE
					cla.relkind IN ('v', 'r', 'm')
				AND (nsp.nspname != 'information_schema' AND nsp.nspname !~ '^pg_')
				AND pg_get_userbyid(cla.relowner) != 'postgres'
				AND cla.relname not in (""" +  ignored_tables + ")"

		c = connector._execute(None, sql)
		tables = connector._fetchall(c)
		connector._close_cursor(c)

		for table in tables:
			db.get_or_create_schema(table[0]).get_or_create_table(table[1], not table[2])

		# FIXME: tables can be considered compatible, if equals for each column: pg_type.atttypid, or format_type(a.atttypid,a.atttypmod) ?
		# get columns: (schema, table, position, name, formatted_type)
		sql = u"""
			SELECT
				nsp.nspname AS nspname,
				c.relname AS relname,
				a.attnum AS ordinal_position,
				a.attname AS column_name,
		--		t.typname AS data_type,
				pg_catalog.format_type(a.atttypid,a.atttypmod) AS formatted_type
			FROM pg_class c
			JOIN pg_attribute a ON a.attrelid = c.oid
		--	JOIN pg_type t ON a.atttypid = t.oid
			JOIN pg_namespace nsp ON c.relnamespace = nsp.oid
			WHERE
					c.relname not in (""" + ignored_tables + """)
				AND c.relkind IN ('v', 'r', 'm')
				AND (nsp.nspname != 'information_schema' AND nsp.nspname !~ '^pg_')
				AND a.attnum > 0
			"""

		c = connector._execute(None, sql)
		fields = connector._fetchall(c)
		connector._close_cursor(c)

		for field in fields:
			# self.print_message(self.tr("Schema: %s  Table: %s  Field: %s Name: %s Type:%s") % tuple(field))
			db.get_or_create_schema(field[0]).get_or_create_table(field[1]).add_field(field[2], field[3], field[4])

		# get primary keys: ( schema, table, PKname, "col1pos col2pos ..." )
		sql = u"""
			SELECT
				nsp.nspname,
				t.relname,
				c.conname,
				array_to_string(c.conkey, ' ')
			FROM pg_constraint c
			JOIN pg_class t ON c.conrelid = t.oid
			JOIN pg_namespace nsp ON c.connamespace = nsp.oid
			WHERE
					c.contype = 'p'
				AND t.relkind IN ('v', 'r', 'm')
				AND ( nsp.nspname != 'information_schema' AND nsp.nspname !~ '^pg_' )
				AND t.relname not in (""" +  ignored_tables + ")"

		c = connector._execute(None, sql)
		primaryKeys = connector._fetchall(c)
		connector._close_cursor(c)

		for pk in primaryKeys:
			# self.print_message(self.tr("Schema: %s  Table: %s  PK: %s") % tuple(pk[0:3]))
			db.get_or_create_schema(pk[0]).get_or_create_table(pk[1]).set_pk([int(f) for f in pk[3].split(" ")])

		return db

class DB(object):
	def __init__(self, connector):
		self._connector = connector
		self._schemas = {}

	def get_or_create_schema(self, schema_name):
		if not self._schemas.has_key(schema_name):
			self._schemas[schema_name] = Schema(schema_name, db = self)
		return self._schemas[schema_name]

	def schemas(self):
		return self._schemas

	def get_connect_params(self):
		# return (username, password, host, port, database)
		# return ('lab1','lab','db.gis.lab',5432,'gislab')
		uri = self._connector.uri()
		return (uri.username(), uri.password(), uri.host(), uri.port(), uri.database())

class Schema(object):
	def __init__(self, schema_name, db = None):
		self._db = db
		self.schema_name = schema_name
		self._tables = {}

	def get_or_create_table(self, table_name, is_view = False):
		if not self._tables.has_key(table_name):
			self._tables[table_name] = Table(table_name, schema = self, is_view = is_view)
		return self._tables[table_name]

	def tables(self):
		return self._tables

class Table(object):
	def __init__(self, table_name, schema = None, is_view = False):
		self.table_name = table_name
		self._schema = schema
		self._field_map = {}
		self._primary_keys = None
		self._is_view = is_view

	def is_view(self):
		return self._is_view

	def pg_comparator_connect_string(self, force_pk = None):
	# def pg_comparator_connect_string_for_table(connection, schema, table, pk):
		(username, password, host, port, database) = self._schema._db.get_connect_params()

		# FIXME: fix pg_comparator, so quoted column names work not only in diff, but also on sync
		# pk = ",".join( [ '"'+k+'"' ...
		# in the meanwhile, hope no column needs to be quoted
		# pk = ",".join(force_pk if force_pk else list(self._primary_keys))
		pk = ",".join([ '"' + f + '"' for f in (force_pk if force_pk else list(self._primary_keys)) ])

		# FIXME: escape [@"/:?] in password
		# No fear of shell code injection, since Popen(shell=False)
		s = 'pgsql://%(login)s:%(pass)s@%(host)s:%(port)s/%(base)s/"%(schema)s"."%(table)s"?%(pk)s' % {
			"login":username,
			"pass":	password,
			"host":	host,
			"port":	port,
			"base":	database,
			"schema": self._schema.schema_name,
			"table": self.table_name,
			"pk": pk,
		}
		return s

	def add_field(self, field_num, field_name, field_type):
			assert not self._field_map.has_key(field_num)
			self._field_map[field_num] = Field(field_name, field_type)

	def set_pk(self, list_of_field_nums):
		assert self._primary_keys is None
		# import binascii
		self._primary_keys = frozenset([
									self._field_map[field_order].field_name
									if self._field_map.has_key(field_order)
									# else "MISSING_FIELD_rnd"+binascii.b2a_hex(os.urandom(3))
									else "MISSING_FIELD"
										for field_order in list_of_field_nums ])

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
		return self._primary_keys

	def copy_info_from(self, fr):
		self._primary_keys = fr._primary_keys
		self._field_map = fr._field_map
		self.table_name = fr.table_name
		# do not copy schema object reference

class Field(object):
	def __init__(self, field_name, field_type):
		self.field_name = field_name
		self.field_type = field_type
	def __repr__(self):
		return 'Field(%s,%s)' % (repr(self.field_type), repr(self.field_name))
	def __key(self):
		return (self.field_name, self.field_type)
	def __eq__(self, field):
		if not isinstance(field,Field):
			return False
		return self.__key() == field.__key()
	def __ne__(self, field):
		return not self.__eq__(field)
	def __hash__(self):
		return hash(self.__key())

def check_pg_comparator_presence():
	retcode = 0
	try:
		retcode = call(["pg_comparator", "--help"], stdin=PIPE, stdout=PIPE, stderr=STDOUT, shell=False)
	except OSError as e:
		retcode = -1
	return retcode == 0

