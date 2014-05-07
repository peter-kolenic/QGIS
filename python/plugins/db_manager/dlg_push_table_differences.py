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

import pdb
import traceback

from .ui.ui_DlgPushTableDifferences import Ui_DbManagerDlgPushTableDifferences as Ui_Dialog
from .ui.ui_DlgPushTableDifferences import _fromUtf8
from .db_plugins.plugin import BaseError
from .db_plugins import createDbPlugin # if db_manager.tree is used, remove this

PG_COMPARE_MAX_RATIO = 2.0
class DlgPushTableDifferences(QDialog, Ui_Dialog):

	def __init__(self, inputTable, parent=None):
		QDialog.__init__(self, parent)
		self.inputTable = inputTable

		if not [ f for f in self.inputTable.fields() if f.primaryKey ]:
			QMessageBox.warning( None,
				self.tr("Table error"),
				self.tr("unable to push differences - table doesn't have primary key column"))
			QMetaObject.invokeMethod(self, "close", Qt.QueuedConnection)
			return

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
		self.connections = data
		if not data:
			self.printMessage(self.tr("Table error - no compatible table to push to found"))
			self.enableControls(enable=False, resetMouseCursor=True)
		else:
			# enable all controls
			self.enableControls(True)
			self.populateDatabases()
			self.populateSchemas()
			self.populateTables()


	def populateDatabases(self):
		self.cboDatabase.clear()
		for connection in [ c[0] for c in self.connections]:
			self.cboDatabase.addItem(connection.connectionName())
		self.cboDatabase.setCurrentIndex(0 if self.connections else -1)

	def populateSchemas(self):
		self.cboSchema.clear()
		dbi = self.cboDatabase.currentIndex()
		if dbi >= 0:
			schemas = self.connections[dbi][1]
			if schemas == None:
				self.cboSchema.setEnabled(False)
				return
			else:
				self.cboSchema.setEnabled(True)

			for schema in schemas.keys():
				self.cboSchema.addItem(schema)
			self.cboSchema.setCurrentIndex(0 if schemas else -1)

	def populateTables(self):
		self.cboTable.clear()
		schi = self.cboSchema.currentIndex()
		if not self.connections or schi < 0:
			self.cboTable.setCurrentIndex(-1)
			return
		tables = None
		try:
			tables = self.connections[self.cboDatabase.currentIndex()][1][self.cboSchema.currentText()][1]
		except KeyError, e:
			return
		for table in tables.keys():
			self.cboTable.addItem(table)
		self.cboTable.setCurrentIndex(0 if tables else -1)

	# return (inputUri,outputUri,outputTable)
	def get_pg_arguments(self):
		dbi = self.cboDatabase.currentIndex()
		pushDiffSchema = self.cboSchema.currentText()
		pushDiffTableName = self.cboTable.currentText()
		if dbi < 0 or not pushDiffSchema or not pushDiffTableName:
			# should never happen. valid selection is invariant
			QMessageBox.warning( None,
				self.tr("Push differences"),
				self.tr("Nowhere to push differences to - select table"))
			return (None, None)

		def pg_comparator_connect_string_for_table(connection, schema, table, pk):
			# FIXME: escape [@"/:?] in password
			# No fear of shell code injection, since Popen(shell=False)
			uri = connection.db.uri()
			s = 'pgsql://%(login)s:%(pass)s@%(host)s:%(port)s/%(base)s/"%(schema)s"."%(table)s"?%(pk)s' % {
				"login":uri.username(),
				"pass":	uri.password(),
				"host":	uri.host(),
				"port":	uri.port(),
				"base":	uri.database(),
				"schema": schema,
				"table": table,
				"pk": pk,
			}
			return s

		## pushDiffTable = self.connections[dbi][1][pushDiffSchema][1][pushDiffTableName][0]
		# FIXME: fix pg_comparator, so quoted column names work not only in diff, but also on sync 
		# pk = ",".join( [ '"'+k+'"' for k in self.connections[dbi][1][pushDiffSchema][1][pushDiffTableName][1] ] )
		# in the meanwhile, hope no column needs to be quoted
		pk = ",".join( self.connections[dbi][1][pushDiffSchema][1][pushDiffTableName][1] )
		pg_inputTableConnectString = pg_comparator_connect_string_for_table(self.inputTable.database().connection(), self.inputTable.uri().schema(), self.inputTable.name, pk)
		pg_outputTableConnectString = pg_comparator_connect_string_for_table(self.connections[dbi][0], pushDiffSchema, pushDiffTableName, pk)
		return (pg_inputTableConnectString, pg_outputTableConnectString) #, pushDiffTable)

	def startCheck(self):
		# (pg_inputTableConnectString, pg_outputTableConnectString, outputTable) = self.get_pg_arguments()
		(pg_inputTableConnectString, pg_outputTableConnectString) = self.get_pg_arguments()
		if not ( pg_inputTableConnectString and pg_outputTableConnectString):
			return
		self.enableControls(False)

		self.checkThread = QThread()
		# self.checkWorker = PGComparatorWorker(pg_inputTableConnectString, pg_outputTableConnectString, outputTable, self.tr)
		self.checkWorker = PGComparatorWorker(pg_inputTableConnectString, pg_outputTableConnectString, self.tr)

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
			else:
				self.printMessage(self.tr("Can't Push - missing privileges"))
		else:
			self.printMessage(self.tr("ERROR during Check"))

	def startSync(self):
		# (pg_inputTableConnectString, pg_outputTableConnectString, outputTable) = self.get_pg_arguments()
		(pg_inputTableConnectString, pg_outputTableConnectString) = self.get_pg_arguments()
		if not ( pg_inputTableConnectString and pg_outputTableConnectString):
			return
		self.enableControls(False)

		self.syncThread = QThread()
		# self.syncWorker = PGComparatorWorker(pg_inputTableConnectString, pg_outputTableConnectString, outputTable, self.tr)
		self.syncWorker = PGComparatorWorker(pg_inputTableConnectString, pg_outputTableConnectString, self.tr)

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
	synced = pyqtSignal(bool, int, int, int, bool)	# success, INSERTs, UPDATEs, DELETEs, has INSERT;UPDATE;DELETE privileges

	# def __init__(self, inputUri, outputUri, outputTable, tr):
	def __init__(self, inputUri, outputUri, tr):
		QObject.__init__(self)
		self.inputUri = inputUri
		self.outputUri = outputUri
		# self.outputTable = outputTable
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
		pg_call = ["pg_comparator", "--debug", "--verbose", "--verbose", "--max-ratio", str(PG_COMPARE_MAX_RATIO), self.inputUri, self.outputUri]
		if do_it:
			pg_call[6:6] = ["-S", "-D"]
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
		# XXX
		# has_privileges = all(self.outputTable.database().connector.getTablePrivileges( (self.outputTable.schema().name, self.outputTable.name) ))
		# self.synced.emit(retcode == 0, inserts[0], updates[0], deletes[0], has_privileges)
		self.synced.emit(retcode == 0, inserts[0], updates[0], deletes[0], True)
		self.finished.emit()

class DBScanForPushCompatibleTables(QObject):
	finished = pyqtSignal()
	dbDataCreated = pyqtSignal(list)
	printMessage = pyqtSignal('QString')
	clearMessages = pyqtSignal()
	def __init__(self, inputTable, tr):
		QObject.__init__(self)
		self.inputTable = inputTable # TODO: ? consider defensive deepcopy ?
		self.tr = tr	# TODO: i hope it doesn't alter any state, so is threadsafe - check this 
						# (can be caching on-demand translating)

	@pyqtSlot()
	def process(self):
		self.clearMessages.emit()
		self.compatibleConnections = []
		# enclose all code in try/except, so as to finish thread in the case of exception
		try:
			# data is stored in self.compatibleConnections, in structure:
			#	self.connection = [ (connection, schemas )]
			#                                    schemas = { name: (schema, compatible_tables) }
			#                                                               compatible_tables = { table_name: (table, [PKs]) }

			# pre-check table for primary key, so as to skip database scans
			# primary key can be composite
			inputTablePK = [ f.name for f in self.inputTable.fields() if f.primaryKey ]
			if len(inputTablePK) == 0:
				self.printMessage.emit(
					self.tr("ERROR: Source table must have simple primary key column, and it has none."))
			else:
				dbpluginclass = createDbPlugin( "postgis" )
				# connections[connectionName()] = getSchemaTableFieldInformation(...)
				connections = {}
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
						# XXX dostan neskor dbname, host, port password info alebo z connection, alebo ho sem rovno uloz
						connections[connection.connectionName()] = (connection, self.getSchemaTableFieldInformation(connection.database().connector))
					else:
						self.printMessage.emit(self.tr("Skipping connection %s, no pg_comparator support") % connection.connectionName())

				inputTable = connections[self.inputTable.database().connection().connectionName()][1][self.inputTable.schemaName()][self.inputTable.name]
				inputTableFields = frozenset(inputTable[0].values())
				inputTablePKs = inputTable[1]

				for connectionName, (connection, schemas_) in connections.iteritems():
					self.printMessage.emit(self.tr("Searching for compatible table in DB: %s") % connectionName)
					# pyqtRemoveInputHook()
					# pdb.set_trace()

					schemas = {}
					for schemaName in schemas_.keys():
						self.printMessage.emit(self.tr("Checking schema %s in connection %s") % (schemaName, connectionName))
						tables = {}
						for tableName, table in schemas_[schemaName].iteritems():
							if table is inputTable:
								self.printMessage.emit(self.tr("Table %s is source table - skipping") % tableName)
								continue # skip source
							if inputTableFields != frozenset(table[0].values()):
								self.printMessage.emit(self.tr("Table %s is not column compatible - skipping") % tableName)
								continue
							if inputTablePKs != table[1]:
								self.printMessage.emit(self.tr("Table %s is column compatible, but has not the same primary keys - skipping") % tableName)
								continue
							tables[tableName] = (tableName, inputTablePKs)
							self.printMessage.emit(self.tr("Compatible table %s found in schema %s in connection %s") % (tableName, schemaName, connectionName))
						if tables:
							schemas[schemaName] = (schemaName, tables)
						else:
							self.printMessage.emit(self.tr("Skipping schema %s, no compatible table") % schemaName)
					if schemas:
						self.compatibleConnections.append((connection, schemas))
					else:
						self.printMessage.emit(self.tr("Skipping connection %s, no compatible table in its schemas") % connection.connectionName())


				if not self.compatibleConnections:
					self.printMessage.emit(self.tr("No compatible tables found in any database"))
		except Exception, e:
			self.printMessage.emit(self.tr("ERROR while scanning DB: ") + unicode(e))
			self.printMessage.emit(traceback.format_exc(e))
			self.compatibleConnections = []
		finally:
			self.printMessage.emit(self.tr("Scanning for tables finished."))
			self.printMessage.emit("")
			self.dbDataCreated.emit(self.compatibleConnections)
			self.finished.emit()

	# returns for database connector:
	# schemas = { name: (schema,	tables) }
	# 								tables = { table_name: [	fields, set( names of PK columns ) ] }
	#															fields = { order:( name, type ) }
	def getSchemaTableFieldInformation(self, connector):
		ignored_tables = ",".join(
			[ "'" + t + "'" for t in
				[ "spatial_ref_sys", "geography_columns", "geometry_columns", "raster_columns", "raster_overviews" ]
			]);

		# # We don't need tables at all
		# # get all tables: (schema, name, isRegular)
		# sql = u"""
		# 	SELECT
		# 		nsp.nspname,
		# 		cla.relname,
		# 		cla.relkind = 'r' isregulartable
		# 	--	,
		# 	--	pg_get_userbyid(cla.relowner) relowner
		# 	FROM pg_class AS cla
		# 	JOIN pg_namespace AS nsp ON nsp.oid = cla.relnamespace
		# 	WHERE
		# 			cla.relkind IN ('v', 'r', 'm')
		# 		AND (nsp.nspname != 'information_schema' AND nsp.nspname !~ '^pg_')
		# 		AND pg_get_userbyid(cla.relowner) != 'postgres'
		# 		AND cla.relname not in (""" +  ignored_tables + ")"

		# c = connector._execute(None, sql)
		# tables = connector._fetchall(c)
		# connector._close_cursor(c)

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
				AND (nsp.nspname != 'information_schema' AND nsp.nspname !~ '^pg_')
				AND a.attnum > 0
			"""

		c = connector._execute(None, sql)
		fields = connector._fetchall(c)
		connector._close_cursor(c)

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

		# schemas = { name: (schema,	tables) }
		# 								tables = { table_name: [	fields, set( names of PK columns ) ] }
		#															fields = { order:( name, type ) }
		schemas = {}
		def getSchema(schemaName):
			if not schemas.has_key(schemaName):
				schemas[schemaName] = {}
			return schemas[schemaName]

		def getTable(schemaName, tableName):
			schema = getSchema(schemaName)
			if not schema.has_key(tableName):
				schema[tableName] = [{},None]
			return schema[tableName]

		for field in fields:
			self.printMessage.emit(self.tr("Schema: %s  Table: %s  Field: %s Name: %s Type:%s") % tuple(field))
			# getTable(field[0],field[1])[0][field[2]] = ( field[3], field[4] )
			tableFields = getTable(field[0],field[1])[0]
			assert not tableFields.has_key(field[2])
			tableFields[field[2]] = ( field[3], field[4] )

		import binascii
		for pk in primaryKeys:
			self.printMessage.emit(self.tr("Schema: %s  Table: %s  PK: %s") % tuple(pk[0:3]))
			table = getTable(pk[0],pk[1])
			assert table[1] is None
			table[1] = frozenset([	table[0][int(fieldOrder)][0]
									if table[0].has_key(int(fieldOrder))
									else "MISSING_FIELD_rnd"+binascii.b2a_hex(os.urandom(3))
										for fieldOrder in pk[3].split(" ") ])
		return schemas


def check_pg_comparator_presence():
	retcode = 0
	try:
		retcode = call(["pg_comparator", "--help"], stdin=PIPE, stdout=PIPE, stderr=STDOUT, shell=False)
	except OSError as e:
		retcode = -1
	return retcode == 0

