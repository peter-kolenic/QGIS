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

		def pg_comparator_connect_string_for_table(table, pk):
			# FIXME: escape [@"/:?] in password
			# No fear of shell code injection, since Popen(shell=False)
			uri = table.uri()
			s = "pgsql://%(login)s:%(pass)s@%(host)s:%(port)s/%(base)s/%(schema_table)s?%(pk)s" % {
				"login":uri.username(),
				"pass":	uri.password(),
				"host":	uri.host(),
				"port":	uri.port(),
				"base":	uri.database(),
				"schema_table":uri.quotedTablename(),
				"pk": pk,
			}
			return s

		pushDiffTable = self.connections[dbi][1][pushDiffSchema][1][pushDiffTableName][0]
		# FIXME: fix pg_comparator, so quoted column names work not only in diff, but also on sync 
		# pk = ",".join( [ '"'+k+'"' for k in self.connections[dbi][1][pushDiffSchema][1][pushDiffTableName][1] ] )
		# in the meanwhile, hope no column needs to be quoted
		pk = ",".join( self.connections[dbi][1][pushDiffSchema][1][pushDiffTableName][1] )
		pg_inputTableConnectString = pg_comparator_connect_string_for_table(self.inputTable, pk)
		pg_outputTableConnectString = pg_comparator_connect_string_for_table(pushDiffTable, pk)
		return (pg_inputTableConnectString, pg_outputTableConnectString, pushDiffTable)

	def startCheck(self):
		(pg_inputTableConnectString, pg_outputTableConnectString, outputTable) = self.get_pg_arguments()
		if not ( pg_inputTableConnectString and pg_outputTableConnectString):
			return
		self.enableControls(False)

		self.checkThread = QThread()
		self.checkWorker = PGComparatorWorker(pg_inputTableConnectString, pg_outputTableConnectString, outputTable, self.tr)

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
		(pg_inputTableConnectString, pg_outputTableConnectString, outputTable) = self.get_pg_arguments()
		if not ( pg_inputTableConnectString and pg_outputTableConnectString):
			return
		self.enableControls(False)

		self.syncThread = QThread()
		self.syncWorker = PGComparatorWorker(pg_inputTableConnectString, pg_outputTableConnectString, outputTable, self.tr)

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

	def __init__(self, inputUri, outputUri, outputTable, tr):
		QObject.__init__(self)
		self.inputUri = inputUri
		self.outputUri = outputUri
		self.outputTable = outputTable
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
		has_privileges = all(self.outputTable.database().connector.getTablePrivileges( (self.outputTable.schema().name, self.outputTable.name) ))
		self.synced.emit(retcode == 0, inserts[0], updates[0], deletes[0], has_privileges)
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
		# TODO: add arguments (, inputTable, tr): -> make this function stateless 
		#   (and allow for more worker threads at once)
		self.clearMessages.emit()
		# FIXME:
		# use something like [ q.data(0) for q in self.parent().tree.model().rootItem.children() ] == ['PostGIS', 'SpatiaLite']
		# DBManager.(DBTree)tree.setModel(DBModel(mainWindow=DBManager)).PluginItem()
		# don't generate from start.
		# find out, where in DTree are connections stored (if we can rely on them beeing already populated, which 
		# can not be the case)
		# Maybe refactor out the get all db connections functionality (this is not the right place)
		# If possible, cache results (i.e. in the case of remote DB connection and 10000 tables there).

		# data is stored in self.connections, in structure:
		#	self.connection = [ (connection, schemas )]
		#                                    schemas = { name: (schema, compatible_tables) }
		#                                                               compatible_tables = { table_name: (table, [PKs]) }

		inputTableUri = self.inputTable.uri().uri()

		inputTableFieldsDefs = [ (f.name, f.dataType, f.primaryKey ) for f in self.inputTable.fields() ]
		# not using more precise f.definition(), because sequencer name
		# differs, and is part of fields default value

		# primary key can be composite
		inputTablePK = [ f.name for f in self.inputTable.fields() if f.primaryKey ]
		self.connections = []
		if len(inputTablePK) == 0:
			self.printMessage.emit(
				self.tr("ERROR: Source table must have simple primary key column, and it has none."))
		else:
			dbpluginclass = createDbPlugin( "postgis" )
			for connection in dbpluginclass.connections(): # might not be threadsafe
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
					schemas = {}
					db = connection.database()
					schemas_ = db.schemas()
					for schema in schemas_:
						self.printMessage.emit(self.tr("Checking schema %s in connection %s") % (schema.name, connection.connectionName()))
						tables = {}
						tables_ = schema.tables()
						for table in tables_:
							if table.uri().uri() == inputTableUri:
								self.printMessage.emit(self.tr("Table %s is source table - skipping") % table.name)
								continue # skip source
							fieldsDefs = [ (f.name, f.dataType, f.primaryKey) for f in table.fields() ]
							if fieldsDefs != inputTableFieldsDefs:
								self.printMessage.emit(self.tr("Table %s is not compatible - skipping") % table.name)
								continue
							tables[table.name] = (table, inputTablePK)
							self.printMessage.emit(self.tr("Compatible table %s found in schema %s in connection %s") % (table.name, schema.name, connection.connectionName()))
						if tables:
							schemas[schema.name] = (schema, tables)
						else:
							self.printMessage.emit(self.tr("Skipping schema %s, no compatible table") % schema.name)
					if schemas:
						self.connections.append((connection, schemas))
					else:
						self.printMessage.emit(self.tr("Skipping connection %s, no compatible table in its schemas") % connection.connectionName())
				else:
					self.printMessage.emit(self.tr("Skipping connection %s, no pg_comparator support") % connection.connectionName())
			if not self.connections:
				self.printMessage.emit(self.tr("No compatible tables found in any database"))
		self.printMessage.emit(self.tr("Scanning for tables finished."))
		self.printMessage.emit("")
		self.dbDataCreated.emit(self.connections)
		self.finished.emit()


def check_pg_comparator_presence():
	retcode = 0
	try:
		retcode = call(["pg_comparator", "--help"], stdin=PIPE, stdout=PIPE, stderr=STDOUT, shell=False)
	except OSError as e:
		retcode = -1
	return retcode == 0


