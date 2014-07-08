# -*- coding: utf-8 -*-

"""
/***************************************************************************
Name                 : Push table plugin
Description          : Support for diff/push table by using pg_comparator 
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

from ....plugin import BaseError
from .... import createDbPlugin
from .ui_DlgPushTable import Ui_DbManagerDlgPushTable as Ui_Dialog
from .ui_DlgPushTable import _fromUtf8
from .workers import PGComparatorWorker, DBScanForPushCompatibleTables
from .model import DBs

class DlgPushTable(QDialog, Ui_Dialog):

	def __init__(self, inputTable, parent=None):
		QDialog.__init__(self, parent)
		self.inputTable = inputTable

		# Views don't have primary columns
		# if not [ f for f in self.inputTable.fields() if f.primaryKey ]:
		# 	QMessageBox.warning( None,
		# 		self.tr("Table error"),
		# 		self.tr("unable to push table - table doesn't have primary key column"))
		# 	QMetaObject.invokeMethod(self, "close", Qt.QueuedConnection)
		# 	return

		self.setupUi(self)
		self.checkButton = QPushButton(_fromUtf8("&Check differences"))
		self.buttonBox.addButton(self.checkButton, QDialogButtonBox.ActionRole)
		self.checkButton.setText(QApplication.translate("DbManagerDlgPushTable",
			"&Check differences", None, QApplication.UnicodeUTF8))
		self.connect(self.checkButton, SIGNAL("clicked()"), self.startCheck)
		self.syncButton = QPushButton(_fromUtf8("&Push table"))
		self.buttonBox.addButton(self.syncButton, QDialogButtonBox.ActionRole)
		self.syncButton.setText(QApplication.translate("DbManagerDlgPushTable",
			"&Push table", None, QApplication.UnicodeUTF8))
		self.connect(self.syncButton, SIGNAL("clicked()"), self.startSync)


		# *PKField is hidden for regular tables, only show when view is source
		self.labelPKField.hide()
		self.cboPKField.hide()

		# initialisation of self.databases
		self.populateDatabases()

		# updates of UI
		self.connect(self.cboDatabase, SIGNAL("currentIndexChanged(int)"), self.populateData)
		self.connect(self.cboSchema, SIGNAL("currentIndexChanged(int)"), self.populateTables)

		self.connect(self.cboDatabase, SIGNAL("currentIndexChanged(int)"), self.disableSyncButton)
		self.connect(self.cboSchema, SIGNAL("currentIndexChanged(int)"), self.disableSyncButton)
		self.connect(self.cboTable, SIGNAL("currentIndexChanged(int)"), self.disableSyncButton)
		self.disableSyncButton()

	def disableSyncButton(self):
		self.syncButton.setEnabled(False)
		self.chboxLockTables.setChecked(False)
		self.chboxLockTables.setEnabled(False)

	# used as callbacks for worker threads
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

	def dbs(self):
		return self.databases[self.cboDatabase.currentText()][1]

	def populateData(self):
		"""Called on change of database listbox in UI."""
		self.clearMessages()
		if not self.cboDatabase.currentText():
			# do nothing
			return
		if self.databases[self.cboDatabase.currentText()][1]:
			# data already scanned
			self.updateDataUI()
			return

		connection = self.databases[self.cboDatabase.currentText()][0]
		connection_error = ""
		for i in [1]:
			if connection.database() == None:
				try:
					# connect to database
					if not connection.connect():
						connection_error = self.tr("Database error") + ": " + self.tr("unable to connect to ") + connection.connectionName()
						# ": " is there so at least something is in error even in case of failure in self.tr
						break
				except BaseError, e:
					connection_error = self.tr("Unable to connect to ") + connection.connectionName() + " " + unicode(e)
					break
			if not connection.database().connector.hasComparatorSupport():
				connection_error = self.tr("Database: ") + connection.connectionName() + self.tr(" doesn't have pg_comparator support.")
				break
		if connection_error:
			self.printMessage(connection_error)
			self.printMessage(self.tr("Not scannig this DB."))
			self.updateDataUI()
			return

		# Scan DB connection for compatible tables (in new thread).
		# disable all controls
		self.enableControls(False)
		self.scanner = DBScanForPushCompatibleTables(
						self.input_table_ref,
						self.tr,
						databaseConnection = self.databases[self.cboDatabase.currentText()][0])
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
		"""Called by scanner thread as result consumer."""
		dbs, = data
		try:
			self.databases[dbs.get_connection_name_when_onlyone()][1] = dbs
		except Exception, e:
			self.printMessage(self.tr("Error scanning db:%s") % unicode(e))
		self.updateDataUI()

	def updateDataUI(self):
		"""UI refreshing.

		enable/disable, populate dependent controls.
		"""
		# enable all controls
		self.enableControls(True)
		if not self.dbs() or self.dbs().is_empty():
			self.printMessage(self.tr("No compatible table to push to found in this DB"))
			self.cboSchema.clear()
			self.cboTable.clear()
			self.cboSchema.setEnabled(False)
			self.cboTable.setEnabled(False)
			self.checkButton.setEnabled(False)
			return

		self.printMessage(self.tr("%d compatible tables to push to found in this DB" % self.dbs().tables_count()))

		self.populateSchemas()
		self.populateTables()
		if self.input_table_ref.is_view():
			self.labelPKField.show()
			self.cboPKField.show()
			self.cboPKField.clear()
			for f in  list(self.input_table_ref.fields()):
				self.cboPKField.addItem(f.field_name)
			# TODO: candidates can be pre-computed
			# Do not allow any misunderstanding on user side by preselecting
			# any field.
			self.cboPKField.setCurrentIndex(-1)
			# disable "Check" button until any field is selected
			self.checkButton.setEnabled(False)
			self.checkButton.connect(self.cboPKField, SIGNAL("currentIndexChanged(int)"), lambda: self.checkButton.setEnabled(True))
			# disable "Sync" button on every change of PK field
			self.checkButton.connect(self.cboPKField, SIGNAL("currentIndexChanged(int)"), lambda: self.syncButton.setEnabled(False))

	def populateDatabases(self):
		"""Initialisation function called only from constructor.

		Initialises self.databases, which is hash with values
		- self.databases["connection name"] = [ connection_object, DBs_object_with_compatible_tables ]
		  OR
		- self.databases["connection name"] = [ connection_object, None ] if not yet scanned

		This function scans database configuration and creates entry for every configured database,
		scans source database, leaves the rest unscanned. Databases are afterwards lazy scanned on
		select.

		"""
		input_connection = self.inputTable.database().connection()
		try:
			if input_connection.database() == None:
				# connect to database
				if not input_connection.connect():
					raise Exception, self.tr("Unable to connect to source DB: ") + input_connection.connectionName()
				if not input_connection.database().connector.hasComparatorSupport():
					raise Exception, self.tr("source database doesn't have pg_comparator support")
		except Exception, e:
			self.printMessage(self.tr("ERROR: ") + unicode(e))
			QMessageBox.warning( None,
				self.tr("DB error"),
				unicode(e))
			QMetaObject.invokeMethod(self, "close", Qt.QueuedConnection)
			return

		self.enableControls(False)
		self.cboDatabase.clear()
		self.databases = {} # self.databases["connection name"] = [ connection_object, DBs_object_with_compatible_tables ]
		dbpluginclass = createDbPlugin("postgis")
		for connection in dbpluginclass.connections():
			self.databases[connection.connectionName()] = [connection, None]
			self.cboDatabase.addItem(connection.connectionName())

		input_database = DBs(print_message_callback = self.printMessage, tr = self.tr)
		self.printMessage(self.tr("Scanning source database"))
		input_database.add_and_scan(input_connection)
		self.input_table_ref = input_database.get_table(input_connection.connectionName(), self.inputTable.schemaName(), self.inputTable.name)
		self.databases[input_connection.connectionName()][1] = input_database.get_compatible_tables_by_ref(self.input_table_ref)
		if self.databases[input_connection.connectionName()][1].is_empty():
			self.printMessage(self.tr("No compatible table found in source database"))
		self.cboDatabase.setEnabled(True)
		QApplication.restoreOverrideCursor()
		self.cboDatabase.setCurrentIndex(-1)

	def populateSchemas(self):
		"""Called by updateDataUI."""
		self.cboSchema.clear()
		if not self.dbs():
			return
		schemas = self.dbs().get_schema_names_for_db_connection(self.cboDatabase.currentText())
		if schemas:
			for schema in schemas:
				self.cboSchema.addItem(schema)
		self.cboSchema.setEnabled(bool(schemas))
		self.cboSchema.setCurrentIndex(0 if schemas else -1)

	def populateTables(self):
		"""Called by updateDataUI and on schema combobox change."""
		self.cboTable.clear()
		if not self.dbs():
			return
		tables = self.dbs().get_table_names_for_db_schema(self.cboDatabase.currentText(), self.cboSchema.currentText())
		if tables:
			for table in tables:
				self.cboTable.addItem(table)
		self.cboTable.setEnabled(bool(tables))
		self.cboTable.setCurrentIndex(0 if tables else -1)

	def get_pg_arguments(self):
		"""Returns (inputUri,outputUri,lockTables) for pg_comparator based on what is selected now in UI."""
		db = self.cboDatabase.currentText()
		pushDiffSchemaName = self.cboSchema.currentText()
		pushDiffTableName = self.cboTable.currentText()
		if not db or not pushDiffSchemaName or not pushDiffTableName:
			# should never happen. valid selection is invariant
			QMessageBox.warning(None,
				self.tr("Push table"),
				self.tr("Nowhere to push table to - select table"))
			return (None, None)

		output_table = self.dbs().get_table(db, pushDiffSchemaName, pushDiffTableName)
		force_pk = [ self.cboPKField.currentText() ] if self.input_table_ref.is_view() else None
		return (self.input_table_ref, output_table, self.chboxLockTables.isChecked(), force_pk)

	def startCheck(self):
		"""Spawns working thread for pg_comparator diff (in our UI called "check")."""
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
		"""Called to signal the check threads return."""
		self.enableControls(True)
		if success:
			self.printMessage("")
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
		"""Spawns working thread for pg_comparator sync."""
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
		"""Called to signal the sync threads return."""
		self.enableControls(True)
		self.syncButton.setEnabled(False)
		# QMessageBox.information(self, self.tr("Push table"), self.tr("%s while pushing table: inserts :%d  updates: %d  deletes: %d") %
		# 	("No error" if success else "Error", inserts, updates, deletes))
		self.printMessage("")
		self.printMessage(self.tr("%s while pushing table: inserts :%d  updates: %d  deletes: %d") %
			("Success" if success else "Error", inserts, updates, deletes))
