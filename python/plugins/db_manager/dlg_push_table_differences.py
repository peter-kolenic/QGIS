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
from subprocess32 import Popen, PIPE, STDOUT

import qgis.core
# from qgis.utils import iface

from .ui.ui_DlgPushTableDifferences import Ui_DbManagerDlgPushTableDifferences as Ui_Dialog
from .ui.ui_DlgPushTableDifferences import _fromUtf8
from .db_plugins.plugin import BaseError
from db_plugins import createDbPlugin # if db_manager.tree is used, remove this

PG_COMPARE_MAX_RATIO=1
class DlgPushTableDifferences(QDialog, Ui_Dialog):

	def __init__(self, inputTable, parent=None):
		QDialog.__init__(self, parent)
		self.inputTable = inputTable

		if not [ f for f in self.inputTable.fields() if f.primaryKey ]:
			QMessageBox.warning( None, self.tr("Table error"),self.tr("unable to push differences - table doesn't have primary key column"))
			QMetaObject.invokeMethod(self,"close",Qt.QueuedConnection)

		self.setupUi(self)
		self.checkButton = QPushButton(_fromUtf8("Check"));
		self.buttonBox.addButton(self.checkButton,QDialogButtonBox.ActionRole)
		self.checkButton.setText(QApplication.translate("DbManagerDlgPushTableDifferences", "Check", None, QApplication.UnicodeUTF8))
		self.connect(self.checkButton, SIGNAL("clicked()"), self.check)
		self.syncButton = QPushButton(_fromUtf8("&Sync"));
		self.buttonBox.addButton(self.syncButton,QDialogButtonBox.ActionRole)
		self.syncButton.setText(QApplication.translate("DbManagerDlgPushTableDifferences", "Sync", None, QApplication.UnicodeUTF8))
		self.connect(self.syncButton, SIGNAL("clicked()"), self.sync)

		self.populateData()
		self.populateDatabases()
		self.populateSchemas()
		self.populateTables()

		# updates of UI
		self.connect(self.cboDatabase, SIGNAL("currentIndexChanged(int)"), self.populateSchemas)
		self.connect(self.cboSchema, SIGNAL("currentIndexChanged(int)"), self.populateTables)

		self.connect(self.cboDatabase, SIGNAL("currentIndexChanged(int)"), self.disableSyncButton)
		self.connect(self.cboSchema, SIGNAL("currentIndexChanged(int)"), self.disableSyncButton)
		self.connect(self.cboTable, SIGNAL("currentIndexChanged(int)"), self.disableSyncButton)
		self.disableSyncButton()

	def emitText(self, text, clear=False):
		[self.plainTextEdit.setPlainText,self.plainTextEdit.appendPlainText][0 if clear else 1](text)

	def populateData(self):
		# XXX
		# pouzit nieco ako [ q.data(0) for q in self.parent().tree.model().rootItem.children() ] == ['PostGIS', 'SpatiaLite']
		# DBManager.(DBTree)tree.setModel(DBModel(mainWindow=DBManager)).PluginItem()
		# negenerovat znova
		# a vysomarit sa z toho, kde v strome su ulozene vyrobene konekcie (ak teda sa mozeme spolahnut na to, ze su populated, co mozno nie)
		# urobit funkciu vrat vsetky db konekcie do niekam, tu to fakt nema co robit
		# a najlepsie populivat naraz, nie opakovane (co ak remote konekcia a 100000 tabuliek ?)

		# alebo, mozno urobit miesto 3 combobox-ov strom kompatibilnych tabuliek (ak by napriklad 10000 DB konekcii, aby sa nepopulovali
		# naraz - podobne ako db_manager.tree (myslim ze sa nepopuluju naraz ale on demand)

		# data is stored in self.connections, in structure:
		#	self.connection = [ (connection, schemas )]
		#                                    schemas = { name: (schema, compatible_tables) }
		#                                                               compatible_tables = { table_name: (table, commonPK) }
		inputTableUri = self.inputTable.uri().uri()
		inputTableFieldsDefs = [ (f.name, f.dataType ) for f in self.inputTable.fields() ] # not using more precise f.definition(), because sequencer name 
																										 # differs, and is part of fields default value
		inputTablePKs = frozenset([ f.name for f in self.inputTable.fields() if f.primaryKey])
		self.connections = []
		dbpluginclass = createDbPlugin( "postgis" )
		for connection in dbpluginclass.connections():
			self.emitText(self.tr("Checking DB connection %s") % connection.connectionName())
			if connection.database() == None:
				# connect to database
				try:
					if not connection.connect():
						# QMessageBox.warning( None, self.tr("Database connection error"),self.tr("Unable to connect to ") + connection.connectionName() )
						self.emitText(self.tr("Database connection error ") + self.tr("Unable to connect to ") + connection.connectionName() )
						continue
				except BaseError, e:
					# QMessageBox.warning( None, self.tr("Unable to connect to ") + connection.connectionName(), unicode(e) )
					self.emitText(self.tr("Unable to connect to ") + connection.connectionName() + " " + unicode(e) )
					continue
			if connection.database().connector.hasComparatorSupport():
				schemas = {}
				db = connection.database()
				schemas_ = db.schemas()
				for schema in schemas_:
					self.emitText(self.tr("Checking schema %s in connection %s") % (schema.name,connection.connectionName()))
					tables = {}
					tables_ = schema.tables()
					for table in tables_:
						if table.uri().uri() == inputTableUri:
							self.emitText(self.tr("Table %s is source table - skipping") % table.name)
							continue # skip source
						fieldsDefs = [ (f.name, f.dataType) for f in table.fields() ]
						if fieldsDefs != inputTableFieldsDefs:
							self.emitText(self.tr("Table %s is not compatible - skipping") % table.name)
							continue
						tablePKs = frozenset([f.name for f in table.fields() if f.primaryKey])	
						commonPKs = set(tablePKs.intersection(inputTablePKs))
						# if the check of primaryKey were done on "Check",
						# user would be able to find out when the table is ill created
						if not commonPKs:
							self.emitText(self.tr("WARNING: Table %s is fields-compatible, but has no common primary key with source table - skipping") % table.name)
							continue
						tables[table.name] = (table,commonPKs.pop())
						self.emitText(self.tr("Compatible table %s found in schema %s in connection %s") % (table.name,schema.name,connection.connectionName()))
					if tables:
						schemas[schema.name] = (schema, tables)
					else:
						self.emitText(self.tr("Skipping schema %s, no compatible table") % schema.name)
				if schemas:
					self.connections.append((connection, schemas))
				else:
					self.emitText(self.tr("Skipping connection %s, no compatible table in its schemas") % connection.connectionName())
			else:
				self.emitText(self.tr("Skipping connection %s, no pg_comparator support") % connection.connectionName())
		if not self.connections:
			self.emitText(self.tr("No compatible tables found in any database"))
		self.emitText(self.tr("Scanning for tables finished."))
		self.emitText("")


	def disableSyncButton(self):
		self.syncButton.setEnabled(False)

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
		if not self.connections or schi <0:
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

	# XXX fix this - should only return selected values, should not show error dialog. maybe some valid selection can be invariant
	def get_pg_arguments(self):
		dbi = self.cboDatabase.currentIndex()
		pushDiffSchema = self.cboSchema.currentText()
		pushDiffTableName = self.cboTable.currentText()
		if dbi <0 or not pushDiffSchema or not pushDiffTableName:
			output = qgis.gui.QgsMessageViewer()
			output.setTitle( self.tr("Push differences") )
			output.setMessageAsPlainText( self.tr("Nowhere to push differences to - select table") )
			output.showMessage()
			return (None,None)

		def pg_comparator_connect_string_for_table(table,pk):
			# XXX escale @ and " in password
			uri = table.uri()
			s = "pgsql://%(login)s:%(pass)s@%(host)s:%(port)s/%(base)s/%(schema_table)s?\"%(pk)s\"" % {
				"login":uri.username(),
				"pass":	uri.password(),
				"host":	uri.host(),
				"port":	uri.port(),
				"base":	uri.database(),
				"schema_table":uri.quotedTablename(),
				"pk": pk,
			}
			print "PG_CONNECT:", s
			return s

		pushDiffTable = self.connections[dbi][1][pushDiffSchema][1][pushDiffTableName][0]
		pk = self.connections[dbi][1][pushDiffSchema][1][pushDiffTableName][1]
		pg_inputTable = pg_comparator_connect_string_for_table(self.inputTable,pk)
		pg_outputTable = pg_comparator_connect_string_for_table(pushDiffTable,pk)
		return (pg_inputTable,pg_outputTable)

	def sync(self):
		(pg_inputTable,pg_outputTable) = self.get_pg_arguments()
		if not ( pg_inputTable and pg_outputTable):
			return
		QApplication.setOverrideCursor(QCursor(Qt.WaitCursor))

		retcode = 0
		outp = "pg_compare:\n"
		outp2=""
		err=""
		errMsg = ""
		try:
			with Popen(["pg_comparator","-S","-D","--max-ratio",str(PG_COMPARE_MAX_RATIO),pg_inputTable,pg_outputTable],bufsize=1,shell=False,stdout=PIPE,stderr=STDOUT,universal_newlines=True) as p:
				for l in iter(p.stdout.readline,''):
					outp+=l
					self.emitText(outp)
				(outp2,err) = p.communicate()
				retcode = p.returncode
		except Exception as e:
			print "Exc", e
			retcode = -1
			errMsg = unicode( e )

		finally:
			QApplication.restoreOverrideCursor()

		text = "pg_comparator push differences finished succesfully" if retcode == 0 else "ERROR: pg_comparator push differences returned errnum: %d" % retcode
		if outp2:
			text += "\nSTDOUT:\n"
			text += outp
		if err:
			text += "\nSTDERR:\n"
			text += err
		self.emitText(outp + text)
		inserts = len([ l for l in outp.split("\n") if l.startswith("INSERT")])
		updates = len([ l for l in outp.split("\n") if l.startswith("UPDATE")])
		deletes = len([ l for l in outp.split("\n") if l.startswith("DELETE")])

		QMessageBox.information(self, self.tr("Push differences"), self.tr("%s while pushing differences: inserts :%d  updates: %d  deletes: %d") % 
			(("No error" if retcode == 0 else ("Error[%d]" % retcode)),inserts,updates,deletes))
		# return QDialog.accept(self) # XXX or continue with synch to another table ?

	def check(self):
		(pg_inputTable,pg_outputTable) = self.get_pg_arguments()
		if not ( pg_inputTable and pg_outputTable):
			return
		QApplication.setOverrideCursor(QCursor(Qt.WaitCursor))

		retcode = 0
		outp = "pg_compare:\n"
		outp2=""
		err=""
		errMsg = ""
		try: # XXX musi to ist v inom threade. jednak aby sa to dalo zabit, a dvak aby sa nieco 
			# zobrazovalo (zjavne to ide v tom istom threade ako nejaka cast renderingu). potom budem moct citat error a output osobitne, a dam ine farby
			with Popen(["pg_comparator","--max-ratio",str(PG_COMPARE_MAX_RATIO),pg_inputTable,pg_outputTable],bufsize=1,shell=False,stdout=PIPE,stderr=STDOUT,universal_newlines=True) as p:
				for l in iter(p.stdout.readline,''):
					outp+=l
					self.emitText(outp)
				(outp2,err) = p.communicate()
				retcode = p.returncode
		except Exception as e:
			print "Exc", e
			retcode = -1
			errMsg = unicode( e )

		finally:
			QApplication.restoreOverrideCursor()

		text = "pg_comparator check finished succesfully" if retcode == 0 else "ERROR: pg_comparator check returned errnum: %d" % retcode
		if retcode == 0:
			self.syncButton.setEnabled(True)
			# self.buttonBox.setStandardButtons(QDialogButtonBox.Cancel)
		if outp2:
			text += "\nSTDOUT:\n"
			text += outp
		if err:
			text += "\nSTDERR:\n"
			text += err
		outp += text
		self.emitText(outp)

def get_primarykey_for_table(table):
	keys = [ f.name for f in table.fields() if f.primaryKey ]
	return keys[0] if keys else None

def check_pg_comparator_presence():
	retcode = 0
	try: 
		with Popen(["pg_comparator","--help"],shell=False) as p:
			(outp2,err) = p.communicate()
			retcode = p.returncode
	except Exception as e:
		retcode = -1
	return retcode == 0


