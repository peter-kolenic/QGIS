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

PG_COMPARE_MAX_RATIO=1
class DlgPushTableDifferences(QDialog, Ui_Dialog):

	def __init__(self, inputTable, parent=None):
		QDialog.__init__(self, parent)
		self.inputTable = inputTable

		if not get_primarykey_for_table(self.inputTable):
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


		self.default_pk = "id"
		self.default_geom = "geom"

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

	def disableSyncButton(self):
		self.syncButton.setEnabled(False)

	def populateDatabases(self):
		self.cboDatabase.clear()
		# XXX
		# pouzit nieco ako [ q.data(0) for q in self.parent().tree.model().rootItem.children() ] == ['PostGIS', 'SpatiaLite']
		# DBManager.(DBTree)tree.setModel(DBModel(mainWindow=DBManager)).PluginItem()
		# negenerovat znova
		# a vysomarit sa z toho, kde v strome su ulozene vyrobene konekcie (ak teda sa mozeme spolahnut na to, ze su populated, co mozno nie)
		# urobit funkciu vrat vsetky db konekcie do niekam, tu to fakt nema co robit
		# a najlepsie populivat naraz, nie opakovane (co ak remote konekcia a 100000 tabuliek ?)

		# alebo, mozno urobit miesto 3 combobox-ov strom kompatibilnych tabuliek (ak by napriklad 10000 DB konekcii, aby sa nepopulovali
		# naraz - podobne ako db_manager.tree (myslim ze sa nepopuluju naraz ale on demand)

		# import je tu kvoli poznamke vyssie - nech potom nezostava 
		from db_plugins import createDbPlugin
		self.connections = []
		dbpluginclass = createDbPlugin( "postgis" ) # ? naozaj len postgis ? neprejst radsej vsteky pluginy ?
		for connection in dbpluginclass.connections():
			if connection.database() == None:
				# connect to database
				try:
					if not connection.connect():
						QMessageBox.warning( None, self.tr("Database connection error"),self.tr("Unable to connect to ") + connection.connectionName() )
						continue
				except BaseError, e:
					QMessageBox.warning( None, self.tr("Unable to connect to ") + connection.connectionName(), unicode(e) )
					continue
			if connection.database().connector.hasComparatorSupport():
				self.cboDatabase.addItem(connection.connectionName())
				self.connections.append(connection)
		self.cboDatabase.setCurrentIndex(0 if self.connections else -1)


	def populateSchemas(self):
		self.cboSchema.clear()
		self.schemas = {}
		dbi = self.cboDatabase.currentIndex()
		if dbi >= 0:
			db = self.connections[dbi].database()

			schemas = db.schemas()
			if schemas == None:
				self.self.cboSchema.setEnabled(False)
				return
			else:
				self.cboSchema.setEnabled(True)

			for schema in schemas:
				self.cboSchema.addItem(schema.name)
				self.schemas[schema.name] = schema
		self.cboSchema.setCurrentIndex(0 if self.schemas else -1)

	def populateTables(self):
		self.cboTable.clear()
		self.tableName2table = {}
		schi = self.cboSchema.currentIndex()
		if not self.connections or not self.schemas or schi <0:
			self.cboTable.setCurrentIndex(-1)
			return

		schema = self.schemas[self.cboSchema.currentText()]

		skipTableUri = self.inputTable.uri().uri()
		# inputTableFieldsDefs = [ f.definition() for f in self.inputTable.fields() ]
		# sequencer ma ine meno
		inputTableFieldsDefs = [ (f.name, f.dataType, f.primaryKey ) for f in self.inputTable.fields() ]
		tables = schema.tables()
		for table in tables:
			if table.uri().uri() == skipTableUri:
				continue
			fieldsDefs = [ (f.name, f.dataType, f.primaryKey) for f in table.fields() ]
			if fieldsDefs != inputTableFieldsDefs or not [ f for f in fieldsDefs if f[2] ]:   # or maybe don't check here, but on "Check"
				# print "SKIP:", table.schemaName(), table.name
				continue

			self.tableName2table[table.name] = table
			self.cboTable.addItem(table.name)

		self.cboTable.setCurrentIndex(0 if tables else -1)

	# XXX fix this - should only return selected values, should not show error dialog. maybe some valid selection can be invariant
	def get_pg_arguments(self):
		dbi = self.cboDatabase.currentIndex()
		pushDiffSchema = self.cboSchema.currentText()
		pushDiffTable = self.cboTable.currentText()
		if dbi <0 or not pushDiffSchema or not pushDiffTable:
			output = qgis.gui.QgsMessageViewer()
			output.setTitle( self.tr("Push differences") )
			output.setMessageAsPlainText( self.tr("Nowhere to push differences to - select table") )
			output.showMessage()
			return (None,None)

		pushDiff = self.tableName2table[pushDiffTable]
		pg_inputTable = pg_comparator_connect_string_for_table(self.inputTable)
		pg_outputTable = pg_comparator_connect_string_for_table(pushDiff)
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
			with Popen(["/usr/bin/pg_comparator","-S","-D","--max-ratio",str(PG_COMPARE_MAX_RATIO),pg_inputTable,pg_outputTable],bufsize=1,shell=False,stdout=PIPE,stderr=STDOUT,universal_newlines=True) as p:
				for l in iter(p.stdout.readline,''):
					outp+=l
					self.plainTextEdit.setPlainText(outp)
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
		self.plainTextEdit.setPlainText(outp + text)
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
			with Popen(["/usr/bin/pg_comparator","--max-ratio",str(PG_COMPARE_MAX_RATIO),pg_inputTable,pg_outputTable],bufsize=1,shell=False,stdout=PIPE,stderr=STDOUT,universal_newlines=True) as p:
				for l in iter(p.stdout.readline,''):
					outp+=l
					self.plainTextEdit.setPlainText(outp)
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
		self.plainTextEdit.setPlainText(outp)


def pg_comparator_connect_string_for_table(table):
	# XXX escale @ and " in password
	uri = table.uri()
	pk = get_primarykey_for_table(table)
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

def get_primarykey_for_table(table):
	keys = [ f.name for f in table.fields() if f.primaryKey ]
	return keys[0] if keys else None 
