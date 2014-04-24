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

import qgis.core
from qgis.utils import iface

from .ui.ui_DlgPublishTable import Ui_DbManagerDlgPublishTable as Ui_Dialog

class DlgPublishTable(QDialog, Ui_Dialog):

	def __init__(self, inputTable, parent=None):
		QDialog.__init__(self, parent)
		self.inputTable = inputTable

		self.setupUi(self)

		self.default_pk = "id"
		self.default_geom = "geom"

		self.populateDatabases()
		self.populateSchemas()
		self.populateTables()

		# updates of UI
		self.connect(self.cboDatabase, SIGNAL("currentIndexChanged(int)"), self.populateSchemas)
		self.connect(self.cboSchema, SIGNAL("currentIndexChanged(int)"), self.populateTables)
		

	# not used - for future reference
##	def checkSupports(self):
##		""" update options available for the current input layer """
##		allowSpatial = self.db.connector.hasSpatialSupport()
##		hasGeomType = self.inLayer and self.inLayer.hasGeometryType()
##		isShapefile = self.inLayer and self.inLayer.providerType() == "ogr" and self.inLayer.storageType() == "ESRI Shapefile"
##		self.chkGeomColumn.setEnabled(allowSpatial and hasGeomType)
##		self.chkSourceSrid.setEnabled(allowSpatial and hasGeomType)
##		self.chkTargetSrid.setEnabled(allowSpatial and hasGeomType)
##		self.chkSinglePart.setEnabled(allowSpatial and hasGeomType and isShapefile)
##		self.chkSpatialIndex.setEnabled(allowSpatial and hasGeomType)

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
						print "can't connect to ", connection
						# return False
				except BaseError, e:
					QMessageBox.warning( None, self.tr("Unable to connect"), unicode(e) )
					return False
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

		tables = schema.tables()
		skipTableUri = self.inputTable.uri().uri()
		for table in tables:
			if table.uri().uri() == skipTableUri:
				continue
			self.tableName2table[table.name] = table
			self.cboTable.addItem(table.name)

		self.cboTable.setCurrentIndex(0 if tables else -1)


	def accept(self):
		dbi = self.cboDatabase.currentIndex()
		publishSchema = self.cboSchema.currentText()
		publishTable = self.cboTable.currentText()
		if dbi <0 or not publishSchema or not publishTable:
			output = qgis.gui.QgsMessageViewer()
			output.setTitle( self.tr("Publish to database") )
			output.setMessageAsPlainText( self.tr("Nowhere to publish to - select table") )
			output.showMessage()
			return

		# publishDB = self.connections[dbi].database()
		# override cursor
		QApplication.setOverrideCursor(QCursor(Qt.WaitCursor))

		# publishUri = publishDB.uri() 
		# print publishUri.uri()

		# geom = self.tableName2table[publishTable].uri().geometryColumn()

		# publishUri.setDataSource(publishSchema, publishTable, geom)
		publishUri = self.tableName2table[publishTable].uri()
		print publishUri.uri()

		successfull_imports = 0
		failed_imports = 0
		ret = 0
		try:
			# do the sync!
			outputLayer = qgis.core.QgsVectorLayer(publishUri.uri(), "", 'postgres')
			outputLayer.startEditing()
			inputLayer = qgis.core.QgsVectorLayer(self.inputTable.uri().uri(), "", 'postgres')
			for feature in inputLayer.getFeatures():
				# print feature
				if outputLayer.addFeature(feature):
					successfull_imports += 1
				else:
					failed_imports += 1
			outputLayer.commitChanges()

			# options = {}
			# ret, errMsg = qgis.core.QgsVectorLayerImport.importLayer( qgis.core.QgsVectorLayer(self.inputTable.uri(), "", 'postgres'), publishUri.uri(), inputProviderName, None, False, False, options )
		except Exception as e:
			ret = -1
			errMsg = unicode( e )

		finally:

			QApplication.restoreOverrideCursor()

		if ret != 0:
			output = qgis.gui.QgsMessageViewer()
			output.setTitle( self.tr("Publish to database") )
			output.setMessageAsPlainText( self.tr("Error %d\n%s") % (ret, errMsg) )
			output.showMessage()
			return

##		# create spatial index
##		# if self.chkSpatialIndex.isEnabled() and self.chkSpatialIndex.isChecked():
##		# 	self.db.connector.createSpatialIndex( (schema, table), geom )

		QMessageBox.information(self, self.tr("Publish to database"), self.tr("Publish: successful :%d  failed: %d") % (successfull_imports, failed_imports))
		return QDialog.accept(self)

