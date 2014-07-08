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

from PyQt4.QtCore import pyqtSignal, pyqtSlot, QObject
from subprocess import Popen, PIPE, STDOUT
from .... import createDbPlugin
from ....plugin import BaseError
from .model import DBs

# pg_comparator --max-ratio argument. Value 2.0 means all lines can be different.
PG_COMPARE_MAX_RATIO = 2.0

class PGComparatorWorker(QObject):
	"""Worker object to call pg_comparator (threadable).

	PGComparatorWorker runs pg_comparator on aruments supplied in constructor,
	emits its messages through "printMessage" and "clearMessages", emits its result
	in "synced".
	If You want to do pg_comparator diff, call (or bind on thread started) check().
	If You want to do pg_comparator sync, call (or bind on thread started) sync().
	PGComparatorWorker can be used as object moved to its own QThread,
	emits "finished" just before exit."""
	finished = pyqtSignal()
	printMessage = pyqtSignal('QString')
	clearMessages = pyqtSignal()
	synced = pyqtSignal(bool, int, int, int, bool)	# success, INSERTs, UPDATEs, DELETEs, has SELECT;INSERT;UPDATE;DELETE privileges

	def __init__(self, inputTable, outputTable, lock, force_pk, tr):
		"""inputTable: table You want to push from
		outputTable: table You want to push to
		lock: do locking or not ?
		force_pk: if supplied, interpreted as list of column names to be used as pg_comparator keys
		tr: is translator function
		"""
		QObject.__init__(self)
		self.inputTable = inputTable
		self.outputTable = outputTable
		self.lock = lock
		self.force_pk = force_pk
		self.tr = tr	# TODO: i hope it doesn't alter any state, so is threadsafe - check this
						# (can be caching on-demand translating)

	@pyqtSlot()
	def check(self):
		"""Call pg_comparator without sync option (no changes in DB)."""
		self.process(False)

	@pyqtSlot()
	def sync(self):
		"""Call pg_comparator with sync option."""
		self.process(True)

	@pyqtSlot(bool)
	def process(self, do_it=False):
		"""calls pg_comparator with values supplied in constructor.
		do_it means do sync, otherwise do diff.
		"""
		pg_call = ["pg_comparator", "--no-lock", "--max-ratio",
					str(PG_COMPARE_MAX_RATIO),
					self.inputTable.pg_comparator_connect_string(force_pk = self.force_pk),
					self.outputTable.pg_comparator_connect_string(force_pk = self.force_pk)]
		if do_it:
			pg_call[4:4] = ["-S", "-D"]
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
			error_message = unicode(e)

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
	"""Scanning object for compatible tables (threadable).

	DBScanForPushCompatibleTables' slot "process" scans in constructor supplied database
	(or all configured if none supplied) for table pg_comparator-push-compatible to supplied one.
	Emits its messages through "printMessage" and "clearMessages", emits its result
	in "dbDataCreated" as model.DBs.
	DBScanForPushCompatibleTables can be used as object moved to its own QThread,
	emits "finished" just before exit."""
	finished = pyqtSignal()
	dbDataCreated = pyqtSignal(tuple)
	printMessage = pyqtSignal('QString')
	clearMessages = pyqtSignal()
	def __init__(self, input_table_ref, tr, databaseConnection = None):
		"""input_table_ref: input table You want to find compatibles
		databaseConnection: if argument is set, search only in given database
		tr: is translator function
		"""
		QObject.__init__(self)
		self.input_table_ref = input_table_ref
		self.tr = tr	# TODO: i hope it doesn't alter any state, so is threadsafe - check this
						# (can be caching on-demand translating)
		self.databaseConnection = databaseConnection

	@pyqtSlot()
	def process(self):
		"""Walks through all connections/supplied connection (based on databaseConnection argument),
		scans database information (schemas/tables/columns/primary fields) by means of
		model.DBs.add_and_scan, then filters push compatible tables
		by model.DBs.get_compatible_tables_by_ref and emits dbDataCreated with resulting DBs
		"""
		self.clearMessages.emit()
		self.compatible_connections = None
		self.input_table = None
		# enclose all code in try/except, so as to finish thread in the case of exception
		try:
			if not self.databaseConnection:
				dbpluginclass = createDbPlugin( "postgis" )
			connections = DBs(print_message_callback = self.printMessage.emit, tr = self.tr)
			for connection in [ self.databaseConnection ] if self.databaseConnection else dbpluginclass.connections():
				# TODO: dbpluginclass.connections() might not be threadsafe
				self.printMessage.emit(self.tr("Scanning DB connection %s") % connection.connectionName())
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
					# Getting DB information from connection.connectionName()
					connections.add_and_scan(connection)

			self.compatible_connections = connections.get_compatible_tables_by_ref(self.input_table_ref)

		except Exception, e:
			self.printMessage.emit(self.tr("ERROR while scanning DB: ") + unicode(e))
			# self.printMessage.emit(traceback.format_exc(e))
			self.compatible_connections = None
		finally:
			self.printMessage.emit(self.tr("Scanning for tables finished."))
			self.printMessage.emit("")
			self.dbDataCreated.emit( (self.compatible_connections,) )
			self.finished.emit()

