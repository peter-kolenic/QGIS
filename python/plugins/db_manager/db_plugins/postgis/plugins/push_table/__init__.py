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

from PyQt4.QtGui import QApplication
from subprocess import call, PIPE, STDOUT

# The load function is called when the "db" database or either one of its
# children db objects (table o schema) is selected by the user.
# @param db is the selected database
# @param mainwindow is the DBManager mainwindow
def load(db, mainwindow):
	pass
	# # add the action to the DBManager menu
	# action = QAction( QIcon(), QApplication.translate("DBManagerPlugin", "&Push2"), db )
	# mainwindow.registerAction( action, QApplication.translate("DBManagerPlugin", "&Table"), run )


# The run function is called once the user clicks on the action "Push table"
# (look above at the load function) from the DBManager menu/toolbar.
# @param item is the selected db item (either db, schema or table)
# @param action is the clicked action on the DBManager menu/toolbar
# @param mainwindow is the DBManager mainwindow
def run(item, action, mainwindow):
	from .dlg_push_table import DlgPushTable
	dlg = DlgPushTable( item, mainwindow )

	QApplication.restoreOverrideCursor()
	try:
		dlg.exec_()
	finally:
		QApplication.setOverrideCursor(Qt.WaitCursor)

def check_pg_comparator_presence():
	# Utility function for checking for presence of pg_comparator
	retcode = 0
	try:
		retcode = call(["pg_comparator", "--help"], stdin=PIPE, stdout=PIPE, stderr=STDOUT, shell=False)
	except OSError as e:
		retcode = -1
	return retcode == 0
