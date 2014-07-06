ADD_SUBDIRECTORY(qgis_topoview)
ADD_SUBDIRECTORY(versioning)
ADD_SUBDIRECTORY(push_table)

PLUGIN_INSTALL(db_manager db_plugins/postgis/plugins __init__.py)
