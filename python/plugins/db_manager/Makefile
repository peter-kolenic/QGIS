%_rc.py : %.qrc
	pyrcc4 -o $*_rc.py  $<

ui_%.py : %.ui
	pyuic4 -o $@ $<

RC_FILES_IN:=db_plugins/spatialite/resources.qrc db_plugins/postgis/resources.qrc resources.qrc
RC_FILES=$(RC_FILES_IN:%.qrc=%_rc.py)

UI_FILES_IN:=$(wildcard ui/*.ui) db_plugins/postgis/plugins/versioning/DlgVersioning.ui
UI_FILES=$(patsubst %.ui,%.py,$(foreach f,$(UI_FILES_IN),$(dir $f)ui_$(notdir $f)))

all:	$(RC_FILES) $(UI_FILES)

clean:
	-rm $(UI_FILES) $(RC_FILES) *.pyc ui/*.pyc
