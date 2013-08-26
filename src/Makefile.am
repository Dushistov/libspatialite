
SUBDIRS = headers \
	gaiaaux \
	gaiaexif \
	gaiageo \
	geopackage \
	spatialite \
	shapefiles \
	srsinit \
	virtualtext \
	wfs \
	dxf \
	md5

AM_CPPFLAGS = @CFLAGS@
AM_CPPFLAGS += -I$(top_srcdir)/src/headers
AM_CPPFLAGS += -I$(top_srcdir)

lib_LTLIBRARIES = libspatialite.la spatialite.la

libspatialite_la_SOURCES = versioninfo/version.c

libspatialite_la_LIBADD = ./gaiaaux/libgaiaaux.la \
	./gaiaexif/libgaiaexif.la \
	./gaiageo/libgaiageo.la \
	./geopackage/libgeopackage.la \
	./spatialite/libsplite.la \
	./shapefiles/libshapefiles.la \
	./dxf/libdxf.la \
	./md5/libmd5.la \
	./srsinit/libsrsinit.la \
	./virtualtext/libvirtualtext.la \
	./wfs/libwfs.la @LIBXML2_LIBS@

if MINGW
libspatialite_la_LDFLAGS = -version-info 4:1:0 -no-undefined
else 
if ANDROID
libspatialite_la_LDFLAGS = -version-info 6:0:1
libspatialite_la_LIBADD += -ldl
else
libspatialite_la_LDFLAGS = -version-info 6:0:1
libspatialite_la_LIBADD += -lpthread -ldl
endif
endif

spatialite_la_SOURCES = versioninfo/version.c

spatialite_la_LIBADD = ./gaiaaux/gaiaaux.la \
	./gaiaexif/gaiaexif.la \
	./gaiageo/gaiageo.la \
	./geopackage/geopackage.la \
	./spatialite/splite.la \
	./shapefiles/shapefiles.la \
	./dxf/dxf.la \
	./md5/md5.la \
	./srsinit/srsinit.la \
	./virtualtext/virtualtext.la \
	./wfs/wfs.la @LIBXML2_LIBS@

spatialite_la_CPPFLAGS = @CFLAGS@
spatialite_la_CPPFLAGS += -I$(top_srcdir)/src/headers -I.
spatialite_la_CPPFLAGS += -DLOADABLE_EXTENSION
spatialite_la_LIBTOOLFLAGS = --tag=disable-static

if MINGW
spatialite_la_LDFLAGS = -module -version-info 4:1:0 -no-undefined
else 
if ANDROID
spatialite_la_LDFLAGS = -module -version-info 6:0:1
spatialite_la_LIBADD += -ldl
else
spatialite_la_LDFLAGS = -module -version-info 6:0:1
spatialite_la_LIBADD += -lpthread -ldl
endif
endif

MOSTLYCLEANFILES = *.gcna *.gcno *.gcda
