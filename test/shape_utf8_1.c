/*

 shape_utf8_1.c -- SpatiaLite Test Case

 Author: Brad Hards <bradh@frogmouth.net>

 ------------------------------------------------------------------------------
 
 Version: MPL 1.1/GPL 2.0/LGPL 2.1
 
 The contents of this file are subject to the Mozilla Public License Version
 1.1 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at
 http://www.mozilla.org/MPL/
 
Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the
License.

The Original Code is the SpatiaLite library

The Initial Developer of the Original Code is Alessandro Furieri
 
Portions created by the Initial Developer are Copyright (C) 2011
the Initial Developer. All Rights Reserved.

Contributor(s):
Brad Hards <bradh@frogmouth.net>
Ahmadou Dicko <dicko.ahmadou@gmail.com>

Alternatively, the contents of this file may be used under the terms of
either the GNU General Public License Version 2 or later (the "GPL"), or
the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
in which case the provisions of the GPL or the LGPL are applicable instead
of those above. If you wish to allow use of your version of this file only
under the terms of either the GPL or the LGPL, and not to allow others to
use your version of this file under the terms of the MPL, indicate your
decision by deleting the provisions above and replace them with the notice
and other provisions required by the GPL or the LGPL. If you do not delete
the provisions above, a recipient may use your version of this file under
the terms of any one of the MPL, the GPL or the LGPL.
 
*/
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "config.h"

#include "sqlite3.h"
#include "spatialite.h"

void cleanup_shapefile(const char *filename)
{
    char nam[1000];
    
    if (!filename) {
	return;
    }
    
    snprintf(nam, 1000, "%s.dbf", filename);
    unlink(nam);
    snprintf(nam, 1000, "%s.prj", filename);
    unlink(nam);
    snprintf(nam, 1000, "%s.shp", filename);
    unlink(nam);
    snprintf(nam, 1000, "%s.shx", filename);
    unlink(nam);
}

int main (int argc, char *argv[])
{
#ifndef OMIT_ICONV	/* only if ICONV is supported */
    int ret;
    sqlite3 *handle;
    char *dumpname = __FILE__"dump";
    char *err_msg = NULL;
    int row_count;

    spatialite_init (0);
    ret = sqlite3_open_v2 (":memory:", &handle, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    if (ret != SQLITE_OK) {
	fprintf(stderr, "cannot open in-memory database: %s\n", sqlite3_errmsg (handle));
	sqlite3_close(handle);
	return -1;
    }
    
    ret = sqlite3_exec (handle, "SELECT InitSpatialMetadata()", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK) {
	fprintf (stderr, "InitSpatialMetadata() error: %s\n", err_msg);
	sqlite3_free(err_msg);
	sqlite3_close(handle);
	return -2;
    }
    
    ret = load_shapefile (handle, "./shp/gaza/barrier", "barrier", "UTF-8", 4326, 
			  NULL, 1, 0, 1, 1, &row_count, err_msg);
    if (!ret) {
        fprintf (stderr, "load_shapefile() error for shp/gaza/barrier: %s\n", err_msg);
	sqlite3_close(handle);
	return -3;
    }
    if (row_count != 5) {
	fprintf (stderr, "unexpected row count for shp/gaza/barrier: %i\n", row_count);
	sqlite3_close(handle);
	return -4;
    }

    ret = load_shapefile (handle, "./shp/gaza/aeroway", "aeroway", "UTF-8", 4326, 
			  "col1", 1, 0, 0, 0, &row_count, err_msg);
    if (!ret) {
        fprintf (stderr, "load_shapefile() error for shp/gaza/aeroway: %s\n", err_msg);
	sqlite3_close(handle);
	return -5;
    }
    if (row_count != 1) {
	fprintf (stderr, "unexpected row count for shp/gaza/aeroway: %i\n", row_count);
	sqlite3_close(handle);
	return -6;
    }

    ret = load_shapefile (handle, "./shp/gaza/route", "route", "UTF-8", 4326, 
			  NULL, 1, 0, 1, 1, &row_count, err_msg);
    if (!ret) {
        fprintf (stderr, "load_shapefile() error for shp/gaza/route: %s\n", err_msg);
	sqlite3_close(handle);
	return -7;
    }
    if (row_count != 2) {
	fprintf (stderr, "unexpected row count for shp/gaza/route: %i\n", row_count);
	sqlite3_close(handle);
	return -8;
    }

fprintf(stderr, "************************* dump route\n");
    ret = dump_shapefile (handle, "route", "Geometry", dumpname, "UTF-8", "", 1, &row_count, err_msg);
    if (!ret) {
        fprintf (stderr, "dump_shapefile() error for UTF-8_1 route: %s\n", err_msg);
	sqlite3_close(handle);
	return -9;
    }
    cleanup_shapefile(dumpname);
    if (row_count != 2) {
	fprintf (stderr, "unexpected dump row count for UTF-8_1 route: %i\n", row_count);
	sqlite3_close(handle);
	return -10;
    }

fprintf(stderr, "************************* dump route 2\n");
    ret = dump_shapefile (handle, "route", "Geometry", dumpname, "UTF-8", "LINESTRING", 1, &row_count, err_msg);
    if (!ret) {
        fprintf (stderr, "dump_shapefile() error for UTF-8_1 route: %s\n", err_msg);
	sqlite3_close(handle);
	return -13;
    }
    cleanup_shapefile(dumpname);
    if (row_count != 2) {
	fprintf (stderr, "unexpected dump row count for UTF-8_1 route LINESTRING: %i\n", row_count);
	sqlite3_close(handle);
	return -14;
    }

fprintf(stderr, "************************* dump route 3\n");
    ret = sqlite3_exec (handle, "SELECT DiscardGeometryColumn('route', 'Geometry')", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK) {
	fprintf (stderr, "DiscardGeometry route error: %s\n", err_msg);
	sqlite3_free(err_msg);
	sqlite3_close(handle);
	return -15;
    }
fprintf(stderr, "************************* dump route 4\n");

    ret = sqlite3_exec (handle, "INSERT INTO geometry_columns (f_table_name, f_geometry_column, geometry_type, coord_dimension, srid, spatial_index_enabled) VALUES ('beta',  'gamma', 2, 2, 4326, 0)", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK) {
	fprintf (stderr, "GeometryColumns route error: %s\n", err_msg);
	sqlite3_free(err_msg);
	sqlite3_close(handle);
	return -16;
    }

fprintf(stderr, "************************* dump route 5\n");
    ret = sqlite3_exec (handle, "INSERT INTO views_geometry_columns (view_name, view_geometry, view_rowid, f_table_name, f_geometry_column) VALUES ('route',  'Geometry', 'ROWID', 'beta', 'gamma')", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK) {
	fprintf (stderr, "ViewsGeometryColumns route error: %s\n", err_msg);
	sqlite3_free(err_msg);
	sqlite3_close(handle);
	return -17;
    }

fprintf(stderr, "************************* dump route 6\n");
    ret = dump_shapefile (handle, "route", "Geometry", dumpname, "UTF-8", NULL, 1, &row_count, err_msg);
    if (!ret) {
        fprintf (stderr, "dump_shapefile() error for UTF-8_1 route (2): %s\n", err_msg);
	sqlite3_close(handle);
	return -17;
    }
fprintf(stderr, "************************* dump route 7\n");
    cleanup_shapefile(dumpname);
    if (row_count != 2) {
	fprintf (stderr, "unexpected dump row count for UTF-8_1 route LINESTRING (2): %i\n", row_count);
	sqlite3_close(handle);
	return -18;
    }
    
    ret = sqlite3_close (handle);
    if (ret != SQLITE_OK) {
        fprintf (stderr, "sqlite3_close() error: %s\n", sqlite3_errmsg (handle));
	return -19;
    }

    spatialite_cleanup();
#endif	/* end ICONV conditional */

    return 0;
}
