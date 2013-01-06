/*

 check_add_rt_metadata_triggers_wrong_arg_type.c - Test case for GeoPackage Extensions

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

The Original Code is GeoPackage extensions

The Initial Developer of the Original Code is Brad Hards
 
Portions created by the Initial Developer are Copyright (C) 2011
the Initial Developer. All Rights Reserved.

Contributor(s):


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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <sqlite3.h>
#include <spatialite.h>

#include "test_helpers.h"

int main (int argc UNUSED, char *argv[] UNUSED)
{
    sqlite3 *db_handle = NULL;
    int ret;
    char *err_msg = NULL;
    void *cache = spatialite_alloc_connection();

    ret = sqlite3_open_v2 (":memory:", &db_handle, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    // For debugging / testing if required
    // ret = sqlite3_open_v2 ("check_add_rt_metadata_triggers_wrong_arg_type.sqlite", &db_handle, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    spatialite_init_ex(db_handle, cache, 0);
    if (ret != SQLITE_OK) {
      fprintf (stderr, "cannot open in-memory db: %s\n", sqlite3_errmsg (db_handle));
      sqlite3_close (db_handle);
      db_handle = NULL;
      return -1;
    }

    /* create a minimal raster_columns table */
    ret = sqlite3_exec (db_handle, "DROP TABLE IF EXISTS raster_columns", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK) {
      fprintf (stderr, "DROP raster_columns error: %s\n", err_msg);
      sqlite3_free (err_msg);
      return -4;
    }
    ret = sqlite3_exec (db_handle, "CREATE TABLE raster_columns (r_table_name TEXT NOT NULL, r_raster_column TEXT NOT NULL, srid INTEGER NOT NULL DEFAULT 0, CONSTRAINT pk_rc PRIMARY KEY (r_table_name, r_raster_column) ON CONFLICT ROLLBACK, CONSTRAINT fk_rc_r_srid FOREIGN KEY (srid) REFERENCES spatial_ref_sys(srid))", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK) {
      fprintf (stderr, "CREATE raster_columns error: %s\n", err_msg);
      sqlite3_free (err_msg);
      return -5;
    }

    /* add in a test entry */
    ret = sqlite3_exec (db_handle, "INSERT INTO raster_columns VALUES (\"test1_matrix_tiles\", \"tile_data\", 4326)",  NULL, NULL, &err_msg);
    if (ret != SQLITE_OK) {
      fprintf (stderr, "INSERT raster_columns error: %s\n", err_msg);
      sqlite3_free (err_msg);
      return -6;
    }

    /* create a target table */
    ret = sqlite3_exec (db_handle, "DROP TABLE IF EXISTS test1_matrix_tiles", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK) {
      fprintf (stderr, "DROP test1_matrix_tiles error: %s\n", err_msg);
      sqlite3_free (err_msg);
      return -7;
    }
    ret = sqlite3_exec (db_handle, "CREATE TABLE test1_matrix_tiles (id INTEGER PRIMARY KEY AUTOINCREMENT, zoom_level INTEGER NOT NULL DEFAULT 0, tile_column INTEGER NOT NULL DEFAULT 0, tile_row INTEGER NOT NULL DEFAULT 0, tile_data BLOB NOT NULL DEFAULT (zeroblob(4)))", NULL, NULL, &err_msg);
    if (ret != SQLITE_OK) {
      fprintf (stderr, "CREATE test1_matrix_tiles error: %s\n", err_msg);
      sqlite3_free (err_msg);
      return -8;
    }
    
    /* test trigger setup */
    ret = sqlite3_exec (db_handle, "SELECT gpkgAddRtMetadataTriggers(0)", NULL, NULL, &err_msg);
    if (ret != SQLITE_ERROR) {
	fprintf(stderr, "Unexpected gpkgAddRtMetadataTriggers result: %i\n", ret);
	return -9;
    }
    if (strcmp(err_msg, "gpkgAddRtMetadataTriggers() error: argument 1 [table] is not of the String type") != 0) {
      fprintf (stderr, "SELECT gpkgAddRtMetadataTriggers unexpected error message: %s\n", err_msg);
      sqlite3_free (err_msg);
      return -10;
    }
    sqlite3_free(err_msg);
    
    ret = sqlite3_close (db_handle);
    if (ret != SQLITE_OK) {
        fprintf (stderr, "sqlite3_close() error: %s\n", sqlite3_errmsg (db_handle));
	return -200;
    }
    
    spatialite_cleanup_ex(cache);
    
    return 0;
}
