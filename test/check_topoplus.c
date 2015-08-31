/*

 check_topoplus.c -- SpatiaLite Test Case

 Author: Sandro Furieri <a.furieri@lqt.it>

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

static int
do_level2_tests (sqlite3 * handle, int *retcode)
{
/* performing basic tests: Level 2 */
    int ret;
    char *err_msg = NULL;

/* loading a Point GeoTable */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_FromGeoTable('elba', NULL, 'elba_pg', 'centroid', 0)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "TopoGeo_FromGeoTable() #3 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -100;
	  return 0;
      }

/* loading a Polygon GeoTable */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_FromGeoTable('elba', NULL, 'elba_pg', 'geometry', 0)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "TopoGeo_FromGeoTable() #4 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -101;
	  return 0;
      }

    return 1;
}

static int
do_level1_tests (sqlite3 * handle, int *retcode)
{
/* performing basic tests: Level 1 */
    int ret;
    char *err_msg = NULL;

/* loading a Linestring GeoTable */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_FromGeoTable('elba', NULL, 'elba_ln', NULL, 0)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "TopoGeo_FromGeoTable() #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -80;
	  return 0;
      }

/* loading a Point GeoTable */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_FromGeoTable('elba', NULL, 'elba_pg', 'centroid', 0)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "TopoGeo_FromGeoTable() #2 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -81;
	  return 0;
      }

    return 1;
}

static int
do_level0_tests (sqlite3 * handle, int *retcode)
{
/* performing basic tests: Level 0 */
    int ret;
    char *err_msg = NULL;

/* attempting to load a Topology - non-existing Topology */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_FromGeoTable('wannebe', NULL, 'elba_ln', NULL, 0)",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() non-existing Topology: expected failure\n");
	  *retcode = -50;
	  return 0;
      }
    if (strcmp
	(err_msg, "SQL/MM Spatial exception - invalid topology name.") != 0)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() non-existing Topology: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -51;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to load a Topology - non-existing GeoTable */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_FromGeoTable('elba', NULL, 'wannabe', NULL, 0)",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() non-existing GeoTable: expected failure\n");
	  *retcode = -52;
	  return 0;
      }
    if (strcmp
	(err_msg, "SQL/MM Spatial exception - invalid input GeoTable.") != 0)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() non-existing GeoTable: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -53;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to load a Topology - wrong DB-prefix */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_FromGeoTable('elba', 'lollypop', 'elba_ln', NULL, 0)",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() wrong DB-prefix: expected failure\n");
	  *retcode = -54;
	  return 0;
      }
    if (strcmp
	(err_msg, "SQL/MM Spatial exception - invalid input GeoTable.") != 0)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() wrong DB-prefix: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -55;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to load a Topology - wrong geometry column */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_FromGeoTable('elba', NULL, 'elba_ln', 'none', 0)",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() non-existing Geometry: expected failure\n");
	  *retcode = -56;
	  return 0;
      }
    if (strcmp
	(err_msg, "SQL/MM Spatial exception - invalid input GeoTable.") != 0)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() non-existing Geometry: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -57;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to load a Topology - mismatching SRID */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_FromGeoTable('badelba1', NULL, 'elba_ln', 'geometry', 0)",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() mismatching SRID: expected failure\n");
	  *retcode = -58;
	  return 0;
      }
    if (strcmp
	(err_msg,
	 "SQL/MM Spatial exception - invalid GeoTable (mismatching SRID or dimensions).")
	!= 0)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() mismatching SRID: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -59;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to load a Topology - mismatching dims */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_FromGeoTable('badelba2', NULL, 'elba_ln', 'GEOMETRY', 0)",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() mismatching dims: expected failure\n");
	  *retcode = -60;
	  return 0;
      }
    if (strcmp
	(err_msg,
	 "SQL/MM Spatial exception - invalid GeoTable (mismatching SRID or dimensions).")
	!= 0)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() mismatching dims: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -61;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to load a Topology - ambiguous geometry column */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_FromGeoTable('elba', NULL, 'elba_pg', NULL, 0)",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() ambiguous Geometry: expected failure\n");
	  *retcode = -62;
	  return 0;
      }
    if (strcmp
	(err_msg, "SQL/MM Spatial exception - invalid input GeoTable.") != 0)
      {
	  fprintf (stderr,
		   "TopoGeo_FromGeoTable() ambiguos Geometry: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -63;
	  return 0;
      }
    sqlite3_free (err_msg);

    return 1;
}

int
main (int argc, char *argv[])
{
    int retcode = 0;

#ifdef POSTGIS_2_2		/* only if TOPOLOGY is enabled */
    int ret;
    sqlite3 *handle;
    char *err_msg = NULL;
    void *cache = spatialite_alloc_connection ();
    char *old_SPATIALITE_SECURITY_ENV = NULL;
#ifdef _WIN32
    char *env;
#endif /* not WIN32 */

    if (argc > 1 || argv[0] == NULL)
	argc = 1;		/* silencing stupid compiler warnings */

    old_SPATIALITE_SECURITY_ENV = getenv ("SPATIALITE_SECURITY");
#ifdef _WIN32
    putenv ("SPATIALITE_SECURITY=relaxed");
#else /* not WIN32 */
    setenv ("SPATIALITE_SECURITY", "relaxed", 1);
#endif

    ret =
	sqlite3_open_v2 (":memory:", &handle,
			 SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "cannot open \":memory:\" database: %s\n",
		   sqlite3_errmsg (handle));
	  sqlite3_close (handle);
	  return -1;
      }

    spatialite_init_ex (handle, cache, 0);

    ret =
	sqlite3_exec (handle, "SELECT InitSpatialMetadata(1)", NULL, NULL,
		      &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "InitSpatialMetadata() error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -2;
      }

/* importing Elba (polygons) from SHP */
    ret =
	sqlite3_exec (handle,
		      "SELECT ImportSHP('./elba-pg', 'elba_pg', 'CP1252', 32632)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "ImportSHP() elba-pg error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -3;
      }

/* importing Elba (linestrings) from SHP */
    ret =
	sqlite3_exec (handle,
		      "SELECT ImportSHP('./elba-ln', 'elba_ln', 'CP1252', 32632)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "ImportSHP() elba-ln error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -4;
      }

    if (old_SPATIALITE_SECURITY_ENV)
      {
#ifdef _WIN32
	  env =
	      sqlite3_mprintf ("SPATIALITE_SECURITY=%s",
			       old_SPATIALITE_SECURITY_ENV);
	  putenv (env);
	  sqlite3_free (env);
#else /* not WIN32 */
	  setenv ("SPATIALITE_SECURITY", old_SPATIALITE_SECURITY_ENV, 1);
#endif
      }
    else
      {
#ifdef _WIN32
	  putenv ("SPATIALITE_SECURITY=");
#else /* not WIN32 */
	  unsetenv ("SPATIALITE_SECURITY");
#endif
      }

/* adding a second Geometry to Elba-polygons */
    ret =
	sqlite3_exec (handle,
		      "SELECT AddGeometryColumn('elba_pg', 'centroid', 32632, 'POINT', 'XY')",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "AddGeometryColumn elba-pg error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -5;
      }
    ret =
	sqlite3_exec (handle,
		      "UPDATE elba_pg SET centroid = ST_Centroid(geometry)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "Update elba-pg centroids error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -6;
      }

/* creating a Topology 2D (wrong SRID) */
    ret =
	sqlite3_exec (handle, "SELECT CreateTopology('badelba1', 4326, 0, 0)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CreateTopology() #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -7;
      }

/* creating a Topology 3D (wrong dims) */
    ret =
	sqlite3_exec (handle, "SELECT CreateTopology('badelba2', 32632, 0, 1)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CreateTopology() #2 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -8;
      }

/* creating a Topology 2D (ok) */
    ret =
	sqlite3_exec (handle, "SELECT CreateTopology('elba', 32632, 0, 0)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CreateTopology() #3 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -9;
      }

/* basic tests: level 0 */
    if (!do_level0_tests (handle, &retcode))
	goto end;

/* basic tests: level 1 */
    if (!do_level1_tests (handle, &retcode))
	goto end;

/* dropping and recreating again a Topology 2D (ok) */
    ret =
	sqlite3_exec (handle, "SELECT DropTopology('elba')", NULL,
		      NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DropTopology() error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -10;
      }
    ret =
	sqlite3_exec (handle, "SELECT CreateTopology('elba', 32632, 0, 0)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CreateTopology() #4 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -11;
      }

/* basic tests: level 2 */
    if (!do_level2_tests (handle, &retcode))
	goto end;

  end:
    sqlite3_close (handle);
    spatialite_cleanup_ex (cache);

#endif /* end TOPOLOGY conditional */

    spatialite_shutdown ();
    return retcode;
}
