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
do_level6_tests (sqlite3 * handle, int *retcode)
{
/* performing basic tests: Level 6 */
    int ret;
    char *err_msg = NULL;

/* Validating a Topology - valid */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_ValidateTopoGeo('elba')",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "ValidateTopoGeo() #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -180;
	  return 0;
      }

/* dirtying the Topology */
    sqlite3_exec (handle, "BEGIN", NULL, NULL, NULL);
    ret =
	sqlite3_exec (handle,
		      "INSERT INTO elba_node (node_id, containing_face, geom) "
		      "SELECT NULL, NULL, geom FROM elba_node WHERE node_id = 7",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO Node #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -181;
	  return 0;
      }
    ret =
	sqlite3_exec (handle,
		      "INSERT INTO elba_node (node_id, containing_face, geom) "
		      "SELECT NULL, NULL, ST_PointN(geom, 33) FROM elba_edge WHERE edge_id = 13",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO Node #2 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -182;
	  return 0;
      }
    ret =
	sqlite3_exec (handle,
		      "UPDATE elba_edge SET geom = GeomFromText('LINESTRING(604477 4736752, 604483 4736751, 604480 4736755, 604483 4736751, 604840 4736648)', 32632) WHERE edge_id = 26",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "UPDATE Edge error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -183;
	  return 0;
      }
    ret =
	sqlite3_exec (handle,
		      "INSERT INTO elba_face (face_id, min_x, min_y, max_x, max_y) VALUES (NULL, 610000, 4700000, 610001, 4700001)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO Face error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -184;
	  return 0;
      }
    ret =
	sqlite3_exec (handle,
		      "DELETE FROM elba_face WHERE face_id = 0",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "DELETE FROM Face #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -185;
	  return 0;
      }
    ret =
	sqlite3_exec (handle,
		      "INSERT INTO elba_node (node_id, containing_face, geom) "
		      "VALUES (NULL, NULL, MakePoint(612771, 4737829, 32632))",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO Node #3 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -186;
	  return 0;
      }
    ret =
	sqlite3_exec (handle,
		      "INSERT INTO elba_face (face_id, min_x, min_y, max_x, max_y) VALUES (NULL, 612771, 4737329, 613771, 4737829)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO Face #2 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -187;
	  return 0;
      }
    ret =
	sqlite3_exec (handle,
		      "INSERT INTO elba_edge (edge_id, start_node, end_node, next_left_edge, next_right_edge, left_face, right_face, geom) "
		      "VALUES (NULL, 49, 49, 46, -46, 28, 33, GeomFromText('LINESTRING(612771 4737829, 613771 4737829, 613771 4737329, 612771 4737329, 612771 4737829)', 32632))",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO Edge #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -188;
	  return 0;
      }
    ret =
	sqlite3_exec (handle,
		      "INSERT INTO elba_node (node_id, containing_face, geom) "
		      "VALUES (NULL, NULL, MakePoint(613200, 4737700, 32632))",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO Node #4 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -189;
	  return 0;
      }
    ret =
	sqlite3_exec (handle,
		      "INSERT INTO elba_face (face_id, min_x, min_y, max_x, max_y) VALUES (NULL, 611771, 4738329, 612771, 4738829)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO Face #3 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -190;
	  return 0;
      }
    ret =
	sqlite3_exec (handle,
		      "INSERT INTO elba_edge (edge_id, start_node, end_node, next_left_edge, next_right_edge, left_face, right_face, geom) "
		      "VALUES (NULL, 50, 50, 47, -47, 29, 33, GeomFromText('LINESTRING(613200 4737700, 613400 4737700, 613400 4737400, 613200 4737400, 613200 4737700)', 32632))",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "INSERT INTO Edge #2 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -191;
	  return 0;
      }
    sqlite3_exec (handle, "COMMIT", NULL, NULL, NULL);

/* Validating yet again the dirtied Topology */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_ValidateTopoGeo('elba')",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "ValidateTopoGeo() #2 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -192;
	  return 0;
      }

    return 1;
}

static int
do_level5_tests (sqlite3 * handle, int *retcode)
{
/* performing basic tests: Level 5 */
    int ret;
    char *err_msg = NULL;

/* Validating a Spatial Network - valid */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_ValidSpatialNet('spatnet')",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "ValidSpatialNet() #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -160;
	  return 0;
      }

/* dirtying the Spatial Network */
    sqlite3_exec (handle, "BEGIN", NULL, NULL, NULL);
    ret =
	sqlite3_exec (handle,
		      "UPDATE spatnet_node SET geometry = NULL WHERE node_id = 1",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "UPDATE Spatial Node error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -161;
	  return 0;
      }
    ret =
	sqlite3_exec (handle,
		      "UPDATE spatnet_link SET geometry = NULL WHERE link_id = 1",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "UPDATE Spatial Link error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -162;
	  return 0;
      }
    ret =
	sqlite3_exec (handle,
		      "UPDATE spatnet_link SET geometry = GeomFromText('LINESTRING(604477 4736752, 604840 4736648)', 32632) WHERE link_id = 26",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "UPDATE Spatial Link error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -163;
	  return 0;
      }
    sqlite3_exec (handle, "COMMIT", NULL, NULL, NULL);

/* Validating yet again the dirtied Spatial Network */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_ValidSpatialNet('spatnet')",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "ValidSpatialNet() #2 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -164;
	  return 0;
      }

    return 1;
}

static int
do_level4_tests (sqlite3 * handle, int *retcode)
{
/* performing basic tests: Level 4 */
    int ret;
    char *err_msg = NULL;

/* Validating a Logical Network - valid */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_ValidLogicalNet('loginet')",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "ValidLogicalNet() #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -150;
	  return 0;
      }

/* dirtying the Logical Network */
    sqlite3_exec (handle, "BEGIN", NULL, NULL, NULL);
    ret =
	sqlite3_exec (handle,
		      "UPDATE loginet_node SET geometry = MakePoint(0, 0, -1) WHERE node_id = 1",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "UPDATE Logical Node error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -151;
	  return 0;
      }
    ret =
	sqlite3_exec (handle,
		      "UPDATE loginet_link SET geometry = GeomFromText('LINESTRING(0 0, 1 1)', -1) WHERE link_id = 1",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "UPDATE Logical Link error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -152;
	  return 0;
      }
    sqlite3_exec (handle, "COMMIT", NULL, NULL, NULL);

/* Validating yet again the dirtied Logical Network */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_ValidLogicalNet('loginet')",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "ValidLogicalNet() #2 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -153;
	  return 0;
      }

    return 1;
}

static int
do_level3_tests (sqlite3 * handle, int *retcode)
{
/* performing basic tests: Level 3 */
    int ret;
    char *err_msg = NULL;

/* cloning a Topology */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_Clone('elba', 'elba_clone')",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "TopoGeo_Clone() #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -120;
	  return 0;
      }

/* attempting to clone a Topology - already existing destination Topology */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_Clone('elba', 'elba_clone')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "TopoGeo_Clone() already existing destination Topology: expected failure\n");
	  *retcode = -121;
	  return 0;
      }
    if (strcmp
	(err_msg,
	 "SQL/MM Spatial exception - invalid topology name (destination).") !=
	0)
      {
	  fprintf (stderr,
		   "TopoGeo_Clone() already existing destination Topology: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -122;
	  return 0;
      }
    sqlite3_free (err_msg);

/* simplifying a Topology */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoGeo_Simplify('elba_clone', 5.0)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "TopoGeo_Simplify() #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -123;
	  return 0;
      }

/* attempting to transform a Topology into a Logical Network (non-existing) */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_LogiNetFromTGeo('loginet', 'lollypop')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "LogiNetFromTGeo() non-existing Topology: expected failure\n");
	  *retcode = -124;
	  return 0;
      }
    if (strcmp (err_msg, "SQL/MM Spatial exception - invalid topology name.") !=
	0)
      {
	  fprintf (stderr,
		   "LogiNetFromTGeo() non-existing Topology: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -125;
	  return 0;
      }
    sqlite3_free (err_msg);

/* transforming a Topology into a Logical Network */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_LogiNetFromTGeo('loginet', 'elba')",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "LogiNetFromTGeo() #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -126;
	  return 0;
      }

/* attempting to transform a Topology into a Spatial Network (non-existing) */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_SpatNetFromTGeo('spatnet', 'lollypop')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "SpatNetFromTGeo() non-existing Topology: expected failure\n");
	  *retcode = -127;
	  return 0;
      }
    if (strcmp (err_msg, "SQL/MM Spatial exception - invalid topology name.") !=
	0)
      {
	  fprintf (stderr,
		   "SpatNetFromTGeo() non-existing Topology: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -128;
	  return 0;
      }
    sqlite3_free (err_msg);

/* transforming a Topology into a Spatial Network */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_SpatNetFromTGeo('spatnet', 'elba')",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "SpatNetFromTGeo() #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -129;
	  return 0;
      }

/* attempting to transform a Topology into a Logical Network (non-empty) */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_LogiNetFromTGeo('loginet', 'elba')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "LogiNetFromTGeo() already populated Network: expected failure\n");
	  *retcode = -130;
	  return 0;
      }
    if (strcmp (err_msg, "SQL/MM Spatial exception - non-empty network.") != 0)
      {
	  fprintf (stderr,
		   "LogiNetFromTGeo() already populated Network: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -131;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to transform a Topology into a Spatial Network (non-empty) */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_SpatNetFromTGeo('spatnet', 'elba')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "SpatNetFromTGeo() already populated Network: expected failure\n");
	  *retcode = -132;
	  return 0;
      }
    if (strcmp (err_msg, "SQL/MM Spatial exception - non-empty network.") != 0)
      {
	  fprintf (stderr,
		   "SpatNetFromTGeo() already populated Network: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -133;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to simplify a Logical Network */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoNet_Simplify('loginet', 25.0)",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "TopoNet_Simplify() Logical Network: expected failure\n");
	  *retcode = -134;
	  return 0;
      }
    if (strcmp
	(err_msg,
	 "TopoNet_Simplify() cannot be applied to Logical Network.") != 0)
      {
	  fprintf (stderr,
		   "TopoNet_Simplify() Logical Network: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -135;
	  return 0;
      }
    sqlite3_free (err_msg);

/* simplifying a Spatial Network */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoNet_Simplify('spatnet', 25.0)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "TopoNet_Simplify() #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -136;
	  return 0;
      }

/* cloning a Logical Network */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoNet_Clone('loginet', 'loginet_clone')",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "TopoNet_Clone() #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -137;
	  return 0;
      }

/* attempting to clone a Logical Network - already existing destination Network */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoNet_Clone('loginet', 'loginet_clone')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "TopoNet_Clone() already existing destination Logical Network: expected failure\n");
	  *retcode = -138;
	  return 0;
      }
    if (strcmp
	(err_msg,
	 "SQL/MM Spatial exception - invalid network name (destination).") != 0)
      {
	  fprintf (stderr,
		   "TopoNet_Clone() already existing destination Logical Network: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -139;
	  return 0;
      }
    sqlite3_free (err_msg);

/* cloning a Spatial Network */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoNet_Clone('spatnet', 'spatnet_clone')",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "TopoNet_Clone() #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -140;
	  return 0;
      }

/* attempting to clone a Spatial Network - already existing destination Network */
    ret =
	sqlite3_exec (handle,
		      "SELECT TopoNet_Clone('spatnet', 'spatnet_clone')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "TopoNet_Clone() already existing destination Spatial Network: expected failure\n");
	  *retcode = -141;
	  return 0;
      }
    if (strcmp
	(err_msg,
	 "SQL/MM Spatial exception - invalid network name (destination).") != 0)
      {
	  fprintf (stderr,
		   "TopoNet_Clone() already existing destination Spatial Network: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -142;
	  return 0;
      }
    sqlite3_free (err_msg);

    return 1;
}


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
	  *retcode = -90;
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
	  *retcode = -91;
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

/* attempting to transform a Topology into a Logical Network (Spatial Network indeed) */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_LogiNetFromTGeo('spatnet', 'elba')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "LogiNetFromTGeo() Spatial Network: expected failure\n");
	  *retcode = -64;
	  return 0;
      }
    if (strcmp
	(err_msg,
	 "ST_LogiNetFromTGeo() cannot be applied to Spatial Network.") != 0)
      {
	  fprintf (stderr,
		   "LogiNetFromTGeo() Spatial Network: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -65;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to transform a Topology into a Spatial Network (Logical Network indeed) */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_SpatNetFromTGeo('loginet', 'elba')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "SpatNetFromTGeo() Logical Network: expected failure\n");
	  *retcode = -66;
	  return 0;
      }
    if (strcmp
	(err_msg,
	 "ST_SpatNetFromTGeo() cannot be applied to Logical Network.") != 0)
      {
	  fprintf (stderr,
		   "SpatNetFromTGeo() Logical Network: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -67;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to transform a Topology into a Spatial Network (mismatching SRID) */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_SpatNetFromTGeo('badnet1', 'elba')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "SpatNetFromTGeo() mismatching SRID: expected failure\n");
	  *retcode = -68;
	  return 0;
      }
    if (strcmp
	(err_msg,
	 "SQL/MM Spatial exception - mismatching SRID or dimensions.") != 0)
      {
	  fprintf (stderr,
		   "SpatNetFromTGeo() mismatching SRID: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -69;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to transform a Topology into a Spatial Network (mismatching dims) */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_SpatNetFromTGeo('badnet2', 'elba')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "SpatNetFromTGeo() mismatching dims: expected failure\n");
	  *retcode = -70;
	  return 0;
      }
    if (strcmp
	(err_msg,
	 "SQL/MM Spatial exception - mismatching SRID or dimensions.") != 0)
      {
	  fprintf (stderr,
		   "SpatNetFromTGeo() mismatching dims: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -71;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to validate a Logical Network (Spatial indeed) */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_ValidLogicalNet('spatnet')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "ValidLogicalNet() mismatching type: expected failure\n");
	  *retcode = -72;
	  return 0;
      }
    if (strcmp
	(err_msg,
	 "ST_ValidLogicalNet() cannot be applied to Spatial Network.") != 0)
      {
	  fprintf (stderr,
		   "ValidLogicalNet() mismatching type: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -73;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to validate a Logical Network (empty) */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_ValidLogicalNet('loginet')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr, "ValidLogicalNet() empty: expected failure\n");
	  *retcode = -74;
	  return 0;
      }
    if (strcmp (err_msg, "SQL/MM Spatial exception - empty network.") != 0)
      {
	  fprintf (stderr,
		   "ValidLogicalNet() empty: unexpected \"%s\"\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -75;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to validate a Spatial Network (Logical indeed) */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_ValidSpatialNet('loginet')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr,
		   "ValidSpatialNet() mismatching type: expected failure\n");
	  *retcode = -76;
	  return 0;
      }
    if (strcmp
	(err_msg,
	 "ST_ValidSpatialNet() cannot be applied to Logical Network.") != 0)
      {
	  fprintf (stderr,
		   "ValidSpatialNet() mismatching type: unexpected \"%s\"\n",
		   err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -77;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to validate a Spatial Network (empty) */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_ValidSpatialNet('spatnet')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr, "ValidSpatialNet() empty: expected failure\n");
	  *retcode = -78;
	  return 0;
      }
    if (strcmp (err_msg, "SQL/MM Spatial exception - empty network.") != 0)
      {
	  fprintf (stderr,
		   "ValidSpatialNet() empty: unexpected \"%s\"\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -79;
	  return 0;
      }
    sqlite3_free (err_msg);

/* attempting to validate a TopoGeo (empty) */
    ret =
	sqlite3_exec (handle,
		      "SELECT ST_ValidateTopoGeo('elba')",
		      NULL, NULL, &err_msg);
    if (ret == SQLITE_OK)
      {
	  fprintf (stderr, "ValidateTopoGeo() empty: expected failure\n");
	  *retcode = -80;
	  return 0;
      }
    if (strcmp (err_msg, "SQL/MM Spatial exception - empty topology.") != 0)
      {
	  fprintf (stderr,
		   "ValidateTopoGeo() empty: unexpected \"%s\"\n", err_msg);
	  sqlite3_free (err_msg);
	  *retcode = -81;
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

    if (sqlite3_libversion_number () < 3008003)
      {
	  fprintf (stderr,
		   "*** check_topoplus skipped: libsqlite < 3.8.3 !!!\n");
	  goto end;
      }

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

/* creating a Logical Network */
    ret =
	sqlite3_exec (handle, "SELECT CreateNetwork('loginet', 0)", NULL,
		      NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CreateNetwork() #1 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -10;
      }

/* creating a Network 2D */
    ret =
	sqlite3_exec (handle, "SELECT CreateNetwork('spatnet', 1, 32632, 0, 0)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CreateNetwork() #2 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -11;
      }

/* creating a Network 2D - wrong SRID */
    ret =
	sqlite3_exec (handle, "SELECT CreateNetwork('badnet1', 1, 3003, 0, 0)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CreateNetwork() #3 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -12;
      }

/* creating a Network 3D */
    ret =
	sqlite3_exec (handle, "SELECT CreateNetwork('badnet2', 1, 32632, 1, 0)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CreateNetwork() #4 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -13;
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
	  return -14;
      }
    ret =
	sqlite3_exec (handle, "SELECT CreateTopology('elba', 32632, 0, 0)",
		      NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "CreateTopology() #4 error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  sqlite3_close (handle);
	  return -15;
      }

/* basic tests: level 2 */
    if (!do_level2_tests (handle, &retcode))
	goto end;

/* basic tests: level 3 */
    if (!do_level3_tests (handle, &retcode))
	goto end;

/* basic tests: level 4 */
    if (!do_level4_tests (handle, &retcode))
	goto end;

/* basic tests: level 5 */
    if (!do_level5_tests (handle, &retcode))
	goto end;

/* basic tests: level 6 */
    if (!do_level6_tests (handle, &retcode))
	goto end;

  end:
    sqlite3_close (handle);
    spatialite_cleanup_ex (cache);

#endif /* end TOPOLOGY conditional */

    spatialite_shutdown ();
    return retcode;
}
