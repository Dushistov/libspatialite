/*

 gaia_auxnet.c -- implementation of the Topology-Network module methods
    
 version 4.3, 2015 August 12

 Author: Sandro Furieri a.furieri@lqt.it

 -----------------------------------------------------------------------------
 
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
 
Portions created by the Initial Developer are Copyright (C) 2015
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

#if defined(_WIN32) && !defined(__MINGW32__)
#include "config-msvc.h"
#else
#include "config.h"
#endif

#ifdef POSTGIS_2_2		/* only if TOPOLOGY is enabled */

#include <spatialite/sqlite.h>
#include <spatialite/debug.h>
#include <spatialite/gaiageo.h>
#include <spatialite/gaia_network.h>
#include <spatialite/gaiaaux.h>

#include <spatialite.h>
#include <spatialite_private.h>

#include <lwn_network.h>

#include "network_private.h"

#define GAIA_UNUSED() if (argc || argv) argc = argc;

SPATIALITE_PRIVATE void
free_internal_cache_networks (void *firstNetwork)
{
/* destroying all Networks registered into the Internal Connection Cache */
    struct gaia_network *p_net = (struct gaia_network *) firstNetwork;
    struct gaia_network *p_net_n;

    while (p_net != NULL)
      {
	  p_net_n = p_net->next;
	  gaiaNetworkDestroy ((GaiaNetworkAccessorPtr) p_net);
	  p_net = p_net_n;
      }
}

static int
do_create_networks (sqlite3 * handle)
{
/* attempting to create the Networks table (if not already existing) */
    const char *sql;
    char *err_msg = NULL;
    int ret;

    sql = "CREATE TABLE IF NOT EXISTS networks (\n"
	"\tnetwork_name TEXT NOT NULL PRIMARY KEY,\n"
	"\tspatial INTEGER NOT NULL,\n"
	"\tsrid INTEGER NOT NULL,\n"
	"\thas_z INTEGER NOT NULL,\n"
	"\tallow_coincident INTEGER NOT NULL,\n"
	"\tnext_node_id INTEGER NOT NULL DEFAULT 1,\n"
	"\tnext_link_id INTEGER NOT NULL DEFAULT 1,\n"
	"\tCONSTRAINT net_srid_fk FOREIGN KEY (srid) "
	"REFERENCES spatial_ref_sys (srid))";
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE TABLE networks - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating Networks triggers */
    sql = "CREATE TRIGGER IF NOT EXISTS network_name_insert\n"
	"BEFORE INSERT ON 'networks'\nFOR EACH ROW BEGIN\n"
	"SELECT RAISE(ABORT,'insert on networks violates constraint: "
	"network_name value must not contain a single quote')\n"
	"WHERE NEW.network_name LIKE ('%''%');\n"
	"SELECT RAISE(ABORT,'insert on networks violates constraint: "
	"network_name value must not contain a double quote')\n"
	"WHERE NEW.network_name LIKE ('%\"%');\n"
	"SELECT RAISE(ABORT,'insert on networks violates constraint: "
	"network_name value must be lower case')\n"
	"WHERE NEW.network_name <> lower(NEW.network_name);\nEND";
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }
    sql = "CREATE TRIGGER IF NOT EXISTS network_name_update\n"
	"BEFORE UPDATE OF 'network_name' ON 'networks'\nFOR EACH ROW BEGIN\n"
	"SELECT RAISE(ABORT,'update on networks violates constraint: "
	"network_name value must not contain a single quote')\n"
	"WHERE NEW.network_name LIKE ('%''%');\n"
	"SELECT RAISE(ABORT,'update on networks violates constraint: "
	"network_name value must not contain a double quote')\n"
	"WHERE NEW.network_name LIKE ('%\"%');\n"
	"SELECT RAISE(ABORT,'update on networks violates constraint: "
	"network_name value must be lower case')\n"
	"WHERE NEW.network_name <> lower(NEW.network_name);\nEND";
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

    return 1;
}

static int
check_new_network (sqlite3 * handle, const char *network_name)
{
/* testing if some already defined DB object forbids creating the new Network */
    char *sql;
    char *prev;
    char *table;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    const char *value;
    int error = 0;

/* testing if the same Network is already defined */
    sql = sqlite3_mprintf ("SELECT Count(*) FROM networks WHERE "
			   "Lower(network_name) = Lower(%Q)", network_name);
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = results[(i * columns) + 0];
		if (atoi (value) != 0)
		    error = 1;
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;

/* testing if some table/geom is already defined in geometry_columns */
    sql = sqlite3_mprintf ("SELECT Count(*) FROM geometry_columns WHERE");
    prev = sql;
    table = sqlite3_mprintf ("%s_node", network_name);
    sql =
	sqlite3_mprintf
	("%s (Lower(f_table_name) = Lower(%Q) AND f_geometry_column = 'geometry')",
	 prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("%s_link", network_name);
    sql =
	sqlite3_mprintf
	("%s OR (Lower(f_table_name) = Lower(%Q) AND f_geometry_column = 'geometry')",
	 prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = results[(i * columns) + 0];
		if (atoi (value) != 0)
		    error = 1;
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;

/* testing if some table is already defined */
    sql = sqlite3_mprintf ("SELECT Count(*) FROM sqlite_master WHERE");
    prev = sql;
    table = sqlite3_mprintf ("%s_node", network_name);
    sql = sqlite3_mprintf ("%s Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("%s_link", network_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("idx_%s_node_geometry", network_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("idx_%s_link_geometry", network_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = results[(i * columns) + 0];
		if (atoi (value) != 0)
		    error = 1;
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;

    return 1;
}

NETWORK_PRIVATE LWN_LINE *
gaianet_convert_linestring_to_lwnline (gaiaLinestringPtr ln, int srid,
				       int has_z)
{
/* converting a Linestring into an LWN_LINE */
    int iv;
    LWN_LINE *line = lwn_alloc_line (ln->Points, srid, has_z);
    for (iv = 0; iv < ln->Points; iv++)
      {
	  double x;
	  double y;
	  double z;
	  double m;
	  if (ln->DimensionModel == GAIA_XY_Z)
	    {
		gaiaGetPointXYZ (ln->Coords, iv, &x, &y, &z);
	    }
	  else if (ln->DimensionModel == GAIA_XY_M)
	    {
		gaiaGetPointXYM (ln->Coords, iv, &x, &y, &m);
	    }
	  else if (ln->DimensionModel == GAIA_XY_Z_M)
	    {
		gaiaGetPointXYZM (ln->Coords, iv, &x, &y, &z, &m);
	    }
	  else
	    {
		gaiaGetPoint (ln->Coords, iv, &x, &y);
	    }
	  line->x[iv] = x;
	  line->y[iv] = y;
	  if (has_z)
	      line->z[iv] = z;
      }
    return line;
}

static int
do_create_node (sqlite3 * handle, const char *network_name, int srid, int has_z)
{
/* attempting to create the Network Node table */
    char *sql;
    char *table;
    char *xtable;
    char *trigger;
    char *xtrigger;
    char *err_msg = NULL;
    int ret;

/* creating the main table */
    table = sqlite3_mprintf ("%s_node", network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n"
			   "\tnode_id INTEGER PRIMARY KEY AUTOINCREMENT)",
			   xtable);
    free (xtable);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE TABLE network-NODE - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* adding the "next_node_ins" trigger */
    trigger = sqlite3_mprintf ("%s_node_next_ins", network_name);
    xtrigger = gaiaDoubleQuotedSql (trigger);
    sqlite3_free (trigger);
    table = sqlite3_mprintf ("%s_node", network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("CREATE TRIGGER \"%s\" AFTER INSERT ON \"%s\"\n"
			   "FOR EACH ROW BEGIN\n"
			   "\tUPDATE networks SET next_node_id = NEW.node_id + 1 "
			   "WHERE Lower(network_name) = Lower(%Q) AND next_node_id < NEW.node_id + 1;\n"
			   "END", xtrigger, xtable, network_name);
    free (xtrigger);
    free (xtable);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("CREATE TRIGGER network-NODE next INSERT - error: %s\n",
	       err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* adding the "next_node_upd" trigger */
    trigger = sqlite3_mprintf ("%s_node_next_upd", network_name);
    xtrigger = gaiaDoubleQuotedSql (trigger);
    sqlite3_free (trigger);
    table = sqlite3_mprintf ("%s_node", network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("CREATE TRIGGER \"%s\" AFTER UPDATE OF node_id ON \"%s\"\n"
	 "FOR EACH ROW BEGIN\n"
	 "\tUPDATE networks SET next_node_id = NEW.node_id + 1 "
	 "WHERE Lower(network_name) = Lower(%Q) AND next_node_id < NEW.node_id + 1;\n"
	 "END", xtrigger, xtable, network_name);
    free (xtrigger);
    free (xtable);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("CREATE TRIGGER network-NODE next UPDATE - error: %s\n",
	       err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating the Node Geometry */
    table = sqlite3_mprintf ("%s_node", network_name);
    sql =
	sqlite3_mprintf
	("SELECT AddGeometryColumn(%Q, 'geometry', %d, 'POINT', %Q)", table,
	 srid, has_z ? "XYZ" : "XY");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("AddGeometryColumn network-NODE - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating a Spatial Index supporting Node Geometry */
    table = sqlite3_mprintf ("%s_node", network_name);
    sql = sqlite3_mprintf ("SELECT CreateSpatialIndex(%Q, 'geometry')", table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("CreateSpatialIndex network-NODE - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

    return 1;
}

static int
do_create_link (sqlite3 * handle, const char *network_name, int srid, int has_z)
{
/* attempting to create the Network Link table */
    char *sql;
    char *table;
    char *xtable;
    char *xconstraint1;
    char *xconstraint2;
    char *xnodes;
    char *trigger;
    char *xtrigger;
    char *err_msg = NULL;
    int ret;

/* creating the main table */
    table = sqlite3_mprintf ("%s_link", network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_link_node_start_fk", network_name);
    xconstraint1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_link_node_end_fk", network_name);
    xconstraint2 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_node", network_name);
    xnodes = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n"
			   "\tlink_id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
			   "\tstart_node INTEGER NOT NULL,\n"
			   "\tend_node INTEGER NOT NULL,\n"
			   "\tCONSTRAINT \"%s\" FOREIGN KEY (start_node) "
			   "REFERENCES \"%s\" (node_id),\n"
			   "\tCONSTRAINT \"%s\" FOREIGN KEY (end_node) "
			   "REFERENCES \"%s\" (node_id))",
			   xtable, xconstraint1, xnodes, xconstraint2, xnodes);
    free (xtable);
    free (xconstraint1);
    free (xconstraint2);
    free (xnodes);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE TABLE network-LINK - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* adding the "next_link_ins" trigger */
    trigger = sqlite3_mprintf ("%s_link_next_ins", network_name);
    xtrigger = gaiaDoubleQuotedSql (trigger);
    sqlite3_free (trigger);
    table = sqlite3_mprintf ("%s_link", network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("CREATE TRIGGER \"%s\" AFTER INSERT ON \"%s\"\n"
			   "FOR EACH ROW BEGIN\n"
			   "\tUPDATE networks SET next_link_id = NEW.link_id + 1 "
			   "WHERE Lower(network_name) = Lower(%Q) AND next_link_id < NEW.link_id + 1;\n"
			   "END", xtrigger, xtable, network_name);
    free (xtrigger);
    free (xtable);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("CREATE TRIGGER network-EDGE next INSERT - error: %s\n",
	       err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* adding the "next_link_upd" trigger */
    trigger = sqlite3_mprintf ("%s_link_next_upd", network_name);
    xtrigger = gaiaDoubleQuotedSql (trigger);
    sqlite3_free (trigger);
    table = sqlite3_mprintf ("%s_link", network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("CREATE TRIGGER \"%s\" AFTER UPDATE OF link_id ON \"%s\"\n"
	 "FOR EACH ROW BEGIN\n"
	 "\tUPDATE networks SET next_link_id = NEW.link_id + 1 "
	 "WHERE Lower(network_name) = Lower(%Q) AND next_link_id < NEW.link_id + 1;\n"
	 "END", xtrigger, xtable, network_name);
    free (xtrigger);
    free (xtable);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("CREATE TRIGGER network-LINK next UPDATE - error: %s\n",
	       err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating the Link Geometry */
    table = sqlite3_mprintf ("%s_link", network_name);
    sql =
	sqlite3_mprintf
	("SELECT AddGeometryColumn(%Q, 'geometry', %d, 'LINESTRING', %Q)",
	 table, srid, has_z ? "XYZ" : "XY");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("AddGeometryColumn network-EDGE - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating a Spatial Index supporting Link Geometry */
    table = sqlite3_mprintf ("%s_link", network_name);
    sql = sqlite3_mprintf ("SELECT CreateSpatialIndex(%Q, 'geometry')", table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("CreateSpatialIndex network-LINK - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating an Index supporting "start_node" */
    table = sqlite3_mprintf ("%s_link", network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("idx_%s_start_node", network_name);
    xconstraint1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf ("CREATE INDEX \"%s\" ON \"%s\" (start_node)",
			 xconstraint1, xtable);
    free (xtable);
    free (xconstraint1);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE INDEX link-startnode - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating an Index supporting "end_node" */
    table = sqlite3_mprintf ("%s_link", network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("idx_%s_end_node", network_name);
    xconstraint1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf ("CREATE INDEX \"%s\" ON \"%s\" (end_node)",
			 xconstraint1, xtable);
    free (xtable);
    free (xconstraint1);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE INDEX link-endnode - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

    return 1;
}

GAIANET_DECLARE int
gaiaNetworkCreate (sqlite3 * handle, const char *network_name, int spatial,
		   int srid, int has_z, int allow_coincident)
{
/* attempting to create a new Network */
    int ret;
    char *sql;

/* creating the Networks table (just in case) */
    if (!do_create_networks (handle))
	return 0;

/* testing for forbidding objects */
    if (!check_new_network (handle, network_name))
	return 0;

/* creating the Network own Tables */
    if (!do_create_node (handle, network_name, srid, has_z))
	goto error;
    if (!do_create_link (handle, network_name, srid, has_z))
	goto error;

/* registering the Network */
    sql = sqlite3_mprintf ("INSERT INTO networks (network_name, "
			   "spatial, srid, has_z, allow_coincident) VALUES (Lower(%Q), %d, %d, %d, %d)",
			   network_name, spatial, srid, has_z,
			   allow_coincident);
    ret = sqlite3_exec (handle, sql, NULL, NULL, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto error;

    return 1;

  error:
    return 0;
}

static int
check_existing_network (sqlite3 * handle, const char *network_name,
			int full_check)
{
/* testing if a Network is already defined */
    char *sql;
    char *prev;
    char *table;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    const char *value;
    int error = 0;

/* testing if the Network is already defined */
    sql = sqlite3_mprintf ("SELECT Count(*) FROM networks WHERE "
			   "Lower(network_name) = Lower(%Q)", network_name);
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = results[(i * columns) + 0];
		if (atoi (value) != 1)
		    error = 1;
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;
    if (!full_check)
	return 1;

/* testing if all table/geom are correctly defined in geometry_columns */
    sql = sqlite3_mprintf ("SELECT Count(*) FROM geometry_columns WHERE");
    prev = sql;
    table = sqlite3_mprintf ("%s_node", network_name);
    sql =
	sqlite3_mprintf
	("%s (Lower(f_table_name) = Lower(%Q) AND f_geometry_column = 'geometry')",
	 prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("%s_link", network_name);
    sql =
	sqlite3_mprintf
	("%s OR (Lower(f_table_name) = Lower(%Q) AND f_geometry_column = 'geometry')",
	 prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = results[(i * columns) + 0];
		if (atoi (value) != 2)
		    error = 1;
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;

/* testing if all tables are already defined */
    sql =
	sqlite3_mprintf
	("SELECT Count(*) FROM sqlite_master WHERE type = 'table' AND (");
    prev = sql;
    table = sqlite3_mprintf ("%s_node", network_name);
    sql = sqlite3_mprintf ("%s Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("%s_link", network_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("idx_%s_node_geometry", network_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("idx_%s_link_geometry", network_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q))", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = results[(i * columns) + 0];
		if (atoi (value) != 4)
		    error = 1;
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;

    return 1;
}

static int
do_drop_network_table (sqlite3 * handle, const char *network_name,
		       const char *which)
{
/* attempting to drop some Network table */
    char *sql;
    char *table;
    char *xtable;
    char *err_msg = NULL;
    int ret;

/* disabling the corresponding Spatial Index */
    table = sqlite3_mprintf ("%s_%s", network_name, which);
    sql = sqlite3_mprintf ("SELECT DisableSpatialIndex(%Q, 'geometry')", table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("DisableSpatialIndex network-%s - error: %s\n", which, err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* discarding the Geometry column */
    table = sqlite3_mprintf ("%s_%s", network_name, which);
    sql =
	sqlite3_mprintf ("SELECT DiscardGeometryColumn(%Q, 'geometry')", table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("DisableGeometryColumn network-%s - error: %s\n", which,
	       err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* dropping the main table */
    table = sqlite3_mprintf ("%s_%s", network_name, which);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("DROP TABLE IF EXISTS \"%s\"", xtable);
    free (xtable);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("DROP network-%s - error: %s\n", which, err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* dropping the corresponding Spatial Index */
    table = sqlite3_mprintf ("idx_%s_%s_geometry", network_name, which);
    sql = sqlite3_mprintf ("DROP TABLE IF EXISTS \"%s\"", table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("DROP SpatialIndex network-%s - error: %s\n", which, err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

    return 1;
}

static int
do_get_network (sqlite3 * handle, const char *net_name, char **network_name,
		int *spatial, int *srid, int *has_z, int *allow_coincident)
{
/* retrieving a Network configuration */
    char *sql;
    int ret;
    sqlite3_stmt *stmt = NULL;
    int ok = 0;
    char *xnetwork_name = NULL;
    int xsrid;
    int xhas_z;
    int xspatial;
    int xallow_coincident;

/* preparing the SQL query */
    sql =
	sqlite3_mprintf
	("SELECT network_name, spatial, srid, has_z, allow_coincident "
	 "FROM networks WHERE Lower(network_name) = Lower(%Q)", net_name);
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SELECT FROM networks error: \"%s\"\n",
			sqlite3_errmsg (handle));
	  return 0;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		int ok_name = 0;
		int ok_srid = 0;
		int ok_z = 0;
		int ok_spatial = 0;
		int ok_allow_coincident = 0;
		if (sqlite3_column_type (stmt, 0) == SQLITE_TEXT)
		  {
		      const char *str =
			  (const char *) sqlite3_column_text (stmt, 0);
		      if (xnetwork_name != NULL)
			  free (xnetwork_name);
		      xnetwork_name = malloc (strlen (str) + 1);
		      strcpy (xnetwork_name, str);
		      ok_name = 1;
		  }
		if (sqlite3_column_type (stmt, 1) == SQLITE_INTEGER)
		  {
		      xspatial = sqlite3_column_int (stmt, 1);
		      ok_spatial = 1;
		  }
		if (sqlite3_column_type (stmt, 2) == SQLITE_INTEGER)
		  {
		      xsrid = sqlite3_column_int (stmt, 2);
		      ok_srid = 1;
		  }
		if (sqlite3_column_type (stmt, 3) == SQLITE_INTEGER)
		  {
		      xhas_z = sqlite3_column_int (stmt, 3);
		      ok_z = 1;
		  }
		if (sqlite3_column_type (stmt, 4) == SQLITE_INTEGER)
		  {
		      xallow_coincident = sqlite3_column_int (stmt, 4);
		      ok_allow_coincident = 1;
		  }
		if (ok_name && ok_spatial && ok_srid && ok_z
		    && ok_allow_coincident)
		  {
		      ok = 1;
		      break;
		  }
	    }
	  else
	    {
		spatialite_e
		    ("step: SELECT FROM networks error: \"%s\"\n",
		     sqlite3_errmsg (handle));
		sqlite3_finalize (stmt);
		return 0;
	    }
      }
    sqlite3_finalize (stmt);

    if (ok)
      {
	  *network_name = xnetwork_name;
	  *srid = xsrid;
	  *has_z = xhas_z;
	  *spatial = xspatial;
	  *allow_coincident = xallow_coincident;
	  return 1;
      }

    if (xnetwork_name != NULL)
	free (xnetwork_name);
    return 0;
}

GAIANET_DECLARE GaiaNetworkAccessorPtr
gaiaGetNetwork (sqlite3 * handle, const void *cache, const char *network_name)
{
/* attempting to get a reference to some Network Accessor Object */
    GaiaNetworkAccessorPtr accessor;

/* attempting to retrieve an alredy cached definition */
    accessor = gaiaNetworkFromCache (cache, network_name);
    if (accessor != NULL)
	return accessor;

/* attempting to create a new Network Accessor */
    accessor = gaiaNetworkFromDBMS (handle, cache, network_name);
    return accessor;
}

GAIANET_DECLARE GaiaNetworkAccessorPtr
gaiaNetworkFromCache (const void *p_cache, const char *network_name)
{
/* attempting to retrieve an already defined Network Accessor Object from the Connection Cache */
    struct gaia_network *ptr;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache == 0)
	return NULL;

    ptr = (struct gaia_network *) (cache->firstNetwork);
    while (ptr != NULL)
      {
	  /* checking for an already registered Network */
	  if (strcasecmp (network_name, ptr->network_name) == 0)
	      return (GaiaNetworkAccessorPtr) ptr;
	  ptr = ptr->next;
      }
    return NULL;
}

GAIANET_DECLARE int
gaiaReadNetworkFromDBMS (sqlite3 *
			 handle,
			 const char
			 *net_name, char **network_name, int *spatial,
			 int *srid, int *has_z, int *allow_coincident)
{
/* testing for existing DBMS objects */
    if (!check_existing_network (handle, net_name, 1))
	return 0;

/* retrieving the Network configuration */
    if (!do_get_network
	(handle, net_name, network_name, spatial, srid, has_z,
	 allow_coincident))
	return 0;
    return 1;
}

GAIANET_DECLARE GaiaNetworkAccessorPtr
gaiaNetworkFromDBMS (sqlite3 * handle, const void *p_cache,
		     const char *network_name)
{
/* attempting to create a Network Accessor Object into the Connection Cache */
    LWN_BE_CALLBACKS *callbacks;
    struct gaia_network *ptr;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache == 0)
	return NULL;

/* allocating and initializing the opaque object */
    ptr = malloc (sizeof (struct gaia_network));
    ptr->db_handle = handle;
    ptr->cache = cache;
    ptr->network_name = NULL;
    ptr->srid = -1;
    ptr->has_z = 0;
    ptr->spatial = 0;
    ptr->allow_coincident = 0;
    ptr->last_error_message = NULL;
    ptr->lwn_iface = lwn_CreateBackendIface ((const LWN_BE_DATA *) ptr);
    ptr->prev = cache->lastNetwork;
    ptr->next = NULL;

    callbacks = malloc (sizeof (LWN_BE_CALLBACKS));
    callbacks->netGetSRID = netcallback_netGetSRID;
    callbacks->netHasZ = netcallback_netHasZ;
    callbacks->netIsSpatial = netcallback_netIsSpatial;
    callbacks->netAllowCoincident = netcallback_netAllowCoincident;
    callbacks->netGetGEOS = netcallback_netGetGEOS;
    callbacks->createNetwork = NULL;
    callbacks->loadNetworkByName = netcallback_loadNetworkByName;
    callbacks->freeNetwork = netcallback_freeNetwork;
    callbacks->getNetNodeWithinDistance2D =
	netcallback_getNetNodeWithinDistance2D;
    callbacks->getLinkWithinDistance2D = netcallback_getLinkWithinDistance2D;
    callbacks->insertNetNodes = netcallback_insertNetNodes;
    callbacks->getNetNodeById = netcallback_getNetNodeById;
    callbacks->updateNetNodesById = netcallback_updateNetNodesById;
    callbacks->deleteNetNodesById = netcallback_deleteNetNodesById;
    callbacks->getLinkByNetNode = netcallback_getLinkByNetNode;
    callbacks->getNextLinkId = netcallback_getNextLinkId;
    callbacks->getNetNodeWithinBox2D = netcallback_getNetNodeWithinBox2D;
    callbacks->getNextLinkId = netcallback_getNextLinkId;
    callbacks->insertLinks = netcallback_insertLinks;
    callbacks->updateLinksById = netcallback_updateLinksById;
    callbacks->getLinkById = netcallback_getLinkById;
    callbacks->deleteLinksById = netcallback_deleteLinksById;
    ptr->callbacks = callbacks;

    lwn_BackendIfaceRegisterCallbacks (ptr->lwn_iface, callbacks);
    ptr->lwn_network = lwn_LoadNetwork (ptr->lwn_iface, network_name);
    if (ptr->lwn_network == NULL)
	goto invalid;

/* creating the SQL prepared statements */
    ptr->stmt_getNetNodeWithinDistance2D =
	do_create_stmt_getNetNodeWithinDistance2D ((GaiaNetworkAccessorPtr)
						   ptr);
    ptr->stmt_getLinkWithinDistance2D =
	do_create_stmt_getLinkWithinDistance2D ((GaiaNetworkAccessorPtr) ptr);
    ptr->stmt_deleteNetNodesById =
	do_create_stmt_deleteNetNodesById ((GaiaNetworkAccessorPtr) ptr);
    ptr->stmt_insertNetNodes =
	do_create_stmt_insertNetNodes ((GaiaNetworkAccessorPtr) ptr);
    ptr->stmt_getNetNodeWithinBox2D =
	do_create_stmt_getNetNodeWithinBox2D ((GaiaNetworkAccessorPtr) ptr);
    ptr->stmt_getNextLinkId =
	do_create_stmt_getNextLinkId ((GaiaNetworkAccessorPtr) ptr);
    ptr->stmt_setNextLinkId =
	do_create_stmt_setNextLinkId ((GaiaNetworkAccessorPtr) ptr);
    ptr->stmt_insertLinks =
	do_create_stmt_insertLinks ((GaiaNetworkAccessorPtr) ptr);
    ptr->stmt_deleteLinksById =
	do_create_stmt_deleteLinksById ((GaiaNetworkAccessorPtr) ptr);

    return (GaiaNetworkAccessorPtr) ptr;

  invalid:
    ptr->stmt_getNetNodeWithinDistance2D = NULL;
    ptr->stmt_getLinkWithinDistance2D = NULL;
    ptr->stmt_insertNetNodes = NULL;
    ptr->stmt_deleteNetNodesById = NULL;
    ptr->stmt_getNetNodeWithinBox2D = NULL;
    ptr->stmt_getNextLinkId = NULL;
    ptr->stmt_setNextLinkId = NULL;
    ptr->stmt_insertLinks = NULL;
    ptr->stmt_deleteLinksById = NULL;
    gaiaNetworkDestroy ((GaiaNetworkAccessorPtr) ptr);
    return NULL;
}

GAIANET_DECLARE void
gaiaNetworkDestroy (GaiaNetworkAccessorPtr net_ptr)
{
/* destroying a Network Accessor Object */
    struct gaia_network *prev;
    struct gaia_network *next;
    struct splite_internal_cache *cache;
    struct gaia_network *ptr = (struct gaia_network *) net_ptr;
    if (ptr == NULL)
	return;

    prev = ptr->prev;
    next = ptr->next;
    cache = (struct splite_internal_cache *) (ptr->cache);
    if (ptr->lwn_network != NULL)
	lwn_FreeNetwork ((LWN_NETWORK *) (ptr->lwn_network));
    if (ptr->lwn_iface != NULL)
	lwn_FreeBackendIface ((LWN_BE_IFACE *) (ptr->lwn_iface));
    if (ptr->callbacks != NULL)
	free (ptr->callbacks);

    if (ptr->network_name != NULL)
	free (ptr->network_name);
    if (ptr->last_error_message != NULL)
	free (ptr->last_error_message);
    if (ptr->stmt_getNetNodeWithinDistance2D != NULL)
	sqlite3_finalize (ptr->stmt_getNetNodeWithinDistance2D);
    if (ptr->stmt_getLinkWithinDistance2D != NULL)
	sqlite3_finalize (ptr->stmt_getLinkWithinDistance2D);
    if (ptr->stmt_insertNetNodes != NULL)
	sqlite3_finalize (ptr->stmt_insertNetNodes);
    if (ptr->stmt_deleteNetNodesById != NULL)
	sqlite3_finalize (ptr->stmt_deleteNetNodesById);
    if (ptr->stmt_getNetNodeWithinBox2D != NULL)
	sqlite3_finalize (ptr->stmt_getNetNodeWithinBox2D);
    if (ptr->stmt_getNextLinkId != NULL)
	sqlite3_finalize (ptr->stmt_getNextLinkId);
    if (ptr->stmt_setNextLinkId != NULL)
	sqlite3_finalize (ptr->stmt_setNextLinkId);
    if (ptr->stmt_insertLinks != NULL)
	sqlite3_finalize (ptr->stmt_insertLinks);
    if (ptr->stmt_deleteLinksById != NULL)
	sqlite3_finalize (ptr->stmt_deleteLinksById);
    free (ptr);

/* unregistering from the Internal Cache double linked list */
    if (prev != NULL)
	prev->next = next;
    if (next != NULL)
	next->prev = prev;
    if (cache->firstNetwork == ptr)
	cache->firstNetwork = next;
    if (cache->lastNetwork == ptr)
	cache->lastNetwork = prev;
}

NETWORK_PRIVATE void
gaianet_reset_last_error_msg (GaiaNetworkAccessorPtr accessor)
{
/* resets the last Network error message */
    struct gaia_network *net = (struct gaia_network *) accessor;
    if (net == NULL)
	return;

    if (net->last_error_message != NULL)
	free (net->last_error_message);
    net->last_error_message = NULL;
}

NETWORK_PRIVATE void
gaianet_set_last_error_msg (GaiaNetworkAccessorPtr accessor, const char *msg)
{
/* sets the last Network error message */
    int len;
    struct gaia_network *net = (struct gaia_network *) accessor;
    if (msg == NULL)
    msg = "no message available";

    spatialite_e ("%s\n", msg);
    if (net == NULL)
	return;

    if (net->last_error_message != NULL)
	return;

    len = strlen (msg);
    net->last_error_message = malloc (len + 1);
    strcpy (net->last_error_message, msg);
}

NETWORK_PRIVATE const char *
gaianet_get_last_exception (GaiaNetworkAccessorPtr accessor)
{
/* returns the last Network error message */
    struct gaia_network *net = (struct gaia_network *) accessor;
    if (net == NULL)
	return NULL;

    return net->last_error_message;
}

GAIANET_DECLARE int
gaiaNetworkDrop (sqlite3 * handle, const char *network_name)
{
/* attempting to drop an already existing Network */
    int ret;
    char *sql;

/* creating the Networks table (just in case) */
    if (!do_create_networks (handle))
	return 0;

/* testing for existing DBMS objects */
    if (!check_existing_network (handle, network_name, 0))
	return 0;

/* dropping the Network own Tables */
    if (!do_drop_network_table (handle, network_name, "link"))
	goto error;
    if (!do_drop_network_table (handle, network_name, "node"))
	goto error;

/* unregistering the Network */
    sql =
	sqlite3_mprintf
	("DELETE FROM networks WHERE Lower(network_name) = Lower(%Q)",
	 network_name);
    ret = sqlite3_exec (handle, sql, NULL, NULL, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto error;

    return 1;

  error:
    return 0;
}

GAIANET_DECLARE sqlite3_int64
gaiaAddIsoNetNode (GaiaNetworkAccessorPtr accessor, gaiaPointPtr pt)
{
/* LWN wrapper - AddIsoNetNode */
    sqlite3_int64 ret;
    LWN_POINT *point = NULL;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    if (pt != NULL)
      {
	  if (pt->DimensionModel == GAIA_XY_Z
	      || pt->DimensionModel == GAIA_XY_Z_M)
	      point = lwn_create_point3d (network->srid, pt->X, pt->Y, pt->Z);
	  else
	      point = lwn_create_point2d (network->srid, pt->X, pt->Y);
      }
    lwn_ResetErrorMsg (network->lwn_iface);
    ret = lwn_AddIsoNetNode ((LWN_NETWORK *) (network->lwn_network), point);
    lwn_free_point (point);

    return ret;
}

GAIANET_DECLARE int
gaiaMoveIsoNetNode (GaiaNetworkAccessorPtr accessor,
		    sqlite3_int64 node, gaiaPointPtr pt)
{
/* LWN wrapper - MoveIsoNetNode */
    int ret;
    LWN_POINT *point = NULL;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    if (pt != NULL)
      {
	  if (pt->DimensionModel == GAIA_XY_Z
	      || pt->DimensionModel == GAIA_XY_Z_M)
	      point = lwn_create_point3d (network->srid, pt->X, pt->Y, pt->Z);
	  else
	      point = lwn_create_point2d (network->srid, pt->X, pt->Y);
      }
    lwn_ResetErrorMsg (network->lwn_iface);
    ret =
	lwn_MoveIsoNetNode ((LWN_NETWORK *) (network->lwn_network), node,
			    point);
    lwn_free_point (point);

    if (ret == 0)
	return 1;
    return 0;
}

GAIANET_DECLARE int
gaiaRemIsoNetNode (GaiaNetworkAccessorPtr accessor, sqlite3_int64 node)
{
/* LWN wrapper - RemIsoNetNode */
    int ret;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    lwn_ResetErrorMsg (network->lwn_iface);
    ret = lwn_RemIsoNetNode ((LWN_NETWORK *) (network->lwn_network), node);

    if (ret == 0)
	return 1;
    return 0;
}

GAIANET_DECLARE sqlite3_int64
gaiaAddLink (GaiaNetworkAccessorPtr accessor,
	     sqlite3_int64 start_node, sqlite3_int64 end_node,
	     gaiaLinestringPtr ln)
{
/* LWN wrapper - AddLink */
    sqlite3_int64 ret;
    LWN_LINE *lwn_line = NULL;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    if (ln != NULL)
      {
	  lwn_line =
	      gaianet_convert_linestring_to_lwnline (ln, network->srid,
						     network->has_z);
      }

    lwn_ResetErrorMsg (network->lwn_iface);
    ret =
	lwn_AddLink ((LWN_NETWORK *) (network->lwn_network), start_node,
		     end_node, lwn_line);

    lwn_free_line (lwn_line);
    return ret;
}

GAIANET_DECLARE int
gaiaChangeLinkGeom (GaiaNetworkAccessorPtr accessor,
		    sqlite3_int64 link_id, gaiaLinestringPtr ln)
{
/* LWN wrapper - ChangeLinkGeom */
    int ret;
    LWN_LINE *lwn_line = NULL;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    if (ln != NULL)
      {
	  lwn_line =
	      gaianet_convert_linestring_to_lwnline (ln, network->srid,
						     network->has_z);
      }

    lwn_ResetErrorMsg (network->lwn_iface);
    ret =
	lwn_ChangeLinkGeom ((LWN_NETWORK *) (network->lwn_network), link_id,
			    lwn_line);
    lwn_free_line (lwn_line);

    if (ret == 0)
	return 1;
    return 0;
}

GAIANET_DECLARE int
gaiaRemoveLink (GaiaNetworkAccessorPtr accessor, sqlite3_int64 link)
{
/* LWN wrapper - RemoveLink */
    int ret;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    lwn_ResetErrorMsg (network->lwn_iface);
    ret = lwn_RemoveLink ((LWN_NETWORK *) (network->lwn_network), link);

    if (ret == 0)
	return 1;
    return 0;
}

GAIANET_DECLARE sqlite3_int64
gaiaNewLogLinkSplit (GaiaNetworkAccessorPtr accessor, sqlite3_int64 link)
{
/* LWN wrapper - NewLogLinkSplit  */
    sqlite3_int64 ret;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    lwn_ResetErrorMsg (network->lwn_iface);
    ret = lwn_NewLogLinkSplit ((LWN_NETWORK *) (network->lwn_network), link);
    return ret;
}

GAIANET_DECLARE sqlite3_int64
gaiaModLogLinkSplit (GaiaNetworkAccessorPtr accessor, sqlite3_int64 link)
{
/* LWN wrapper - ModLogLinkSplit  */
    sqlite3_int64 ret;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    lwn_ResetErrorMsg (network->lwn_iface);
    ret = lwn_ModLogLinkSplit ((LWN_NETWORK *) (network->lwn_network), link);
    return ret;
}

GAIANET_DECLARE sqlite3_int64
gaiaNewGeoLinkSplit (GaiaNetworkAccessorPtr accessor, sqlite3_int64 link,
		     gaiaPointPtr pt)
{
/* LWN wrapper - NewGeoLinkSplit  */
    sqlite3_int64 ret;
    LWN_POINT *point = NULL;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    if (pt != NULL)
      {
	  if (pt->DimensionModel == GAIA_XY_Z
	      || pt->DimensionModel == GAIA_XY_Z_M)
	      point = lwn_create_point3d (network->srid, pt->X, pt->Y, pt->Z);
	  else
	      point = lwn_create_point2d (network->srid, pt->X, pt->Y);
      }
    lwn_ResetErrorMsg (network->lwn_iface);
    ret =
	lwn_NewGeoLinkSplit ((LWN_NETWORK *) (network->lwn_network), link,
			     point);
    lwn_free_point (point);
    return ret;
}

GAIANET_DECLARE sqlite3_int64
gaiaModGeoLinkSplit (GaiaNetworkAccessorPtr accessor, sqlite3_int64 link,
		     gaiaPointPtr pt)
{
/* LWN wrapper - ModGeoLinkSplit  */
    sqlite3_int64 ret;
    LWN_POINT *point = NULL;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    if (pt != NULL)
      {
	  if (pt->DimensionModel == GAIA_XY_Z
	      || pt->DimensionModel == GAIA_XY_Z_M)
	      point = lwn_create_point3d (network->srid, pt->X, pt->Y, pt->Z);
	  else
	      point = lwn_create_point2d (network->srid, pt->X, pt->Y);
      }
    lwn_ResetErrorMsg (network->lwn_iface);
    ret =
	lwn_ModGeoLinkSplit ((LWN_NETWORK *) (network->lwn_network), link,
			     point);
    lwn_free_point (point);
    return ret;
}

GAIANET_DECLARE sqlite3_int64
gaiaNewLinkHeal (GaiaNetworkAccessorPtr accessor, sqlite3_int64 link,
		 sqlite3_int64 anotherlink)
{
/* LWN wrapper - NewLinkHeal  */
    sqlite3_int64 ret;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    lwn_ResetErrorMsg (network->lwn_iface);
    ret =
	lwn_NewLinkHeal ((LWN_NETWORK *) (network->lwn_network), link,
			 anotherlink);

    return ret;
}

GAIANET_DECLARE sqlite3_int64
gaiaModLinkHeal (GaiaNetworkAccessorPtr accessor, sqlite3_int64 link,
		 sqlite3_int64 anotherlink)
{
/* LWN wrapper - ModLinkHeal  */
    sqlite3_int64 ret;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    lwn_ResetErrorMsg (network->lwn_iface);
    ret =
	lwn_ModLinkHeal ((LWN_NETWORK *) (network->lwn_network), link,
			 anotherlink);

    return ret;
}

GAIANET_DECLARE sqlite3_int64
gaiaGetNetNodeByPoint (GaiaNetworkAccessorPtr accessor, gaiaPointPtr pt,
		       double tolerance)
{
/* LWN wrapper - GetNetNodeByPoint */
    sqlite3_int64 ret;
    LWN_POINT *point = NULL;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    if (pt != NULL)
      {
	  if (pt->DimensionModel == GAIA_XY_Z
	      || pt->DimensionModel == GAIA_XY_Z_M)
	      point = lwn_create_point3d (network->srid, pt->X, pt->Y, pt->Z);
	  else
	      point = lwn_create_point2d (network->srid, pt->X, pt->Y);
      }
    lwn_ResetErrorMsg (network->lwn_iface);

    ret =
	lwn_GetNetNodeByPoint ((LWN_NETWORK *) (network->lwn_network), point,
			       tolerance);

    lwn_free_point (point);
    return ret;
}

GAIANET_DECLARE sqlite3_int64
gaiaGetLinkByPoint (GaiaNetworkAccessorPtr accessor, gaiaPointPtr pt,
		    double tolerance)
{
/* LWN wrapper - GetLinkByPoint */
    sqlite3_int64 ret;
    LWN_POINT *point = NULL;
    struct gaia_network *network = (struct gaia_network *) accessor;
    if (network == NULL)
	return 0;

    if (pt != NULL)
      {
	  if (pt->DimensionModel == GAIA_XY_Z
	      || pt->DimensionModel == GAIA_XY_Z_M)
	      point = lwn_create_point3d (network->srid, pt->X, pt->Y, pt->Z);
	  else
	      point = lwn_create_point2d (network->srid, pt->X, pt->Y);
      }
    lwn_ResetErrorMsg (network->lwn_iface);

    ret =
	lwn_GetLinkByPoint ((LWN_NETWORK *) (network->lwn_network), point,
			    tolerance);

    lwn_free_point (point);
    return ret;
}

static int
do_check_create_valid_logicalnet_table (GaiaNetworkAccessorPtr accessor)
{
/* attemtping to create or validate the target table */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    char *errMsg = NULL;
    struct gaia_network *net = (struct gaia_network *) accessor;

/* attempting to drop the table (just in case if it already exists) */
    table = sqlite3_mprintf ("%s_valid_logicalnet", net->network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("DROP TABLE IF EXISTS temp.\"%s\"", xtable);
    free (xtable);
    ret = sqlite3_exec (net->db_handle, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("ST_ValidLogicalNet exception: %s", errMsg);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  sqlite3_free (errMsg);
	  return 0;
      }

/* attempting to create the table */
    table = sqlite3_mprintf ("%s_valid_logicalnet", net->network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("CREATE TEMP TABLE \"%s\" (\n\terror TEXT,\n"
	 "\tprimitive1 INTEGER,\n\tprimitive2 INTEGER)", xtable);
    free (xtable);
    ret = sqlite3_exec (net->db_handle, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("ST_ValidLogicalNet exception: %s", errMsg);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  sqlite3_free (errMsg);
	  return 0;
      }

    return 1;
}

static int
do_loginet_check_nodes (GaiaNetworkAccessorPtr accessor, sqlite3_stmt * stmt)
{
/* checking for nodes with geometry */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_network *net = (struct gaia_network *) accessor;

    table = sqlite3_mprintf ("%s_node", net->network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("SELECT node_id FROM \"%s\" WHERE geometry IS NOT NULL", xtable);
    free (xtable);
    ret =
	sqlite3_prepare_v2 (net->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("ST_ValidLogicalNet() - Nodes error: \"%s\"",
			       sqlite3_errmsg (net->db_handle));
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 node_id = sqlite3_column_int64 (stmt_in, 0);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "node has geometry", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, node_id);
		sqlite3_bind_null (stmt, 3);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidLogicalNet() insert error: \"%s\"",
			   sqlite3_errmsg (net->db_handle));
		      gaianet_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidLogicalNet() - Nodes step error: %s",
		     sqlite3_errmsg (net->db_handle));
		gaianet_set_last_error_msg ((GaiaNetworkAccessorPtr) net, msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

static int
do_loginet_check_links (GaiaNetworkAccessorPtr accessor, sqlite3_stmt * stmt)
{
/* checking for links with geometry */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_network *net = (struct gaia_network *) accessor;

    table = sqlite3_mprintf ("%s_link", net->network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("SELECT link_id FROM \"%s\" WHERE geometry IS NOT NULL", xtable);
    free (xtable);
    ret =
	sqlite3_prepare_v2 (net->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("ST_ValidLogicalNet() - Links error: \"%s\"",
			       sqlite3_errmsg (net->db_handle));
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 link_id = sqlite3_column_int64 (stmt_in, 0);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "link has geometry", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, link_id);
		sqlite3_bind_null (stmt, 3);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidLogicalNet() insert error: \"%s\"",
			   sqlite3_errmsg (net->db_handle));
		      gaianet_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidLogicalNet() - Links step error: %s",
		     sqlite3_errmsg (net->db_handle));
		gaianet_set_last_error_msg ((GaiaNetworkAccessorPtr) net, msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

GAIANET_DECLARE int
gaiaValidLogicalNet (GaiaNetworkAccessorPtr accessor)
{
/* generating a validity report for a given Logical Network */
    char *table;
    char *xtable;
    char *sql;
    int ret;
    sqlite3_stmt *stmt = NULL;
    struct gaia_network *net = (struct gaia_network *) accessor;
    if (net == NULL)
	return 0;

    if (!do_check_create_valid_logicalnet_table (accessor))
	return 0;

    table = sqlite3_mprintf ("%s_valid_logicalnet", net->network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("INSERT INTO \"%s\" (error, primitive1, primitive2) VALUES (?, ?, ?)",
	 xtable);
    free (xtable);
    ret = sqlite3_prepare_v2 (net->db_handle, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg = sqlite3_mprintf ("ST_ValidLogicalNet error: \"%s\"",
				       sqlite3_errmsg (net->db_handle));
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    if (!do_loginet_check_nodes (accessor, stmt))
	goto error;

    if (!do_loginet_check_links (accessor, stmt))
	goto error;

    sqlite3_finalize (stmt);
    return 1;

  error:
    if (stmt != NULL)
	sqlite3_finalize (stmt);
    return 0;
}

static int
do_check_create_valid_spatialnet_table (GaiaNetworkAccessorPtr accessor)
{
/* attemtping to create or validate the target table */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    char *errMsg;
    struct gaia_network *net = (struct gaia_network *) accessor;

/* attempting to drop the table (just in case if it already exists) */
    table = sqlite3_mprintf ("%s_valid_spatialnet", net->network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("DROP TABLE IF EXISTS temp.\"%s\"", xtable);
    free (xtable);
    ret = sqlite3_exec (net->db_handle, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("ST_ValidSpatialNet exception: %s", errMsg);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  sqlite3_free (errMsg);
	  return 0;
      }

/* attempting to create the table */
    table = sqlite3_mprintf ("%s_valid_spatialnet", net->network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("CREATE TEMP TABLE \"%s\" (\n\terror TEXT,\n"
	 "\tprimitive1 INTEGER,\n\tprimitive2 INTEGER)", xtable);
    free (xtable);
    ret = sqlite3_exec (net->db_handle, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("ST_ValidSpatialNet exception: %s", errMsg);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  sqlite3_free (errMsg);
	  return 0;
      }

    return 1;
}

static int
do_spatnet_check_nodes (GaiaNetworkAccessorPtr accessor, sqlite3_stmt * stmt)
{
/* checking for nodes without geometry */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_network *net = (struct gaia_network *) accessor;

    table = sqlite3_mprintf ("%s_node", net->network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf ("SELECT node_id FROM \"%s\" WHERE geometry IS NULL",
			 xtable);
    free (xtable);
    ret =
	sqlite3_prepare_v2 (net->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("ST_ValidSpatialNet() - Nodes error: \"%s\"",
			       sqlite3_errmsg (net->db_handle));
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 node_id = sqlite3_column_int64 (stmt_in, 0);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "missing node geometry", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, node_id);
		sqlite3_bind_null (stmt, 3);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidSpatialNet() insert error: \"%s\"",
			   sqlite3_errmsg (net->db_handle));
		      gaianet_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidSpatialNet() - Nodes step error: %s",
		     sqlite3_errmsg (net->db_handle));
		gaianet_set_last_error_msg ((GaiaNetworkAccessorPtr) net, msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

static int
do_spatnet_check_links (GaiaNetworkAccessorPtr accessor, sqlite3_stmt * stmt)
{
/* checking for links without geometry */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_network *net = (struct gaia_network *) accessor;

    table = sqlite3_mprintf ("%s_link", net->network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf ("SELECT link_id FROM \"%s\" WHERE geometry IS NULL",
			 xtable);
    free (xtable);
    ret =
	sqlite3_prepare_v2 (net->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("ST_ValidSpatialNet() - Links error: \"%s\"",
			       sqlite3_errmsg (net->db_handle));
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 link_id = sqlite3_column_int64 (stmt_in, 0);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "missing link geometry", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, link_id);
		sqlite3_bind_null (stmt, 3);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidSpatialNet() insert error: \"%s\"",
			   sqlite3_errmsg (net->db_handle));
		      gaianet_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidSpatialNet() - Links step error: %s",
		     sqlite3_errmsg (net->db_handle));
		gaianet_set_last_error_msg ((GaiaNetworkAccessorPtr) net, msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

static int
do_spatnet_check_start_nodes (GaiaNetworkAccessorPtr accessor,
			      sqlite3_stmt * stmt)
{
/* checking for links mismatching start nodes */
    char *sql;
    char *table;
    char *xtable1;
    char *xtable2;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_network *net = (struct gaia_network *) accessor;

    table = sqlite3_mprintf ("%s_link", net->network_name);
    xtable1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_node", net->network_name);
    xtable2 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("SELECT l.link_id, l.start_node FROM \"%s\" AS l "
			   "JOIN \"%s\" AS n ON (l.start_node = n.node_id) "
			   "WHERE ST_Disjoint(ST_StartPoint(l.geometry), n.geometry) = 1",
			   xtable1, xtable2);
    free (xtable1);
    free (xtable2);
    ret =
	sqlite3_prepare_v2 (net->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf
	      ("ST_ValidSpatialNet() - StartNodes error: \"%s\"",
	       sqlite3_errmsg (net->db_handle));
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 link_id = sqlite3_column_int64 (stmt_in, 0);
		sqlite3_int64 node_id = sqlite3_column_int64 (stmt_in, 1);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "geometry start mismatch", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, link_id);
		sqlite3_bind_int64 (stmt, 3, node_id);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidSpatialNet() insert error: \"%s\"",
			   sqlite3_errmsg (net->db_handle));
		      gaianet_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidSpatialNet() - StartNodes step error: %s",
		     sqlite3_errmsg (net->db_handle));
		gaianet_set_last_error_msg ((GaiaNetworkAccessorPtr) net, msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

static int
do_spatnet_check_end_nodes (GaiaNetworkAccessorPtr accessor,
			    sqlite3_stmt * stmt)
{
/* checking for links mismatching end nodes */
    char *sql;
    char *table;
    char *xtable1;
    char *xtable2;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_network *net = (struct gaia_network *) accessor;

    table = sqlite3_mprintf ("%s_link", net->network_name);
    xtable1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_node", net->network_name);
    xtable2 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("SELECT l.link_id, l.end_node FROM \"%s\" AS l "
			   "JOIN \"%s\" AS n ON (l.end_node = n.node_id) "
			   "WHERE ST_Disjoint(ST_EndPoint(l.geometry), n.geometry) = 1",
			   xtable1, xtable2);
    free (xtable1);
    free (xtable2);
    ret =
	sqlite3_prepare_v2 (net->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("ST_ValidSpatialNet() - EndNodes error: \"%s\"",
			       sqlite3_errmsg (net->db_handle));
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 link_id = sqlite3_column_int64 (stmt_in, 0);
		sqlite3_int64 node_id = sqlite3_column_int64 (stmt_in, 1);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "geometry end mismatch", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, link_id);
		sqlite3_bind_int64 (stmt, 3, node_id);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidSpatialNet() insert error: \"%s\"",
			   sqlite3_errmsg (net->db_handle));
		      gaianet_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidSpatialNet() - EndNodes step error: %s",
		     sqlite3_errmsg (net->db_handle));
		gaianet_set_last_error_msg ((GaiaNetworkAccessorPtr) net, msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

GAIANET_DECLARE int
gaiaValidSpatialNet (GaiaNetworkAccessorPtr accessor)
{
/* generating a validity report for a given Spatial Network */
    char *table;
    char *xtable;
    char *sql;
    int ret;
    sqlite3_stmt *stmt = NULL;
    struct gaia_network *net = (struct gaia_network *) accessor;
    if (net == NULL)
	return 0;

    if (!do_check_create_valid_spatialnet_table (accessor))
	return 0;

    table = sqlite3_mprintf ("%s_valid_spatialnet", net->network_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("INSERT INTO \"%s\" (error, primitive1, primitive2) VALUES (?, ?, ?)",
	 xtable);
    free (xtable);
    ret = sqlite3_prepare_v2 (net->db_handle, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg = sqlite3_mprintf ("ST_ValidSpatialNet error: \"%s\"",
				       sqlite3_errmsg (net->db_handle));
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    if (!do_spatnet_check_nodes (accessor, stmt))
	goto error;

    if (!do_spatnet_check_links (accessor, stmt))
	goto error;

    if (!do_spatnet_check_start_nodes (accessor, stmt))
	goto error;

    if (!do_spatnet_check_end_nodes (accessor, stmt))
	goto error;

    sqlite3_finalize (stmt);
    return 1;

  error:
    if (stmt != NULL)
	sqlite3_finalize (stmt);
    return 0;
}

#endif /* end TOPOLOGY conditionals */
