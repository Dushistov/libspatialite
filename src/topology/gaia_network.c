/*

 gaia_network.c -- implementation of Topology-Network SQL functions
    
 version 4.3, 2015 August 11

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

#include <spatialite.h>
#include <spatialite_private.h>

#include <lwn_network.h>

#include "network_private.h"

#define GAIA_UNUSED() if (argc || argv) argc = argc;


SPATIALITE_PRIVATE void
start_net_savepoint (const void *handle, const void *data)
{
/* starting a new SAVEPOINT */
    char *sql;
    int ret;
    char *err_msg;
    sqlite3 *sqlite = (sqlite3 *) handle;
    struct splite_internal_cache *cache = (struct splite_internal_cache *) data;
    if (sqlite == NULL || cache == NULL)
	return;

/* creating an unique SavePoint name */
    if (cache->network_savepoint_name != NULL)
	sqlite3_free (cache->network_savepoint_name);
    cache->network_savepoint_name = NULL;
    cache->network_savepoint_name =
	sqlite3_mprintf ("netsvpt%04x", cache->next_network_savepoint);
    if (cache->next_network_savepoint >= 0xffffffffu)
	cache->next_network_savepoint = 0;
    else
	cache->next_network_savepoint += 1;

/* starting a SavePoint */
    sql = sqlite3_mprintf ("SAVEPOINT %s", cache->network_savepoint_name);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("%s - error: %s\n", sql, err_msg);
	  sqlite3_free (err_msg);
      }
    sqlite3_free (sql);
}

SPATIALITE_PRIVATE void
release_net_savepoint (const void *handle, const void *data)
{
/* releasing the current SAVEPOINT (if any) */
    char *sql;
    int ret;
    char *err_msg;
    sqlite3 *sqlite = (sqlite3 *) handle;
    struct splite_internal_cache *cache = (struct splite_internal_cache *) data;
    if (sqlite == NULL || cache == NULL)
	return;
    if (cache->network_savepoint_name == NULL)
	return;

/* releasing the current SavePoint */
    sql =
	sqlite3_mprintf ("RELEASE SAVEPOINT %s", cache->network_savepoint_name);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("%s - error: %s\n", sql, err_msg);
	  sqlite3_free (err_msg);
      }
    sqlite3_free (sql);
    sqlite3_free (cache->network_savepoint_name);
    cache->network_savepoint_name = NULL;
}

SPATIALITE_PRIVATE void
rollback_net_savepoint (const void *handle, const void *data)
{
/* rolling back the current SAVEPOINT (if any) */
    char *sql;
    int ret;
    char *err_msg;
    sqlite3 *sqlite = (sqlite3 *) handle;
    struct splite_internal_cache *cache = (struct splite_internal_cache *) data;
    if (sqlite == NULL || cache == NULL)
	return;
    if (cache->network_savepoint_name == NULL)
	return;

/* rolling back the current SavePoint */
    sql =
	sqlite3_mprintf ("ROLLBACK TO SAVEPOINT %s",
			 cache->network_savepoint_name);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("%s - error: %s\n", sql, err_msg);
	  sqlite3_free (err_msg);
      }
    sqlite3_free (sql);
/* releasing the current SavePoint */
    sql =
	sqlite3_mprintf ("RELEASE SAVEPOINT %s", cache->network_savepoint_name);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("%s - error: %s\n", sql, err_msg);
	  sqlite3_free (err_msg);
      }
    sqlite3_free (sql);
    sqlite3_free (cache->network_savepoint_name);
    cache->network_savepoint_name = NULL;
}

SPATIALITE_PRIVATE void
fnctaux_GetLastNetworkException (const void *xcontext, int argc,
				 const void *xargv)
{
/* SQL function:
/ GetLastNetworkException  ( text network-name )
/
/ returns: the more recent exception raised by given Topology-Network
/ NULL on invalid args (or when there is no pending exception)
*/
    const char *network_name;
    GaiaNetworkAccessorPtr accessor;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
      {
	  sqlite3_result_null (context);
	  return;
      }

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }

    sqlite3_result_text (context, gaianet_get_last_exception (accessor), -1,
			 SQLITE_STATIC);
}

SPATIALITE_PRIVATE void
fnctaux_CreateNetwork (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ ST_InitTopoNet ( text network-name )
/ CreateNetwork ( text network-name )
/ CreateNetwork ( text network-name, bool spatial )
/ CreateNetwork ( text network-name, bool spatial, int srid )
/ CreateNetwork ( text network-name, bool spatial, int srid, bool hasZ )
/ CreateNetwork ( text network-name, bool spatial, int srid, bool hasZ,
/                 bool allow_coincident )
/
/ returns: 1 on success, 0 on failure
/ -1 on invalid args
*/
    int ret;
    const char *network_name;
    int srid = -1;
    int has_z = 0;
    int spatial = 0;
    int allow_coincident = 1;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (argc >= 2)
      {
	  if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	      ;
	  else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	      spatial = sqlite3_value_int (argv[1]);
	  else
	    {
		sqlite3_result_int (context, -1);
		return;
	    }
      }
    if (argc >= 3)
      {
	  if (sqlite3_value_type (argv[2]) == SQLITE_NULL)
	      ;
	  else if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
	      srid = sqlite3_value_int (argv[2]);
	  else
	    {
		sqlite3_result_int (context, -1);
		return;
	    }
      }
    if (argc >= 4)
      {
	  if (sqlite3_value_type (argv[3]) == SQLITE_NULL)
	      ;
	  else if (sqlite3_value_type (argv[3]) == SQLITE_INTEGER)
	      has_z = sqlite3_value_int (argv[3]);
	  else
	    {
		sqlite3_result_int (context, -1);
		return;
	    }
      }
    if (argc >= 5)
      {
	  if (sqlite3_value_type (argv[4]) == SQLITE_NULL)
	      ;
	  else if (sqlite3_value_type (argv[4]) == SQLITE_INTEGER)
	      allow_coincident = sqlite3_value_int (argv[4]);
	  else
	    {
		sqlite3_result_int (context, -1);
		return;
	    }
      }

    start_net_savepoint (sqlite, cache);
    ret =
	gaiaNetworkCreate (sqlite, network_name, spatial, srid, has_z,
			   allow_coincident);
    if (!ret)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    sqlite3_result_int (context, ret);
}

SPATIALITE_PRIVATE void
fnctaux_DropNetwork (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ DropNetwork ( text network-name )
/
/ returns: 1 on success, 0 on failure
/ -1 on invalid args
*/
    int ret;
    const char *network_name;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GaiaNetworkAccessorPtr accessor;
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
      {
	  sqlite3_result_int (context, -1);
	  return;
      }

    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor != NULL)
	gaiaNetworkDestroy (accessor);

    start_net_savepoint (sqlite, cache);
    ret = gaiaNetworkDrop (sqlite, network_name);
    if (!ret)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    sqlite3_result_int (context, ret);
}

static int
check_matching_srid_dims (GaiaNetworkAccessorPtr accessor, int srid, int dims)
{
/* checking for matching SRID and DIMs */
    struct gaia_network *net = (struct gaia_network *) accessor;
    if (net->srid != srid)
	return 0;
    if (net->has_z)
      {
	  if (dims == GAIA_XY_Z || dims == GAIA_XY_Z_M)
	      ;
	  else
	      return 0;
      }
    else
      {
	  if (dims == GAIA_XY_Z || dims == GAIA_XY_Z_M)
	      return 0;
      }
    return 1;
}

SPATIALITE_PRIVATE void
fnctaux_AddIsoNetNode (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ ST_AddIsoNetNode ( text network-name, Geometry point )
/
/ returns: the ID of the inserted Node on success
/ raises an exception on failure
*/
    sqlite3_int64 ret;
    const char *network_name;
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr point = NULL;
    gaiaPointPtr pt = NULL;
    int invalid = 0;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    int gpkg_amphibious = 0;
    int gpkg_mode = 0;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (cache != NULL)
      {
	  gpkg_amphibious = cache->gpkg_amphibious_mode;
	  gpkg_mode = cache->gpkg_mode;
      }
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;

    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
      {
	  if (net->spatial)
	      goto spatial_err;
      }
    else if (sqlite3_value_type (argv[1]) == SQLITE_BLOB)
      {
	  if (net->spatial == 0)
	      goto logical_err;
	  p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
	  n_bytes = sqlite3_value_bytes (argv[1]);

	  /* attempting to get a Point Geometry */
	  point =
	      gaiaFromSpatiaLiteBlobWkbEx (p_blob, n_bytes, gpkg_mode,
					   gpkg_amphibious);
	  if (!point)
	      goto invalid_arg;
	  if (point->FirstLinestring != NULL)
	      invalid = 1;
	  if (point->FirstPolygon != NULL)
	      invalid = 1;
	  if (point->FirstPoint != point->LastPoint
	      || point->FirstPoint == NULL)
	      invalid = 1;
	  if (invalid)
	      goto invalid_arg;

	  if (!check_matching_srid_dims
	      (accessor, point->Srid, point->DimensionModel))
	      goto invalid_geom;
	  pt = point->FirstPoint;
      }
    else
	goto invalid_arg;

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaAddIsoNetNode (accessor, pt);
    if (ret <= 0)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    if (point != NULL)
      {
	  gaiaFreeGeomColl (point);
	  point = NULL;
      }
    if (ret <= 0)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  return;
      }
    sqlite3_result_int64 (context, ret);
    return;

  no_net:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;

  invalid_geom:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid geometry (mismatching SRID or dimensions).",
			  -1);
    return;

  spatial_err:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - Spatial Network can't accept null geometry.",
			  -1);
    return;

  logical_err:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - Logical Network can't accept not null geometry.",
			  -1);
    return;
}

SPATIALITE_PRIVATE void
fnctaux_MoveIsoNetNode (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ ST_MoveIsoNetNode ( text network-name, int node_id, Geometry point )
/
/ returns: TEXT (description of new location)
/ raises an exception on failure
*/
    char xid[80];
    char *newpos = NULL;
    int ret;
    const char *net_name;
    sqlite3_int64 node_id;
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr point = NULL;
    gaiaPointPtr pt = NULL;
    int invalid = 0;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    int gpkg_amphibious = 0;
    int gpkg_mode = 0;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (cache != NULL)
      {
	  gpkg_amphibious = cache->gpkg_amphibious_mode;
	  gpkg_mode = cache->gpkg_mode;
      }
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	net_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	node_id = sqlite3_value_int64 (argv[1]);
    else
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, net_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;

    if (sqlite3_value_type (argv[2]) == SQLITE_NULL)
      {
	  if (net->spatial)
	      goto spatial_err;
      }
    else if (sqlite3_value_type (argv[2]) == SQLITE_BLOB)
      {
	  if (net->spatial == 0)
	      goto logical_err;
	  p_blob = (unsigned char *) sqlite3_value_blob (argv[2]);
	  n_bytes = sqlite3_value_bytes (argv[2]);

	  /* attempting to get a Point Geometry */
	  point =
	      gaiaFromSpatiaLiteBlobWkbEx (p_blob, n_bytes, gpkg_mode,
					   gpkg_amphibious);
	  if (!point)
	      goto invalid_arg;
	  if (point->FirstLinestring != NULL)
	      invalid = 1;
	  if (point->FirstPolygon != NULL)
	      invalid = 1;
	  if (point->FirstPoint != point->LastPoint
	      || point->FirstPoint == NULL)
	      invalid = 1;
	  if (invalid)
	      goto invalid_arg;
	  if (!check_matching_srid_dims
	      (accessor, point->Srid, point->DimensionModel))
	      goto invalid_geom;
	  pt = point->FirstPoint;
      }
    else
	goto invalid_arg;
    sprintf (xid, "%lld", node_id);
    if (pt == NULL)
	newpos =
	    sqlite3_mprintf ("Isolated Node %s moved to NULL location", xid);
    else
	newpos =
	    sqlite3_mprintf ("Isolated Node %s moved to location %f,%f", xid,
			     pt->X, pt->Y);

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaMoveIsoNetNode (accessor, node_id, pt);
    if (!ret)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    if (point != NULL)
	gaiaFreeGeomColl (point);
    point = NULL;
    if (!ret)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  if (newpos != NULL)
	      sqlite3_free (newpos);
	  return;
      }
    sqlite3_result_text (context, newpos, strlen (newpos), sqlite3_free);
    return;

  no_net:
    if (newpos != NULL)
	sqlite3_free (newpos);
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    if (newpos != NULL)
	sqlite3_free (newpos);
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    if (newpos != NULL)
	sqlite3_free (newpos);
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;

  invalid_geom:
    if (newpos != NULL)
	sqlite3_free (newpos);
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid geometry (mismatching SRID or dimensions).",
			  -1);
    return;

  spatial_err:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - Spatial Network can't accept null geometry.",
			  -1);
    return;

  logical_err:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - Logical Network can't accept not null geometry.",
			  -1);
    return;
}

SPATIALITE_PRIVATE void
fnctaux_RemIsoNetNode (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ ST_RemIsoNetNode ( text network-name, int node_id )
/
/ returns: TEXT (description of operation)
/ raises an exception on failure
*/
    char xid[80];
    char *newpos = NULL;
    int ret;
    const char *network_name;
    sqlite3_int64 node_id;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	node_id = sqlite3_value_int64 (argv[1]);
    else
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;
    sprintf (xid, "%lld", node_id);
    newpos = sqlite3_mprintf ("Isolated NetNode %s removed", xid);

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaRemIsoNetNode (accessor, node_id);
    if (!ret)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    if (!ret)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  if (newpos != NULL)
	      sqlite3_free (newpos);
	  return;
      }
    sqlite3_result_text (context, newpos, strlen (newpos), sqlite3_free);
    return;

  no_net:
    if (newpos != NULL)
	sqlite3_free (newpos);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    if (newpos != NULL)
	sqlite3_free (newpos);
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    if (newpos != NULL)
	sqlite3_free (newpos);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;
}

SPATIALITE_PRIVATE void
fnctaux_AddLink (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ ST_AddLink ( text network-name, int start_node_id, int end_node_id, Geometry linestring )
/
/ returns: the ID of the inserted Link on success, 0 on failure
/ raises an exception on failure
*/
    sqlite3_int64 ret;
    const char *network_name;
    sqlite3_int64 start_node_id;
    sqlite3_int64 end_node_id;
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr line = NULL;
    gaiaLinestringPtr ln = NULL;
    int invalid = 0;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    int gpkg_amphibious = 0;
    int gpkg_mode = 0;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (cache != NULL)
      {
	  gpkg_amphibious = cache->gpkg_amphibious_mode;
	  gpkg_mode = cache->gpkg_mode;
      }
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	start_node_id = sqlite3_value_int64 (argv[1]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[2]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
	end_node_id = sqlite3_value_int64 (argv[2]);
    else
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;

    if (sqlite3_value_type (argv[3]) == SQLITE_NULL)
      {
	  if (net->spatial)
	      goto spatial_err;
      }
    else if (sqlite3_value_type (argv[3]) == SQLITE_BLOB)
      {
	  if (net->spatial == 0)
	      goto logical_err;
	  p_blob = (unsigned char *) sqlite3_value_blob (argv[3]);
	  n_bytes = sqlite3_value_bytes (argv[3]);

/* attempting to get a Linestring Geometry */
	  line =
	      gaiaFromSpatiaLiteBlobWkbEx (p_blob, n_bytes, gpkg_mode,
					   gpkg_amphibious);
	  if (!line)
	      goto invalid_arg;
	  if (line->FirstPoint != NULL)
	      invalid = 1;
	  if (line->FirstPolygon != NULL)
	      invalid = 1;
	  if (line->FirstLinestring != line->LastLinestring
	      || line->FirstLinestring == NULL)
	      invalid = 1;
	  if (invalid)
	      goto invalid_arg;
	  if (!check_matching_srid_dims
	      (accessor, line->Srid, line->DimensionModel))
	      goto invalid_geom;
	  ln = line->FirstLinestring;
      }
    else
	goto invalid_arg;

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaAddLink (accessor, start_node_id, end_node_id, ln);
    if (ret <= 0)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    if (line != NULL)
	gaiaFreeGeomColl (line);
    line = NULL;
    if (ret <= 0)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  return;
      }
    sqlite3_result_int64 (context, ret);
    return;

  no_net:
    if (line != NULL)
	gaiaFreeGeomColl (line);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    if (line != NULL)
	gaiaFreeGeomColl (line);
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    if (line != NULL)
	gaiaFreeGeomColl (line);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;

  invalid_geom:
    if (line != NULL)
	gaiaFreeGeomColl (line);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid geometry (mismatching SRID or dimensions).",
			  -1);
    return;

  spatial_err:
    if (line != NULL)
	gaiaFreeGeomColl (line);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - Spatial Network can't accept null geometry.",
			  -1);
    return;

  logical_err:
    if (line != NULL)
	gaiaFreeGeomColl (line);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - Logical Network can't accept not null geometry.",
			  -1);
    return;
}

SPATIALITE_PRIVATE void
fnctaux_ChangeLinkGeom (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ ST_ChangeLinkGeom ( text network-name, int link_id, Geometry linestring )
/
/ returns: TEXT (description of operation)
/ raises an exception on failure
*/
    char xid[80];
    char *newpos = NULL;
    int ret;
    const char *network_name;
    sqlite3_int64 link_id;
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr line = NULL;
    gaiaLinestringPtr ln = NULL;
    int invalid = 0;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    int gpkg_amphibious = 0;
    int gpkg_mode = 0;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (cache != NULL)
      {
	  gpkg_amphibious = cache->gpkg_amphibious_mode;
	  gpkg_mode = cache->gpkg_mode;
      }
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	link_id = sqlite3_value_int64 (argv[1]);
    else
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;

    if (sqlite3_value_type (argv[2]) == SQLITE_NULL)
      {
	  if (net->spatial)
	      goto spatial_err;
      }
    else if (sqlite3_value_type (argv[2]) == SQLITE_BLOB)
      {
	  if (net->spatial == 0)
	      goto logical_err;
	  p_blob = (unsigned char *) sqlite3_value_blob (argv[2]);
	  n_bytes = sqlite3_value_bytes (argv[2]);

	  /* attempting to get a Linestring Geometry */
	  line =
	      gaiaFromSpatiaLiteBlobWkbEx (p_blob, n_bytes, gpkg_mode,
					   gpkg_amphibious);
	  if (!line)
	      goto invalid_arg;
	  if (line->FirstPoint != NULL)
	      invalid = 1;
	  if (line->FirstPolygon != NULL)
	      invalid = 1;
	  if (line->FirstLinestring != line->LastLinestring
	      || line->FirstLinestring == NULL)
	      invalid = 1;
	  if (invalid)
	      goto invalid_arg;

	  if (!check_matching_srid_dims
	      (accessor, line->Srid, line->DimensionModel))
	      goto invalid_geom;
	  ln = line->FirstLinestring;
      }
    else
	goto invalid_arg;
    sprintf (xid, "%lld", link_id);
    newpos = sqlite3_mprintf ("Link %s changed", xid);

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaChangeLinkGeom (accessor, link_id, ln);
    if (!ret)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    if (line != NULL)
	gaiaFreeGeomColl (line);
    line = NULL;
    if (!ret)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  if (newpos != NULL)
	      sqlite3_free (newpos);
	  return;
      }
    sqlite3_result_text (context, newpos, strlen (newpos), sqlite3_free);
    return;

  no_net:
    if (newpos != NULL)
	sqlite3_free (newpos);
    if (line != NULL)
	gaiaFreeGeomColl (line);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    if (newpos != NULL)
	sqlite3_free (newpos);
    if (line != NULL)
	gaiaFreeGeomColl (line);
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    if (newpos != NULL)
	sqlite3_free (newpos);
    if (line != NULL)
	gaiaFreeGeomColl (line);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;

  invalid_geom:
    if (newpos != NULL)
	sqlite3_free (newpos);
    if (line != NULL)
	gaiaFreeGeomColl (line);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid geometry (mismatching SRID or dimensions).",
			  -1);
    return;

  spatial_err:
    if (line != NULL)
	gaiaFreeGeomColl (line);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - Spatial Network can't accept null geometry.",
			  -1);
    return;

  logical_err:
    if (line != NULL)
	gaiaFreeGeomColl (line);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - Logical Network can't accept not null geometry.",
			  -1);
    return;
}

SPATIALITE_PRIVATE void
fnctaux_RemoveLink (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ ST_RemoveLink ( text network-name, int link_id )
/
/ returns: TEXT (description of operation)
/ raises an exception on failure
*/
    char xid[80];
    char *newpos = NULL;
    int ret;
    const char *network_name;
    sqlite3_int64 link_id;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	link_id = sqlite3_value_int64 (argv[1]);
    else
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;
    sprintf (xid, "%lld", link_id);
    newpos = sqlite3_mprintf ("Link %s removed", xid);

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaRemoveLink (accessor, link_id);
    if (!ret)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    if (!ret)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  if (newpos != NULL)
	      sqlite3_free (newpos);
	  return;
      }
    sqlite3_result_text (context, newpos, strlen (newpos), sqlite3_free);
    return;

  no_net:
    if (newpos != NULL)
	sqlite3_free (newpos);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    if (newpos != NULL)
	sqlite3_free (newpos);
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    if (newpos != NULL)
	sqlite3_free (newpos);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;
}

SPATIALITE_PRIVATE void
fnctaux_NewLogLinkSplit (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ ST_NewLogLinkSplit ( text network-name, int link_id )
/
/ returns: the ID of the inserted Node on success
/ raises an exception on failure
*/
    sqlite3_int64 ret;
    const char *network_name;
    sqlite3_int64 link_id;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	link_id = sqlite3_value_int64 (argv[1]);
    else
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;
    if (net->spatial)
	goto spatial_err;

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaNewLogLinkSplit (accessor, link_id);
    if (ret <= 0)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    if (ret <= 0)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  return;
      }
    sqlite3_result_int64 (context, ret);
    return;

  no_net:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;

  spatial_err:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - ST_NewLogLinkSplit can't support Spatial Network; try using ST_NewGeoLinkSplit.",
			  -1);
    return;
}

SPATIALITE_PRIVATE void
fnctaux_ModLogLinkSplit (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ ST_ModLogLinkSplit ( text network-name, int link_id )
/
/ returns: the ID of the inserted Node on success
/ raises an exception on failure
*/
    sqlite3_int64 ret;
    const char *network_name;
    sqlite3_int64 link_id;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	link_id = sqlite3_value_int64 (argv[1]);
    else
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;
    if (net->spatial)
	goto spatial_err;

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaModLogLinkSplit (accessor, link_id);
    if (ret <= 0)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    if (ret <= 0)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  return;
      }
    sqlite3_result_int64 (context, ret);
    return;

  no_net:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;

  spatial_err:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - ST_ModLogLinkSplit can't support Spatial Network; try using ST_ModGeoLinkSplit.",
			  -1);
    return;
}

SPATIALITE_PRIVATE void
fnctaux_NewGeoLinkSplit (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ ST_NewGeoLinkSplit ( text network-name, int link_id, Geometry point )
/
/ returns: the ID of the inserted Node on success
/ raises an exception on failure
*/
    sqlite3_int64 ret;
    const char *network_name;
    sqlite3_int64 link_id;
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr point = NULL;
    gaiaPointPtr pt;
    int invalid = 0;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    int gpkg_amphibious = 0;
    int gpkg_mode = 0;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (cache != NULL)
      {
	  gpkg_amphibious = cache->gpkg_amphibious_mode;
	  gpkg_mode = cache->gpkg_mode;
      }
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	link_id = sqlite3_value_int64 (argv[1]);
    else
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;
    if (net->spatial == 0)
	goto logical_err;

    if (sqlite3_value_type (argv[2]) == SQLITE_NULL)
	goto spatial_err;
    else if (sqlite3_value_type (argv[2]) == SQLITE_BLOB)
      {
	  p_blob = (unsigned char *) sqlite3_value_blob (argv[2]);
	  n_bytes = sqlite3_value_bytes (argv[2]);
      }
    else
	goto invalid_arg;

/* attempting to get a Point Geometry */
    point =
	gaiaFromSpatiaLiteBlobWkbEx (p_blob, n_bytes, gpkg_mode,
				     gpkg_amphibious);
    if (!point)
	goto invalid_arg;
    if (point->FirstLinestring != NULL)
	invalid = 1;
    if (point->FirstPolygon != NULL)
	invalid = 1;
    if (point->FirstPoint != point->LastPoint || point->FirstPoint == NULL)
	invalid = 1;
    if (invalid)
	goto invalid_arg;
    if (!check_matching_srid_dims
	(accessor, point->Srid, point->DimensionModel))
	goto invalid_geom;
    pt = point->FirstPoint;

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaNewGeoLinkSplit (accessor, link_id, pt);
    if (ret <= 0)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    gaiaFreeGeomColl (point);
    point = NULL;
    if (ret <= 0)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  return;
      }
    sqlite3_result_int64 (context, ret);
    return;

  no_net:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;

  invalid_geom:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid geometry (mismatching SRID or dimensions).",
			  -1);
    return;

  spatial_err:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - Spatial Network can't accept null geometry.",
			  -1);
    return;

  logical_err:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - ST_NewGeoLinkSplit can't support Logical Network; try using ST_NewLogLinkSplit.",
			  -1);
    return;
}

SPATIALITE_PRIVATE void
fnctaux_ModGeoLinkSplit (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ ST_ModGeoLinkSplit ( text network-name, int link_id, Geometry point )
/
/ returns: the ID of the inserted Node on success
/ raises an exception on failure
*/
    sqlite3_int64 ret;
    const char *network_name;
    sqlite3_int64 link_id;
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr point = NULL;
    gaiaPointPtr pt;
    int invalid = 0;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    int gpkg_amphibious = 0;
    int gpkg_mode = 0;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (cache != NULL)
      {
	  gpkg_amphibious = cache->gpkg_amphibious_mode;
	  gpkg_mode = cache->gpkg_mode;
      }
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	link_id = sqlite3_value_int64 (argv[1]);
    else
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;
    if (net->spatial == 0)
	goto logical_err;

    if (sqlite3_value_type (argv[2]) == SQLITE_NULL)
	goto spatial_err;
    else if (sqlite3_value_type (argv[2]) == SQLITE_BLOB)
      {
	  p_blob = (unsigned char *) sqlite3_value_blob (argv[2]);
	  n_bytes = sqlite3_value_bytes (argv[2]);
      }
    else
	goto invalid_arg;

/* attempting to get a Point Geometry */
    point =
	gaiaFromSpatiaLiteBlobWkbEx (p_blob, n_bytes, gpkg_mode,
				     gpkg_amphibious);
    if (!point)
	goto invalid_arg;
    if (point->FirstLinestring != NULL)
	invalid = 1;
    if (point->FirstPolygon != NULL)
	invalid = 1;
    if (point->FirstPoint != point->LastPoint || point->FirstPoint == NULL)
	invalid = 1;
    if (invalid)
	goto invalid_arg;
    if (!check_matching_srid_dims
	(accessor, point->Srid, point->DimensionModel))
	goto invalid_geom;
    pt = point->FirstPoint;

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaModGeoLinkSplit (accessor, link_id, pt);
    if (ret <= 0)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    gaiaFreeGeomColl (point);
    point = NULL;
    if (ret <= 0)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  return;
      }
    sqlite3_result_int64 (context, ret);
    return;

  no_net:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;

  invalid_geom:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid geometry (mismatching SRID or dimensions).",
			  -1);
    return;

  spatial_err:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - Spatial Network can't accept null geometry.",
			  -1);
    return;

  logical_err:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - ST_ModGeoLinkSplit can't support Logical Network; try using ST_ModLogLinkSplit.",
			  -1);
    return;
}

SPATIALITE_PRIVATE void
fnctaux_NewLinkHeal (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ ST_NewLinkHeal ( text network-name, int link_id, int anotherlink_id )
/
/ returns: the ID of the removed Node on success
/ raises an exception on failure
*/
    sqlite3_int64 ret;
    const char *network_name;
    sqlite3_int64 link_id;
    sqlite3_int64 anotherlink_id;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	link_id = sqlite3_value_int64 (argv[1]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[2]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
	anotherlink_id = sqlite3_value_int64 (argv[2]);
    else
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaNewLinkHeal (accessor, link_id, anotherlink_id);
    if (ret <= 0)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    if (ret <= 0)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  return;
      }
    sqlite3_result_int64 (context, ret);
    return;

  no_net:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;
}

SPATIALITE_PRIVATE void
fnctaux_ModLinkHeal (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ ST_ModLinkHeal ( text network-name, int link_id )
/
/ returns: the ID of the removed Node on success
/ raises an exception on failure
*/
    sqlite3_int64 ret;
    const char *network_name;
    sqlite3_int64 link_id;
    sqlite3_int64 anotherlink_id;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	link_id = sqlite3_value_int64 (argv[1]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[2]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
	anotherlink_id = sqlite3_value_int64 (argv[2]);
    else
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaModLinkHeal (accessor, link_id, anotherlink_id);
    if (ret <= 0)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    if (ret <= 0)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  return;
      }
    sqlite3_result_int64 (context, ret);
    return;

  no_net:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;
}

SPATIALITE_PRIVATE void
fnctaux_GetNetNodeByPoint (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ GetNetNodeByPoint ( text network-name, Geometry point, double tolerance )
/
/ returns: the ID of some Node on success, 0 if no Node was found
/ raises an exception on failure
*/
    sqlite3_int64 ret;
    const char *network_name;
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr point = NULL;
    gaiaPointPtr pt;
    double tolerance;
    int invalid = 0;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    int gpkg_amphibious = 0;
    int gpkg_mode = 0;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (cache != NULL)
      {
	  gpkg_amphibious = cache->gpkg_amphibious_mode;
	  gpkg_mode = cache->gpkg_mode;
      }
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[1]) == SQLITE_BLOB)
      {
	  p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
	  n_bytes = sqlite3_value_bytes (argv[1]);
      }
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[2]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
      {
	  int t = sqlite3_value_int (argv[2]);
	  tolerance = t;
      }
    else if (sqlite3_value_type (argv[2]) == SQLITE_FLOAT)
	tolerance = sqlite3_value_int (argv[2]);
    else
	goto invalid_arg;

/* attempting to get a Point Geometry */
    point =
	gaiaFromSpatiaLiteBlobWkbEx (p_blob, n_bytes, gpkg_mode,
				     gpkg_amphibious);
    if (!point)
	goto invalid_arg;
    if (point->FirstLinestring != NULL)
	invalid = 1;
    if (point->FirstPolygon != NULL)
	invalid = 1;
    if (point->FirstPoint != point->LastPoint || point->FirstPoint == NULL)
	invalid = 1;
    if (invalid)
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;
    if (net->spatial == 0)
	goto logical_err;
    pt = point->FirstPoint;

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaGetNetNodeByPoint (accessor, pt, tolerance);
    if (ret < 0)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    gaiaFreeGeomColl (point);
    point = NULL;
    if (ret < 0)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  return;
      }
    sqlite3_result_int64 (context, ret);
    return;

  no_net:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;

  logical_err:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "GetNetNodekByPoint() cannot be applied to Logical Network.",
			  -1);
    return;
}

SPATIALITE_PRIVATE void
fnctaux_GetLinkByPoint (const void *xcontext, int argc, const void *xargv)
{
/* SQL function:
/ GetLinkByPoint ( text network-name, Geometry point, double tolerance )
/
/ returns: the ID of some Link on success
/ raises an exception on failure
*/
    sqlite3_int64 ret;
    const char *network_name;
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr point = NULL;
    gaiaPointPtr pt;
    double tolerance;
    int invalid = 0;
    GaiaNetworkAccessorPtr accessor;
    struct gaia_network *net;
    int gpkg_amphibious = 0;
    int gpkg_mode = 0;
    sqlite3_context *context = (sqlite3_context *) xcontext;
    sqlite3_value **argv = (sqlite3_value **) xargv;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    struct splite_internal_cache *cache = sqlite3_user_data (context);
    GAIA_UNUSED ();		/* LCOV_EXCL_LINE */
    if (cache != NULL)
      {
	  gpkg_amphibious = cache->gpkg_amphibious_mode;
	  gpkg_mode = cache->gpkg_mode;
      }
    if (sqlite3_value_type (argv[0]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[0]) == SQLITE_TEXT)
	network_name = (const char *) sqlite3_value_text (argv[0]);
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[1]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[1]) == SQLITE_BLOB)
      {
	  p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
	  n_bytes = sqlite3_value_bytes (argv[1]);
      }
    else
	goto invalid_arg;
    if (sqlite3_value_type (argv[2]) == SQLITE_NULL)
	goto null_arg;
    else if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
      {
	  int t = sqlite3_value_int (argv[2]);
	  tolerance = t;
      }
    else if (sqlite3_value_type (argv[2]) == SQLITE_FLOAT)
	tolerance = sqlite3_value_int (argv[2]);
    else
	goto invalid_arg;

/* attempting to get a Point Geometry */
    point =
	gaiaFromSpatiaLiteBlobWkbEx (p_blob, n_bytes, gpkg_mode,
				     gpkg_amphibious);
    if (!point)
	goto invalid_arg;
    if (point->FirstLinestring != NULL)
	invalid = 1;
    if (point->FirstPolygon != NULL)
	invalid = 1;
    if (point->FirstPoint != point->LastPoint || point->FirstPoint == NULL)
	invalid = 1;
    if (invalid)
	goto invalid_arg;

/* attempting to get a Network Accessor */
    accessor = gaiaGetNetwork (sqlite, cache, network_name);
    if (accessor == NULL)
	goto no_net;
    net = (struct gaia_network *) accessor;
    if (net->spatial == 0)
	goto logical_err;
    pt = point->FirstPoint;

    gaianet_reset_last_error_msg (accessor);
    start_net_savepoint (sqlite, cache);
    ret = gaiaGetLinkByPoint (accessor, pt, tolerance);
    if (ret < 0)
	rollback_net_savepoint (sqlite, cache);
    else
	release_net_savepoint (sqlite, cache);
    gaiaFreeGeomColl (point);
    point = NULL;
    if (ret < 0)
      {
	  const char *msg = lwn_GetErrorMsg (net->lwn_iface);
	  gaianet_set_last_error_msg (accessor, msg);
	  sqlite3_result_error (context, msg, -1);
	  return;
      }
    sqlite3_result_int64 (context, ret);
    return;

  no_net:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid network name.",
			  -1);
    return;

  null_arg:
    sqlite3_result_error (context, "SQL/MM Spatial exception - null argument.",
			  -1);
    return;

  invalid_arg:
    sqlite3_result_error (context,
			  "SQL/MM Spatial exception - invalid argument.", -1);
    return;

  logical_err:
    if (point != NULL)
	gaiaFreeGeomColl (point);
    sqlite3_result_error (context,
			  "GetLinkByPoint() cannot be applied to Logical Network.",
			  -1);
    return;
}

#endif /* end TOPOLOGY conditionals */
