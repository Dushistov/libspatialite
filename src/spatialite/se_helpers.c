/*

 se_helpers.c -- SLD/SE helper functions

 version 4.2.1, 2014 December 9

 Author: Sandro Furieri a.furieri@lqt.it

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
 
Portions created by the Initial Developer are Copyright (C) 2008-2013
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

/*
 
CREDITS:

this module has been partly funded by:
Regione Toscana - Settore Sistema Informativo Territoriale ed Ambientale
(implementing XML support - ISO Metadata and SLD/SE Styles) 

*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#if defined(_WIN32) && !defined(__MINGW32__)
#include "config-msvc.h"
#else
#include "config.h"
#endif

#include <spatialite/sqlite.h>
#include <spatialite/debug.h>

#include <spatialite.h>
#include <spatialite_private.h>

#ifdef _WIN32
#define strcasecmp	_stricmp
#endif /* not WIN32 */


#ifdef ENABLE_LIBXML2		/* including LIBXML2 */


SPATIALITE_PRIVATE int
register_external_graphic (void *p_sqlite, const char *xlink_href,
			   const unsigned char *p_blob, int n_bytes,
			   const char *title, const char *abstract,
			   const char *file_name)
{
/* auxiliary function: inserts or updates an ExternalGraphic Resource */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int exists = 0;
    int extras = 0;
    int retval = 0;

/* checking if already exists */
    sql = "SELECT xlink_href FROM SE_external_graphics WHERE xlink_href = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("registerExternalGraphic: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, xlink_href, strlen (xlink_href), SQLITE_STATIC);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	      exists = 1;
      }
    sqlite3_finalize (stmt);

    if (title != NULL && abstract != NULL && file_name != NULL)
	extras = 1;
    if (exists)
      {
	  /* update */
	  if (extras)
	    {
		/* full infos */
		sql = "UPDATE SE_external_graphics "
		    "SET resource = ?, title = ?, abstract = ?, file_name = ? "
		    "WHERE xlink_href = ?";
	    }
	  else
	    {
		/* limited basic infos */
		sql = "UPDATE SE_external_graphics "
		    "SET resource = ? WHERE xlink_href = ?";
	    }
      }
    else
      {
	  /* insert */
	  if (extras)
	    {
		/* full infos */
		sql = "INSERT INTO SE_external_graphics "
		    "(xlink_href, resource, title, abstract, file_name) "
		    "VALUES (?, ?, ?, ?, ?)";
	    }
	  else
	    {
		/* limited basic infos */
		sql = "INSERT INTO SE_external_graphics "
		    "(xlink_href, resource) VALUES (?, ?)";
	    }
      }
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("registerExternalGraphic: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    if (exists)
      {
	  /* update */
	  if (extras)
	    {
		/* full infos */
		sqlite3_bind_blob (stmt, 1, p_blob, n_bytes, SQLITE_STATIC);
		sqlite3_bind_text (stmt, 2, title, strlen (title),
				   SQLITE_STATIC);
		sqlite3_bind_text (stmt, 3, abstract, strlen (abstract),
				   SQLITE_STATIC);
		sqlite3_bind_text (stmt, 4, file_name, strlen (file_name),
				   SQLITE_STATIC);
		sqlite3_bind_text (stmt, 5, xlink_href, strlen (xlink_href),
				   SQLITE_STATIC);
	    }
	  else
	    {
		/* limited basic infos */
		sqlite3_bind_blob (stmt, 1, p_blob, n_bytes, SQLITE_STATIC);
		sqlite3_bind_text (stmt, 2, xlink_href, strlen (xlink_href),
				   SQLITE_STATIC);
	    }
      }
    else
      {
	  /* insert */
	  if (extras)
	    {
		/* full infos */
		sqlite3_bind_text (stmt, 1, xlink_href, strlen (xlink_href),
				   SQLITE_STATIC);
		sqlite3_bind_blob (stmt, 2, p_blob, n_bytes, SQLITE_STATIC);
		sqlite3_bind_text (stmt, 3, title, strlen (title),
				   SQLITE_STATIC);
		sqlite3_bind_text (stmt, 4, abstract, strlen (abstract),
				   SQLITE_STATIC);
		sqlite3_bind_text (stmt, 5, file_name, strlen (file_name),
				   SQLITE_STATIC);
	    }
	  else
	    {
		/* limited basic infos */
		sqlite3_bind_text (stmt, 1, xlink_href, strlen (xlink_href),
				   SQLITE_STATIC);
		sqlite3_bind_blob (stmt, 2, p_blob, n_bytes, SQLITE_STATIC);
	    }
      }
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	retval = 1;
    else
	spatialite_e ("registerExternalGraphic() error: \"%s\"\n",
		      sqlite3_errmsg (sqlite));
    sqlite3_finalize (stmt);
    return retval;
  stop:
    return 0;
}

SPATIALITE_PRIVATE int
register_vector_style (void *p_sqlite, const unsigned char *p_blob, int n_bytes)
{
/* auxiliary function: inserts a Vector Style definition */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;

    if (p_blob != NULL && n_bytes > 0)
      {
	  /* attempting to insert the Vector Style */
	  sql = "INSERT INTO SE_vector_styles "
	      "(style_id, style) VALUES (NULL, ?)";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("registerVectorStyle: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		return 0;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_blob (stmt, 1, p_blob, n_bytes, SQLITE_STATIC);
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		spatialite_e ("registerVectorStyle() error: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		sqlite3_finalize (stmt);
		return 0;
	    }
	  sqlite3_finalize (stmt);
	  return 1;
      }
    else
	return 0;
}

static int
check_vector_style_by_id (sqlite3 * sqlite, int style_id)
{
/* checks if a Vector Style do actually exists - by ID */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int count = 0;

    sql = "SELECT style_id FROM SE_vector_styles " "WHERE style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Vector Style by ID: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int (stmt, 1, style_id);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	      count++;
      }
    sqlite3_finalize (stmt);
    if (count == 1)
	return 1;
    return 0;
  stop:
    return 0;
}

static int
check_vector_style_by_name (sqlite3 * sqlite, const char *style_name,
			    sqlite3_int64 * id)
{
/* checks if a Vector Style do actually exists - by name */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int count = 0;
    sqlite3_int64 xid;

    sql = "SELECT style_id FROM SE_vector_styles "
	"WHERE Lower(style_name) = Lower(?)";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Vector Style by Name: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, style_name, strlen (style_name), SQLITE_STATIC);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		xid = sqlite3_column_int64 (stmt, 0);
		count++;
	    }
      }
    sqlite3_finalize (stmt);
    if (count == 1)
      {
	  *id = xid;
	  return 1;
      }
    return 0;
  stop:
    return 0;
}

static int
check_vector_style_refs_by_id (sqlite3 * sqlite, int style_id, int *has_refs)
{
/* checks if a Vector Style do actually exists - by ID */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int count = 0;
    int ref_count = 0;

    sql = "SELECT s.style_id, l.style_id FROM SE_vector_styles AS s "
	"LEFT JOIN SE_vector_styled_layers AS l ON (l.style_id = s.style_id) "
	"WHERE s.style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Vector Style Refs by ID: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int (stmt, 1, style_id);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		count++;
		if (sqlite3_column_type (stmt, 1) == SQLITE_INTEGER)
		    ref_count++;
	    }
      }
    sqlite3_finalize (stmt);
    if (count == 1)
      {
	  if (ref_count > 0)
	      *has_refs = 1;
	  return 1;
      }
    return 0;
  stop:
    return 0;
}

static int
check_vector_style_refs_by_name (sqlite3 * sqlite, const char *style_name,
				 sqlite3_int64 * id, int *has_refs)
{
/* checks if a Vector Style do actually exists - by name */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int count = 0;
    int ref_count = 0;
    sqlite3_int64 xid;

    sql = "SELECT style_id FROM SE_vector_styles "
	"WHERE Lower(style_name) = Lower(?)";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Vector Style Refs by Name: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, style_name, strlen (style_name), SQLITE_STATIC);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		xid = sqlite3_column_int64 (stmt, 0);
		count++;
	    }
      }
    sqlite3_finalize (stmt);
    if (count < 1)
	return 0;
    *id = xid;
    sql = "SELECT s.style_id, l.style_id FROM SE_vector_styles AS s "
	"LEFT JOIN SE_vector_styled_layers AS l ON (l.style_id = s.style_id) "
	"WHERE s.style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Vector Style Refs by ID: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int (stmt, 1, *id);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		count++;
		if (sqlite3_column_type (stmt, 1) == SQLITE_INTEGER)
		    ref_count++;
	    }
      }
    sqlite3_finalize (stmt);
    if (count == 1)
      {
	  if (ref_count > 0)
	      *has_refs = 1;
	  return 1;
      }
    return 0;
  stop:
    return 0;
}

static int
do_insert_vector_style_layer (sqlite3 * sqlite, const char *f_table_name,
			      const char *f_geometry_column, sqlite3_int64 id)
{
/* auxiliary function: really inserting a Vector Styled Layer */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int retval = 0;
    sql = "INSERT INTO SE_vector_styled_layers "
	"(f_table_name, f_geometry_column, style_id) VALUES (?, ?, ?)";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("registerVectorStyledLayer: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, f_table_name, strlen (f_table_name),
		       SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, f_geometry_column, strlen (f_geometry_column),
		       SQLITE_STATIC);
    sqlite3_bind_int64 (stmt, 3, id);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	retval = 1;
    else
	spatialite_e ("registerVectorStyledLayer() error: \"%s\"\n",
		      sqlite3_errmsg (sqlite));
    sqlite3_finalize (stmt);
    return retval;
  stop:
    return 0;
}

static int
do_delete_vector_style_refs (sqlite3 * sqlite, sqlite3_int64 id)
{
/* auxiliary function: deleting all Vector Style references */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int retval = 0;
    sql = "DELETE FROM SE_vector_styled_layers WHERE style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("unregisterVectorStyle: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int64 (stmt, 1, id);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	retval = 1;
    else
	spatialite_e ("unregisterVectorStyle() error: \"%s\"\n",
		      sqlite3_errmsg (sqlite));
    sqlite3_finalize (stmt);
    return retval;
  stop:
    return 0;
}

static int
do_delete_vector_style (sqlite3 * sqlite, sqlite3_int64 id)
{
/* auxiliary function: really deleting a Vector Style */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int retval = 0;
    sql = "DELETE FROM SE_vector_style WHERE style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("unregisterVectorStyle: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int64 (stmt, 1, id);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	retval = 1;
    else
	spatialite_e ("unregisterVectorStyle() error: \"%s\"\n",
		      sqlite3_errmsg (sqlite));
    sqlite3_finalize (stmt);
    return retval;
  stop:
    return 0;
}

SPATIALITE_PRIVATE int
unregister_vector_style (void *p_sqlite, int style_id,
			 const char *style_name, int remove_all)
{
/* auxiliary function: deletes a Vector Style definition */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    sqlite3_int64 id;
    int has_refs = 0;

    if (style_id >= 0)
      {
	  /* checking if the Vector Style do actually exists */
	  if (check_vector_style_refs_by_id (sqlite, style_id, &has_refs))
	      id = style_id;
	  else
	      return 0;
	  if (has_refs)
	    {
		if (!remove_all)
		    return 0;
		/* deleting all references */
		if (!do_delete_vector_style_refs (sqlite, id))
		    return 0;
	    }
	  /* deleting the Vector Style */
	  return do_delete_vector_style (sqlite, id);
      }
    else if (style_name != NULL)
      {
	  /* checking if the Vector Style do actually exists */
	  if (!check_vector_style_refs_by_name
	      (sqlite, style_name, &id, &has_refs))
	      return 0;
	  if (has_refs)
	    {
		if (!remove_all)
		    return 0;
		/* deleting all references */
		if (!do_delete_vector_style_refs (sqlite, id))
		    return 0;
	    }
	  /* deleting the Vector Style */
	  return do_delete_vector_style (sqlite, id);
      }
    else
	return 0;
}

static int
do_reload_vector_style (sqlite3 * sqlite, sqlite3_int64 id,
			const unsigned char *p_blob, int n_bytes)
{
/* auxiliary function: reloads a Vector Style definition */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;

    if (p_blob != NULL && n_bytes > 0)
      {
	  /* attempting to update the Vector Style */
	  sql = "UPDATE SE_vector_styles SET style = ? " "WHERE style_id = ?";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("reloadVectorStyle: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		return 0;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_blob (stmt, 1, p_blob, n_bytes, SQLITE_STATIC);
	  sqlite3_bind_int64 (stmt, 2, id);
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		spatialite_e ("reloadVectorStyle() error: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		sqlite3_finalize (stmt);
		return 0;
	    }
	  sqlite3_finalize (stmt);
	  return 1;
      }
    else
	return 0;
}

SPATIALITE_PRIVATE int
reload_vector_style (void *p_sqlite, int style_id,
		     const char *style_name,
		     const unsigned char *p_blob, int n_bytes)
{
/* auxiliary function: reloads a Vector Style definition */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    sqlite3_int64 id;

    if (style_id >= 0)
      {
	  /* checking if the Vector Style do actually exists */
	  if (check_vector_style_by_id (sqlite, style_id))
	      id = style_id;
	  else
	      return 0;
	  /* reloading the Vector Style */
	  return do_reload_vector_style (sqlite, id, p_blob, n_bytes);
      }
    else if (style_name != NULL)
      {
	  /* checking if the Vector Style do actually exists */
	  if (!check_vector_style_by_name (sqlite, style_name, &id))
	      return 0;
	  /* reloading the Vector Style */
	  return do_reload_vector_style (sqlite, id, p_blob, n_bytes);
      }
    else
	return 0;
}

SPATIALITE_PRIVATE int
register_vector_styled_layer_ex (void *p_sqlite, const char *f_table_name,
				 const char *f_geometry_column, int style_id,
				 const char *style_name,
				 const unsigned char *p_blob, int n_bytes)
{
/* auxiliary function: inserts a Vector Styled Layer definition */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    sqlite3_int64 id;

    if (f_table_name == NULL || f_geometry_column == NULL)
	return 0;

    if (style_id >= 0)
      {
	  /* checking if the Vector Style do actually exists */
	  if (check_vector_style_by_id (sqlite, style_id))
	      id = style_id;
	  else
	      return 0;
	  /* inserting the Vector Styled Layer */
	  return do_insert_vector_style_layer (sqlite, f_table_name,
					       f_geometry_column, id);
      }
    else if (style_name != NULL)
      {
	  /* checking if the Vector Style do actually exists */
	  if (!check_vector_style_by_name (sqlite, style_name, &id))
	      return 0;
	  /* inserting the Vector Styled Layer */
	  return do_insert_vector_style_layer (sqlite, f_table_name,
					       f_geometry_column, id);
      }
    else if (p_blob != NULL && n_bytes > 0)
      {
	  /* attempting to insert the Vector Style */
	  sql = "INSERT INTO SE_vector_styles "
	      "(style_id, style) VALUES (NULL, ?)";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("registerVectorStyle: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		return 0;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_blob (stmt, 1, p_blob, n_bytes, SQLITE_STATIC);
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      id = sqlite3_last_insert_rowid (sqlite);
	  else
	    {
		spatialite_e ("registerVectorStyle() error: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		sqlite3_finalize (stmt);
		return 0;
	    }
	  sqlite3_finalize (stmt);
	  stmt = NULL;
	  /* inserting the Vector Styled Layer */
	  return do_insert_vector_style_layer (sqlite, f_table_name,
					       f_geometry_column, id);
      }
    else
	return 0;
}

SPATIALITE_PRIVATE int
register_vector_styled_layer (void *p_sqlite, const char *f_table_name,
			      const char *f_geometry_column, int style_id,
			      const unsigned char *p_blob, int n_bytes)
{
/* auxiliary function: inserts a Vector Styled Layer definition */
    return register_vector_styled_layer_ex (p_sqlite, f_table_name,
					    f_geometry_column, style_id, NULL,
					    p_blob, n_bytes);
}

static int
check_vector_styled_layer_by_id (sqlite3 * sqlite, const char *f_table_name,
				 const char *f_geometry_column, int style_id)
{
/* checks if a Vector Styled Layer do actually exists - by ID */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int count = 0;

    sql = "SELECT style_id FROM SE_vector_styled_layers "
	"WHERE Lower(f_table_name) = Lower(?) AND "
	"Lower(f_geometry_column) = Lower(?) AND style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Vector Styled Layer by ID: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, f_table_name, strlen (f_table_name),
		       SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, f_geometry_column, strlen (f_geometry_column),
		       SQLITE_STATIC);
    sqlite3_bind_int64 (stmt, 3, style_id);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	      count++;
      }
    sqlite3_finalize (stmt);
    if (count == 1)
	return 1;
    return 0;
  stop:
    return 0;
}

static int
check_vector_styled_layer_by_name (sqlite3 * sqlite, const char *f_table_name,
				   const char *f_geometry_column,
				   const char *style_name, sqlite3_int64 * id)
{
/* checks if a Vector Styled Layer do actually exists - by name */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int count = 0;
    sqlite3_int64 xid;

    sql = "SELECT l.style_id FROM SE_vector_styled_layers AS l "
	"JOIN SE_vector_styles AS s ON (l.style_id = s.style_id) "
	"WHERE Lower(l.f_geometry_column) = Lower(?) AND "
	"Lower(l.f_geometry_column) = Lower(?) AND Lower(s,style_name) = Lower(?)";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Vector Styled Layer by Name: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, f_table_name, strlen (f_table_name),
		       SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, f_geometry_column, strlen (f_geometry_column),
		       SQLITE_STATIC);
    sqlite3_bind_text (stmt, 3, style_name, strlen (style_name), SQLITE_STATIC);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		xid = sqlite3_column_int64 (stmt, 0);
		count++;
	    }
      }
    sqlite3_finalize (stmt);
    if (count == 1)
      {
	  *id = xid;
	  return 1;
      }
    return 0;
  stop:
    return 0;
}

static int
do_delete_vector_style_layer (sqlite3 * sqlite, const char *f_table_name,
			      const char *f_geometry_column, sqlite3_int64 id)
{
/* auxiliary function: really deleting a Vector Styled Layer */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int retval = 0;
    sql = "DELETE FROM SE_vector_styled_layers "
	"WHERE Lower(f_table_name) = Lower(?) AND "
	"Lower(f_geometry_column) = Lower(?) AND style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("unregisterVectorStyledLayer: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, f_table_name, strlen (f_table_name),
		       SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, f_geometry_column, strlen (f_geometry_column),
		       SQLITE_STATIC);
    sqlite3_bind_int64 (stmt, 3, id);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	retval = 1;
    else
	spatialite_e ("unregisterVectorStyledLayer() error: \"%s\"\n",
		      sqlite3_errmsg (sqlite));
    sqlite3_finalize (stmt);
    return retval;
  stop:
    return 0;
}

SPATIALITE_PRIVATE int
unregister_vector_styled_layer (void *p_sqlite, const char *f_table_name,
				const char *f_geometry_column, int style_id,
				const char *style_name)
{
/* auxiliary function: removes a Vector Styled Layer definition */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    sqlite3_int64 id;

    if (f_table_name == NULL || f_geometry_column == NULL)
	return 0;

    if (style_id >= 0)
      {
	  /* checking if the Vector Styled Layer do actually exists */
	  if (check_vector_styled_layer_by_id (sqlite, f_table_name,
					       f_geometry_column, style_id))
	      id = style_id;
	  else
	      return 0;
	  /* removing the Vector Styled Layer */
	  return do_delete_vector_style_layer (sqlite, f_table_name,
					       f_geometry_column, id);
      }
    else if (style_name != NULL)
      {
	  /* checking if the Vector Styled Layer do actually exists */
	  if (!check_vector_styled_layer_by_name (sqlite, f_table_name,
						  f_geometry_column, style_name,
						  &id))
	      return 0;
	  /* removing the Vector Styled Layer */
	  return do_delete_vector_style_layer (sqlite, f_table_name,
					       f_geometry_column, id);
      }
    else
	return 0;
}

SPATIALITE_PRIVATE int
register_raster_style (void *p_sqlite, const unsigned char *p_blob, int n_bytes)
{
/* auxiliary function: inserts a Raster Style definition */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;

    if (p_blob != NULL && n_bytes > 0)
      {
	  /* attempting to insert the Raster Style */
	  sql = "INSERT INTO SE_raster_styles "
	      "(style_id, style) VALUES (NULL, ?)";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("registerRasterStyle: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		return 0;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_blob (stmt, 1, p_blob, n_bytes, SQLITE_STATIC);
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		spatialite_e ("registerRasterStyle() error: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		sqlite3_finalize (stmt);
		return 0;
	    }
	  sqlite3_finalize (stmt);
	  return 1;
      }
    else
	return 0;
}

static int
do_delete_raster_style_refs (sqlite3 * sqlite, sqlite3_int64 id)
{
/* auxiliary function: deleting all Raster Style references */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int retval = 0;
    sql = "DELETE FROM SE_raster_styled_layers WHERE style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("unregisterRasterStyle: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int64 (stmt, 1, id);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	retval = 1;
    else
	spatialite_e ("unregisterRasterStyle() error: \"%s\"\n",
		      sqlite3_errmsg (sqlite));
    sqlite3_finalize (stmt);
    return retval;
  stop:
    return 0;
}

static int
check_raster_style_refs_by_id (sqlite3 * sqlite, int style_id, int *has_refs)
{
/* checks if a Raster Style do actually exists - by ID */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int count = 0;
    int ref_count = 0;

    sql = "SELECT s.style_id, l.style_id FROM SE_raster_styles AS s "
	"LEFT JOIN SE_raster_styled_layers AS l ON (l.style_id = s.style_id) "
	"WHERE s.style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Raster Style Refs by ID: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int (stmt, 1, style_id);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		count++;
		if (sqlite3_column_type (stmt, 1) == SQLITE_INTEGER)
		    ref_count++;
	    }
      }
    sqlite3_finalize (stmt);
    if (count == 1)
      {
	  if (ref_count > 0)
	      *has_refs = 1;
	  return 1;
      }
    return 0;
  stop:
    return 0;
}

static int
check_raster_style_refs_by_name (sqlite3 * sqlite, const char *style_name,
				 sqlite3_int64 * id, int *has_refs)
{
/* checks if a Raster Style do actually exists - by name */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int count = 0;
    int ref_count = 0;
    sqlite3_int64 xid;

    sql = "SELECT style_id FROM SE_raster_styles "
	"WHERE Lower(style_name) = Lower(?)";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Raster Style Refs by Name: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, style_name, strlen (style_name), SQLITE_STATIC);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		xid = sqlite3_column_int64 (stmt, 0);
		count++;
	    }
      }
    sqlite3_finalize (stmt);
    if (count < 1)
	return 0;
    *id = xid;
    sql = "SELECT s.style_id, l.style_id FROM SE_raster_styles AS s "
	"LEFT JOIN SE_raster_styled_layers AS l ON (l.style_id = s.style_id) "
	"WHERE s.style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Raster Style Refs by ID: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int (stmt, 1, *id);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		count++;
		if (sqlite3_column_type (stmt, 1) == SQLITE_INTEGER)
		    ref_count++;
	    }
      }
    sqlite3_finalize (stmt);
    if (count == 1)
      {
	  if (ref_count > 0)
	      *has_refs = 1;
	  return 1;
      }
    return 0;
  stop:
    return 0;
}

static int
do_delete_raster_style (sqlite3 * sqlite, sqlite3_int64 id)
{
/* auxiliary function: really deleting a Raster Style */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int retval = 0;
    sql = "DELETE FROM SE_raster_style WHERE style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("unregisterRasterStyle: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int64 (stmt, 1, id);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	retval = 1;
    else
	spatialite_e ("unregisterRasterStyle() error: \"%s\"\n",
		      sqlite3_errmsg (sqlite));
    sqlite3_finalize (stmt);
    return retval;
  stop:
    return 0;
}

SPATIALITE_PRIVATE int
unregister_raster_style (void *p_sqlite, int style_id,
			 const char *style_name, int remove_all)
{
/* auxiliary function: deletes a Raster Style definition */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    sqlite3_int64 id;
    int has_refs = 0;

    if (style_id >= 0)
      {
	  /* checking if the Raster Style do actually exists */
	  if (check_raster_style_refs_by_id (sqlite, style_id, &has_refs))
	      id = style_id;
	  else
	      return 0;
	  if (has_refs)
	    {
		if (!remove_all)
		    return 0;
		/* deleting all references */
		if (!do_delete_raster_style_refs (sqlite, id))
		    return 0;
	    }
	  /* deleting the Raster Style */
	  return do_delete_raster_style (sqlite, id);
      }
    else if (style_name != NULL)
      {
	  /* checking if the Raster Style do actually exists */
	  if (!check_raster_style_refs_by_name
	      (sqlite, style_name, &id, &has_refs))
	      return 0;
	  if (has_refs)
	    {
		if (!remove_all)
		    return 0;
		/* deleting all references */
		if (!do_delete_raster_style_refs (sqlite, id))
		    return 0;
	    }
	  /* deleting the Raster Style */
	  return do_delete_raster_style (sqlite, id);
      }
    else
	return 0;
}

static int
check_raster_style_by_id (sqlite3 * sqlite, int style_id)
{
/* checks if a Raster Style do actually exists - by ID */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int count = 0;

    sql = "SELECT style_id FROM SE_raster_styles " "WHERE style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Raster Style by ID: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int (stmt, 1, style_id);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	      count++;
      }
    sqlite3_finalize (stmt);
    if (count == 1)
	return 1;
    return 0;
  stop:
    return 0;
}

static int
check_raster_style_by_name (sqlite3 * sqlite, const char *style_name,
			    sqlite3_int64 * id)
{
/* checks if a Raster Style do actually exists - by name */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int count = 0;
    sqlite3_int64 xid;

    sql = "SELECT style_id FROM SE_raster_styles "
	"WHERE Lower(style_name) = Lower(?)";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Raster Style by Name: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, style_name, strlen (style_name), SQLITE_STATIC);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		xid = sqlite3_column_int64 (stmt, 0);
		count++;
	    }
      }
    sqlite3_finalize (stmt);
    if (count == 1)
      {
	  *id = xid;
	  return 1;
      }
    return 0;
  stop:
    return 0;
}

static int
do_reload_raster_style (sqlite3 * sqlite, sqlite3_int64 id,
			const unsigned char *p_blob, int n_bytes)
{
/* auxiliary function: reloads a Raster Style definition */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;

    if (p_blob != NULL && n_bytes > 0)
      {
	  /* attempting to update the Raster Style */
	  sql = "UPDATE SE_raster_styles SET style = ? " "WHERE style_id = ?";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("reloadRasterStyle: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		return 0;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_blob (stmt, 1, p_blob, n_bytes, SQLITE_STATIC);
	  sqlite3_bind_int64 (stmt, 2, id);
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		spatialite_e ("reloadRasterStyle() error: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		sqlite3_finalize (stmt);
		return 0;
	    }
	  sqlite3_finalize (stmt);
	  return 1;
      }
    else
	return 0;
}

SPATIALITE_PRIVATE int
reload_raster_style (void *p_sqlite, int style_id,
		     const char *style_name,
		     const unsigned char *p_blob, int n_bytes)
{
/* auxiliary function: reloads a Raster Style definition */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    sqlite3_int64 id;

    if (style_id >= 0)
      {
	  /* checking if the Raster Style do actually exists */
	  if (check_raster_style_by_id (sqlite, style_id))
	      id = style_id;
	  else
	      return 0;
	  /* reloading the Raster Style */
	  return do_reload_raster_style (sqlite, id, p_blob, n_bytes);
      }
    else if (style_name != NULL)
      {
	  /* checking if the Raster Style do actually exists */
	  if (!check_raster_style_by_name (sqlite, style_name, &id))
	      return 0;
	  /* reloading the Raster Style */
	  return do_reload_raster_style (sqlite, id, p_blob, n_bytes);
      }
    else
	return 0;
}

static int
do_insert_raster_style_layer (sqlite3 * sqlite, const char *coverage_name,
			      sqlite3_int64 id)
{
/* auxiliary function: really inserting a Raster Styled Layer */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int retval = 0;
    sql = "INSERT INTO SE_raster_styled_layers "
	"(coverage_name, style_id) VALUES (?, ?)";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("registerRasterStyledLayer: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, coverage_name, strlen (coverage_name),
		       SQLITE_STATIC);
    sqlite3_bind_int64 (stmt, 2, id);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	retval = 1;
    else
	spatialite_e ("registerRasterStyledLayer() error: \"%s\"\n",
		      sqlite3_errmsg (sqlite));
    sqlite3_finalize (stmt);
    return retval;
  stop:
    return 0;
}

SPATIALITE_PRIVATE int
register_raster_styled_layer_ex (void *p_sqlite, const char *coverage_name,
				 int style_id, const char *style_name,
				 const unsigned char *p_blob, int n_bytes)
{
/* auxiliary function: inserts a Raster Styled Layer definition */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    sqlite3_int64 id;

    if (coverage_name == NULL)
	return 0;

    if (style_id >= 0)
      {
	  /* checking if the Raster Style do actually exists */
	  if (check_raster_style_by_id (sqlite, style_id))
	      id = style_id;
	  else
	      return 0;
	  /* inserting the Raster Styled Layer */
	  return do_insert_raster_style_layer (sqlite, coverage_name, id);
      }
    else if (style_name != NULL)
      {
	  /* checking if the Raster Style do actually exists */
	  if (!check_raster_style_by_name (sqlite, style_name, &id))
	      return 0;
	  /* inserting the Raster Styled Layer */
	  return do_insert_raster_style_layer (sqlite, coverage_name, id);
      }
    else if (p_blob != NULL && n_bytes > 0)
      {
	  /* attempting to insert the Raster Style */
	  sql = "INSERT INTO SE_raster_styles "
	      "(style_id, style) VALUES (NULL, ?)";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("registerRasterStyle: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		return 0;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_blob (stmt, 1, p_blob, n_bytes, SQLITE_STATIC);
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      id = sqlite3_last_insert_rowid (sqlite);
	  else
	    {
		spatialite_e ("registerRasterStyle() error: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		sqlite3_finalize (stmt);
		return 0;
	    }
	  sqlite3_finalize (stmt);
	  stmt = NULL;
	  /* inserting the Raster Styled Layer */
	  return do_insert_raster_style_layer (sqlite, coverage_name, id);
      }
    else
	return 0;
}

SPATIALITE_PRIVATE int
register_raster_styled_layer (void *p_sqlite, const char *coverage_name,
			      int style_id, const unsigned char *p_blob,
			      int n_bytes)
{
/* auxiliary function: inserts a Raster Styled Layer definition */
    return register_raster_styled_layer_ex (p_sqlite, coverage_name, style_id,
					    NULL, p_blob, n_bytes);
}

static int
check_raster_styled_layer_by_id (sqlite3 * sqlite, const char *coverage_name,
				 int style_id)
{
/* checks if a Raster Styled Layer do actually exists - by ID */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int count = 0;

    sql = "SELECT style_id FROM SE_raster_styled_layers "
	"WHERE Lower(coverage_name) = Lower(?) AND style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Raster Styled Layer by ID: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, coverage_name, strlen (coverage_name),
		       SQLITE_STATIC);
    sqlite3_bind_int64 (stmt, 2, style_id);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	      count++;
      }
    sqlite3_finalize (stmt);
    if (count == 1)
	return 1;
    return 0;
  stop:
    return 0;
}

static int
check_raster_styled_layer_by_name (sqlite3 * sqlite, const char *coverage_name,
				   const char *style_name, sqlite3_int64 * id)
{
/* checks if a Raster Styled Layer do actually exists - by name */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int count = 0;
    sqlite3_int64 xid;

    sql = "SELECT l.style_id FROM SE_raster_styled_layers AS l "
	"JOIN SE_raster_styles AS s ON (l.style_id = s.style_id) "
	"WHERE Lower(l.coverage_name) = Lower(?) AND "
	"Lower(s,style_name) = Lower(?)";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("check Raster Styled Layer by Name: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, coverage_name, strlen (coverage_name),
		       SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, style_name, strlen (style_name), SQLITE_STATIC);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		xid = sqlite3_column_int64 (stmt, 0);
		count++;
	    }
      }
    sqlite3_finalize (stmt);
    if (count == 1)
      {
	  *id = xid;
	  return 1;
      }
    return 0;
  stop:
    return 0;
}

static int
do_delete_raster_style_layer (sqlite3 * sqlite, const char *coverage_name,
			      sqlite3_int64 id)
{
/* auxiliary function: really deleting a Raster Styled Layer */
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int retval = 0;
    sql = "DELETE FROM SE_raster_styled_layers "
	"WHERE Lower(coverage_name) = Lower(?) AND " "style_id = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("unregisterRasterStyledLayer: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, coverage_name, strlen (coverage_name),
		       SQLITE_STATIC);
    sqlite3_bind_int64 (stmt, 2, id);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	retval = 1;
    else
	spatialite_e ("unregisterRasterStyledLayer() error: \"%s\"\n",
		      sqlite3_errmsg (sqlite));
    sqlite3_finalize (stmt);
    return retval;
  stop:
    return 0;
}

SPATIALITE_PRIVATE int
unregister_raster_styled_layer (void *p_sqlite, const char *coverage_name,
				int style_id, const char *style_name)
{
/* auxiliary function: removes a Raster Styled Layer definition */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    sqlite3_int64 id;

    if (coverage_name == NULL)
	return 0;

    if (style_id >= 0)
      {
	  /* checking if the Raster Styled Layer do actually exists */
	  if (check_raster_styled_layer_by_id (sqlite, coverage_name, style_id))
	      id = style_id;
	  else
	      return 0;
	  /* removing the Raster Styled Layer */
	  return do_delete_raster_style_layer (sqlite, coverage_name, id);
      }
    else if (style_name != NULL)
      {
	  /* checking if the Raster Styled Layer do actually exists */
	  if (!check_raster_styled_layer_by_name
	      (sqlite, coverage_name, style_name, &id))
	      return 0;
	  /* removing the Raster Styled Layer */
	  return do_delete_raster_style_layer (sqlite, coverage_name, id);
      }
    else
	return 0;
}

SPATIALITE_PRIVATE int
register_styled_group (void *p_sqlite, const char *group_name,
		       const char *f_table_name,
		       const char *f_geometry_column,
		       const char *coverage_name, int paint_order)
{
/* auxiliary function: inserts or updates a Styled Group Item */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int exists_group = 0;
    int exists = 0;
    int retval = 0;
    sqlite3_int64 id;

    /* checking if the Group already exists */
    sql = "SELECT group_name FROM SE_styled_groups "
	"WHERE group_name = Lower(?)";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("registerStyledGroupsRefs: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, group_name, strlen (group_name), SQLITE_STATIC);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	      exists_group = 1;
      }
    sqlite3_finalize (stmt);

    if (!exists_group)
      {
	  /* insert group */
	  sql = "INSERT INTO SE_styled_groups (group_name) VALUES (?)";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("registerStyledGroupsRefs: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		goto stop;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_text (stmt, 1, group_name, strlen (group_name),
			     SQLITE_STATIC);
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      retval = 1;
	  else
	      spatialite_e ("registerStyledGroupsRefs() error: \"%s\"\n",
			    sqlite3_errmsg (sqlite));
	  sqlite3_finalize (stmt);
	  if (retval == 0)
	      goto stop;
	  retval = 0;
      }

    if (paint_order >= 0)
      {
	  /* checking if the group-item already exists */
	  sql = "SELECT id FROM SE_styled_group_refs "
	      "WHERE group_name = Lower(?) AND paint_order = ?";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("registerStyledGroupsRefs: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		goto stop;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_text (stmt, 1, group_name, strlen (group_name),
			     SQLITE_STATIC);
	  sqlite3_bind_int (stmt, 2, paint_order);
	  while (1)
	    {
		/* scrolling the result set rows */
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE)
		    break;	/* end of result set */
		if (ret == SQLITE_ROW)
		  {
		      id = sqlite3_column_int64 (stmt, 0);
		      exists++;
		  }
	    }
	  sqlite3_finalize (stmt);
	  if (exists != 1)
	      goto stop;
      }

    if (paint_order < 0)
      {
	  /* assigning the next paint_order value */
	  paint_order = 0;
	  sql = "SELECT Max(paint_order) FROM SE_styled_group_refs "
	      "WHERE group_name = Lower(?) ";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("registerStyledGroupsRefs: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		goto stop;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_text (stmt, 1, group_name, strlen (group_name),
			     SQLITE_STATIC);
	  while (1)
	    {
		/* scrolling the result set rows */
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE)
		    break;	/* end of result set */
		if (ret == SQLITE_ROW)
		  {
		      if (sqlite3_column_type (stmt, 0) == SQLITE_INTEGER)
			  paint_order = sqlite3_column_int (stmt, 0) + 1;
		  }
	    }
	  sqlite3_finalize (stmt);
      }

    if (exists)
      {
	  /* update */
	  sql = "UPDATE SE_styled_group_refs SET paint_order = ? "
	      "WHERE id = ?";
      }
    else
      {
	  /* insert */
	  if (coverage_name == NULL)
	    {
		/* vector styled layer */
		sql = "INSERT INTO SE_styled_group_refs "
		    "(id, group_name, f_table_name, f_geometry_column, paint_order) "
		    "VALUES (NULL, ?, ?, ?, ?)";
	    }
	  else
	    {
		/* raster styled layer */
		sql = "INSERT INTO SE_styled_group_refs "
		    "(id, group_name, coverage_name, paint_order) "
		    "VALUES (NULL, ?, ?, ?)";
	    }
      }
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("registerStyledGroupsRefs: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    if (exists)
      {
	  /* update */
	  sqlite3_bind_int (stmt, 1, paint_order);
	  sqlite3_bind_int64 (stmt, 2, id);
      }
    else
      {
	  /* insert */
	  sqlite3_bind_text (stmt, 1, group_name, strlen (group_name),
			     SQLITE_STATIC);
	  if (coverage_name == NULL)
	    {
		/* vector styled layer */
		sqlite3_bind_text (stmt, 2, f_table_name,
				   strlen (f_table_name), SQLITE_STATIC);
		sqlite3_bind_text (stmt, 3, f_geometry_column,
				   strlen (f_geometry_column), SQLITE_STATIC);
		sqlite3_bind_int (stmt, 4, paint_order);
	    }
	  else
	    {
		/* raster styled layer */
		sqlite3_bind_text (stmt, 2, coverage_name,
				   strlen (coverage_name), SQLITE_STATIC);
		sqlite3_bind_int (stmt, 3, paint_order);
	    }
      }
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	retval = 1;
    else
	spatialite_e ("registerStyledGroupsRefs() error: \"%s\"\n",
		      sqlite3_errmsg (sqlite));
    sqlite3_finalize (stmt);
    return retval;
  stop:
    return 0;
}

SPATIALITE_PRIVATE int
styled_group_set_infos (void *p_sqlite, const char *group_name,
			const char *title, const char *abstract)
{
/* auxiliary function: inserts or updates the Styled Group descriptive infos */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int exists = 0;
    int retval = 0;

    /* checking if the Group already exists */
    sql = "SELECT group_name FROM SE_styled_groups "
	"WHERE group_name = Lower(?)";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("styledGroupSetInfos: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, group_name, strlen (group_name), SQLITE_STATIC);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	      exists = 1;
      }
    sqlite3_finalize (stmt);

    if (!exists)
      {
	  /* insert group */
	  sql =
	      "INSERT INTO SE_styled_groups (group_name, title, abstract) VALUES (?, ?, ?)";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("styledGroupSetInfos: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		goto stop;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_text (stmt, 1, group_name, strlen (group_name),
			     SQLITE_STATIC);
	  sqlite3_bind_text (stmt, 2, title, strlen (title), SQLITE_STATIC);
	  sqlite3_bind_text (stmt, 3, abstract, strlen (abstract),
			     SQLITE_STATIC);
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      retval = 1;
	  else
	      spatialite_e ("styledGroupSetInfos() error: \"%s\"\n",
			    sqlite3_errmsg (sqlite));
	  sqlite3_finalize (stmt);
      }
    else
      {
	  /* update group */
	  sql =
	      "UPDATE SE_styled_groups SET title = ?, abstract = ? WHERE group_name = ?";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("styledGroupSetInfos: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		goto stop;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_text (stmt, 1, title, strlen (title), SQLITE_STATIC);
	  sqlite3_bind_text (stmt, 2, abstract, strlen (abstract),
			     SQLITE_STATIC);
	  sqlite3_bind_text (stmt, 3, group_name, strlen (group_name),
			     SQLITE_STATIC);
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      retval = 1;
	  else
	      spatialite_e ("styledGroupSetInfos() error: \"%s\"\n",
			    sqlite3_errmsg (sqlite));
	  sqlite3_finalize (stmt);
      }
    return retval;
  stop:
    return 0;
}

SPATIALITE_PRIVATE int
register_group_style (void *p_sqlite, const char *group_name, int style_id,
		      const unsigned char *p_blob, int n_bytes)
{
/* auxiliary function: inserts or updates a Group Style */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int exists = 0;
    int retval = 0;

    if (style_id >= 0)
      {
	  /* checking if already exists */
	  sql = "SELECT style_id FROM SE_group_styles "
	      "WHERE group_name = Lower(?) AND style_id = ?";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("registerGroupStyle: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		goto stop;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_text (stmt, 1, group_name, strlen (group_name),
			     SQLITE_STATIC);
	  sqlite3_bind_int (stmt, 2, style_id);
	  while (1)
	    {
		/* scrolling the result set rows */
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE)
		    break;	/* end of result set */
		if (ret == SQLITE_ROW)
		    exists = 1;
	    }
	  sqlite3_finalize (stmt);
      }
    else
      {
	  /* assigning the next style_id value */
	  style_id = 0;
	  sql = "SELECT Max(style_id) FROM SE_group_styles "
	      "WHERE group_name = Lower(?) ";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("registerGroupStyle: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		goto stop;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_text (stmt, 1, group_name, strlen (group_name),
			     SQLITE_STATIC);
	  while (1)
	    {
		/* scrolling the result set rows */
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE)
		    break;	/* end of result set */
		if (ret == SQLITE_ROW)
		  {
		      if (sqlite3_column_type (stmt, 0) == SQLITE_INTEGER)
			  style_id = sqlite3_column_int (stmt, 0) + 1;
		  }
	    }
	  sqlite3_finalize (stmt);
      }

    if (exists)
      {
	  /* update */
	  sql = "UPDATE SE_group_styles SET style = ? "
	      "WHERE group_name = Lower(?) AND style_id = ?";
      }
    else
      {
	  /* insert */
	  sql = "INSERT INTO SE_group_styles "
	      "(group_name, style_id, style) VALUES (?, ?, ?)";
      }
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("registerGroupStyle: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    if (exists)
      {
	  /* update */
	  sqlite3_bind_blob (stmt, 1, p_blob, n_bytes, SQLITE_STATIC);
	  sqlite3_bind_text (stmt, 2, group_name, strlen (group_name),
			     SQLITE_STATIC);
	  sqlite3_bind_int (stmt, 3, style_id);
      }
    else
      {
	  /* insert */
	  sqlite3_bind_text (stmt, 1, group_name, strlen (group_name),
			     SQLITE_STATIC);
	  sqlite3_bind_int (stmt, 2, style_id);
	  sqlite3_bind_blob (stmt, 3, p_blob, n_bytes, SQLITE_STATIC);
      }
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	retval = 1;
    else
	spatialite_e ("registerGroupStyled() error: \"%s\"\n",
		      sqlite3_errmsg (sqlite));
    sqlite3_finalize (stmt);
    return retval;
  stop:
    return 0;
}

SPATIALITE_PRIVATE int
get_iso_metadata_id (void *p_sqlite, const char *fileIdentifier, void *p_id)
{
/* auxiliary function: return the ID of the row corresponding to "fileIdentifier" */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    sqlite3_int64 *p64 = (sqlite3_int64 *) p_id;
    sqlite3_int64 id;
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int ok = 0;

    sql = "SELECT id FROM ISO_metadata WHERE fileId = ?";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("getIsoMetadataId: \"%s\"\n", sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, fileIdentifier, strlen (fileIdentifier),
		       SQLITE_STATIC);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		ok++;
		id = sqlite3_column_int64 (stmt, 0);
	    }
      }
    sqlite3_finalize (stmt);

    if (ok == 1)
      {
	  *p64 = id;
	  return 1;
      }
  stop:
    return 0;
}

SPATIALITE_PRIVATE int
register_iso_metadata (void *p_sqlite, const char *scope,
		       const unsigned char *p_blob, int n_bytes, void *p_id,
		       const char *fileIdentifier)
{
/* auxiliary function: inserts or updates an ISO Metadata */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    sqlite3_int64 *p64 = (sqlite3_int64 *) p_id;
    sqlite3_int64 id = *p64;
    int ret;
    const char *sql;
    sqlite3_stmt *stmt;
    int exists = 0;
    int retval = 0;

    if (id >= 0)
      {
	  /* checking if already exists - by ID */
	  sql = "SELECT id FROM ISO_metadata WHERE id = ?";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("registerIsoMetadata: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		goto stop;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_int64 (stmt, 1, id);
	  while (1)
	    {
		/* scrolling the result set rows */
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE)
		    break;	/* end of result set */
		if (ret == SQLITE_ROW)
		    exists = 1;
	    }
	  sqlite3_finalize (stmt);
      }
    if (fileIdentifier != NULL)
      {
	  /* checking if already exists - by fileIdentifier */
	  sql = "SELECT id FROM ISO_metadata WHERE fileId = ?";
	  ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("registerIsoMetadata: \"%s\"\n",
			      sqlite3_errmsg (sqlite));
		goto stop;
	    }
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_text (stmt, 1, fileIdentifier, strlen (fileIdentifier),
			     SQLITE_STATIC);
	  while (1)
	    {
		/* scrolling the result set rows */
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE)
		    break;	/* end of result set */
		if (ret == SQLITE_ROW)
		  {
		      exists = 1;
		      id = sqlite3_column_int64 (stmt, 0);
		  }
	    }
	  sqlite3_finalize (stmt);
      }

    if (exists)
      {
	  /* update */
	  sql = "UPDATE ISO_metadata SET md_scope = ?, metadata = ? "
	      "WHERE id = ?";
      }
    else
      {
	  /* insert */
	  sql = "INSERT INTO ISO_metadata "
	      "(id, md_scope, metadata) VALUES (?, ?, ?)";
      }
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("registerIsoMetadata: \"%s\"\n",
			sqlite3_errmsg (sqlite));
	  goto stop;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    if (exists)
      {
	  /* update */
	  sqlite3_bind_text (stmt, 1, scope, strlen (scope), SQLITE_STATIC);
	  sqlite3_bind_blob (stmt, 2, p_blob, n_bytes, SQLITE_STATIC);
	  sqlite3_bind_int (stmt, 3, id);
      }
    else
      {
	  /* insert */
	  if (id < 0)
	      sqlite3_bind_null (stmt, 1);
	  else
	      sqlite3_bind_int64 (stmt, 1, id);
	  sqlite3_bind_text (stmt, 2, scope, strlen (scope), SQLITE_STATIC);
	  sqlite3_bind_blob (stmt, 3, p_blob, n_bytes, SQLITE_STATIC);
      }
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	retval = 1;
    else
	spatialite_e ("registerIsoMetadata() error: \"%s\"\n",
		      sqlite3_errmsg (sqlite));
    sqlite3_finalize (stmt);
    return retval;
  stop:
    return 0;
}

#endif /* end including LIBXML2 */
