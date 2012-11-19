/*

 shapefiles.c -- implements shapefile support [import - export]

 version 4.0, 2012 August 6

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
 
Portions created by the Initial Developer are Copyright (C) 2008-2012
the Initial Developer. All Rights Reserved.

Contributor(s): Brad Hards <bradh@frogmouth.net>

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

#if defined(_WIN32) && !defined(__MINGW32__)
/* MSVC strictly requires this include [off_t] */
#include <sys/types.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#if defined(_WIN32) && !defined(__MINGW32__)
#include "config-msvc.h"
#else
#include "config.h"
#endif

#include <spatialite/sqlite.h>
#include <spatialite/debug.h>

#include <spatialite/gaiaaux.h>
#include <spatialite/gaiageo.h>
#include <spatialite.h>
#include <spatialite_private.h>

#ifndef OMIT_FREEXL
#include <freexl.h>
#endif

#if defined(_WIN32) && !defined(__MINGW32__)
#define strcasecmp	_stricmp
#define strncasecmp	_strnicmp
#endif

struct dupl_column
{
/* a column value in a duplicated row */
    int pos;
    char *name;
    int type;
    sqlite3_int64 int_value;
    double dbl_value;
    const char *txt_value;
    const void *blob;
    int size;
    int query_pos;
    struct dupl_column *next;
};

struct dupl_row
{
/* a duplicated row with column values */
    int count;
    struct dupl_column *first;
    struct dupl_column *last;
    const char *table;
};

static void
clean_dupl_row (struct dupl_row *str)
{
/* destroying a duplicated row struct */
    struct dupl_column *p;
    struct dupl_column *pn;
    p = str->first;
    while (p)
      {
	  pn = p->next;
	  free (p->name);
	  free (p);
	  p = pn;
      }
}

static void
add_to_dupl_row (struct dupl_row *str, const char *name)
{
/* adding a column to the duplicated row struct */
    int len;
    struct dupl_column *p = malloc (sizeof (struct dupl_column));
    p->pos = str->count;
    len = strlen (name);
    p->name = malloc (len + 1);
    strcpy (p->name, name);
    str->count++;
    p->type = SQLITE_NULL;
    p->next = NULL;
    if (str->first == NULL)
	str->first = p;
    if (str->last)
	str->last->next = p;
    str->last = p;
}

static void
set_int_value (struct dupl_row *str, int pos, sqlite3_int64 value)
{
/* setting up an integer value */
    struct dupl_column *p = str->first;
    while (p)
      {
	  if (p->pos == pos)
	    {
		p->type = SQLITE_INTEGER;
		p->int_value = value;
		return;
	    }
	  p = p->next;
      }
}

static void
set_double_value (struct dupl_row *str, int pos, double value)
{
/* setting up a double value */
    struct dupl_column *p = str->first;
    while (p)
      {
	  if (p->pos == pos)
	    {
		p->type = SQLITE_FLOAT;
		p->dbl_value = value;
		return;
	    }
	  p = p->next;
      }
}

static void
set_text_value (struct dupl_row *str, int pos, const char *value)
{
/* setting up a text value */
    struct dupl_column *p = str->first;
    while (p)
      {
	  if (p->pos == pos)
	    {
		p->type = SQLITE_TEXT;
		p->txt_value = value;
		return;
	    }
	  p = p->next;
      }
}

static void
set_blob_value (struct dupl_row *str, int pos, const void *blob, int size)
{
/* setting up a blob value */
    struct dupl_column *p = str->first;
    while (p)
      {
	  if (p->pos == pos)
	    {
		p->type = SQLITE_BLOB;
		p->blob = blob;
		p->size = size;
		return;
	    }
	  p = p->next;
      }
}

static void
set_null_value (struct dupl_row *str, int pos)
{
/* setting up a NULL value */
    struct dupl_column *p = str->first;
    while (p)
      {
	  if (p->pos == pos)
	    {
		p->type = SQLITE_NULL;
		return;
	    }
	  p = p->next;
      }
}

static void
reset_query_pos (struct dupl_row *str)
{
/* resetting QueryPos for BLOBs */
    struct dupl_column *p = str->first;
    while (p)
      {
	  p->query_pos = -1;
	  p = p->next;
      }
}

static int
check_dupl_blob2 (struct dupl_column *ptr, const void *blob, int size)
{
/* checking a BLOB value */
    if (ptr->type != SQLITE_BLOB)
	return 0;
    if (ptr->size != size)
	return 0;
    if (memcmp (ptr->blob, blob, size) != 0)
	return 0;
    return 1;
}

static int
check_dupl_blob (struct dupl_row *str, int pos, const void *blob, int size)
{
/* checking a BLOB value */
    struct dupl_column *p = str->first;
    while (p)
      {
	  if (p->query_pos == pos)
	    {
		return check_dupl_blob2 (p, blob, size);
	    }
	  p = p->next;
      }
    return 0;
}

static gaiaDbfFieldPtr
getDbfField (gaiaDbfListPtr list, char *name)
{
/* find a DBF attribute by name */
    gaiaDbfFieldPtr fld = list->First;
    while (fld)
      {
	  if (strcasecmp (fld->Name, name) == 0)
	      return fld;
	  fld = fld->Next;
      }
    return NULL;
}

#ifndef OMIT_ICONV		/* ICONV enabled: supporting SHP */

SPATIALITE_DECLARE int
load_shapefile (sqlite3 * sqlite, char *shp_path, char *table, char *charset,
		int srid, char *column, int coerce2d, int compressed,
		int verbose, int spatial_index, int *rows, char *err_msg)
{
    return load_shapefile_ex (sqlite, shp_path, table, charset, srid, column,
			      NULL, NULL, coerce2d, compressed, verbose,
			      spatial_index, rows, err_msg);
}

SPATIALITE_DECLARE int
load_shapefile_ex (sqlite3 * sqlite, char *shp_path, char *table, char *charset,
		   int srid, char *g_column, char *gtype, char *pk_column,
		   int coerce2d, int compressed, int verbose, int spatial_index,
		   int *rows, char *err_msg)
{
    sqlite3_stmt *stmt = NULL;
    int ret;
    char *errMsg = NULL;
    char *sql;
    char *dummy;
    int already_exists = 0;
    int metadata = 0;
    int sqlError = 0;
    gaiaShapefilePtr shp = NULL;
    gaiaDbfFieldPtr dbf_field;
    int cnt;
    int col_cnt;
    int seed;
    int len;
    int dup;
    int idup;
    int current_row;
    char **col_name = NULL;
    unsigned char *blob;
    int blob_size;
    char *geom_type;
    char *txt_dims;
    char *geo_column = g_column;
    char *xgtype = gtype;
    char *qtable = NULL;
    char *qpk_name = NULL;
    char *pk_name = NULL;
    int pk_autoincr = 1;
    char *xname;
    int pk_type = SQLITE_INTEGER;
    int pk_set;
    gaiaOutBuffer sql_statement;
    if (!geo_column)
	geo_column = "Geometry";
    if (!xgtype)
	;
    else
      {
	  if (strcasecmp (xgtype, "LINESTRING") == 0)
	      xgtype = "LINESTRING";
	  else if (strcasecmp (xgtype, "LINESTRINGZ") == 0)
	      xgtype = "LINESTRINGZ";
	  else if (strcasecmp (xgtype, "LINESTRINGM") == 0)
	      xgtype = "LINESTRINGM";
	  else if (strcasecmp (xgtype, "LINESTRINGZM") == 0)
	      xgtype = "LINESTRINGZM";
	  else if (strcasecmp (xgtype, "MULTILINESTRING") == 0)
	      xgtype = "MULTILINESTRING";
	  else if (strcasecmp (xgtype, "MULTILINESTRINGZ") == 0)
	      xgtype = "MULTILINESTRINGZ";
	  else if (strcasecmp (xgtype, "MULTILINESTRINGM") == 0)
	      xgtype = "MULTILINESTRINGM";
	  else if (strcasecmp (xgtype, "MULTILINESTRINGZM") == 0)
	      xgtype = "MULTILINESTRINGZM";
	  else if (strcasecmp (xgtype, "POLYGON") == 0)
	      xgtype = "POLYGON";
	  else if (strcasecmp (xgtype, "POLYGONZ") == 0)
	      xgtype = "POLYGONZ";
	  else if (strcasecmp (xgtype, "POLYGONM") == 0)
	      xgtype = "POLYGONM";
	  else if (strcasecmp (xgtype, "POLYGONZM") == 0)
	      xgtype = "POLYGONZM";
	  else if (strcasecmp (xgtype, "MULTIPOLYGON") == 0)
	      xgtype = "MULTIPOLYGON";
	  else if (strcasecmp (xgtype, "MULTIPOLYGONZ") == 0)
	      xgtype = "MULTIPOLYGONZ";
	  else if (strcasecmp (xgtype, "MULTIPOLYGONM") == 0)
	      xgtype = "MULTIPOLYGONM";
	  else if (strcasecmp (xgtype, "MULTIPOLYGONZM") == 0)
	      xgtype = "MULTIPOLYGONZM";
	  else
	      xgtype = NULL;
      }
    qtable = gaiaDoubleQuotedSql (table);
/* checking if TABLE already exists */
    sql =
	sqlite3_mprintf ("SELECT name FROM sqlite_master WHERE type = 'table' "
			 "AND Lower(name) = Lower(%Q)", table);
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  if (!err_msg)
	      spatialite_e ("load shapefile error: <%s>\n",
			    sqlite3_errmsg (sqlite));
	  else
	      sprintf (err_msg, "load shapefile error: <%s>\n",
		       sqlite3_errmsg (sqlite));
	  goto clean_up;
      }
    while (1)
      {
	  /* scrolling the result set */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	      already_exists = 1;
	  else
	    {
		spatialite_e ("load shapefile error: <%s>\n",
			      sqlite3_errmsg (sqlite));
		break;
	    }
      }
    sqlite3_finalize (stmt);
    if (already_exists)
      {
	  if (!err_msg)
	      spatialite_e ("load shapefile error: table '%s' already exists\n",
			    table);
	  else
	      sprintf (err_msg,
		       "load shapefile error: table '%s' already exists\n",
		       table);
	  if (qtable)
	      free (qtable);
	  if (qpk_name)
	      free (qpk_name);
	  return 0;
      }
/* checking if MetaData GEOMETRY_COLUMNS exists */
    sql =
	"SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'geometry_columns'";
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  if (!err_msg)
	      spatialite_e ("load shapefile error: <%s>\n",
			    sqlite3_errmsg (sqlite));
	  else
	      sprintf (err_msg, "load shapefile error: <%s>\n",
		       sqlite3_errmsg (sqlite));
	  if (qtable)
	      free (qtable);
	  if (qpk_name)
	      free (qpk_name);
	  return 0;
      }
    while (1)
      {
	  /* scrolling the result set */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	      metadata = 1;
	  else
	    {
		spatialite_e ("load shapefile error: <%s>\n",
			      sqlite3_errmsg (sqlite));
		break;
	    }
      }
    sqlite3_finalize (stmt);
    shp = gaiaAllocShapefile ();
    gaiaOpenShpRead (shp, shp_path, charset, "UTF-8");
    if (!(shp->Valid))
      {
	  if (!err_msg)
	    {
		spatialite_e
		    ("load shapefile error: cannot open shapefile '%s'\n",
		     shp_path);
		if (shp->LastError)
		    spatialite_e ("\tcause: %s\n", shp->LastError);
	    }
	  else
	    {
		char extra[512];
		*extra = '\0';
		if (shp->LastError)
		    sprintf (extra, "\n\tcause: %s\n", shp->LastError);
		sprintf (err_msg,
			 "load shapefile error: cannot open shapefile '%s'%s",
			 shp_path, extra);
	    }
	  gaiaFreeShapefile (shp);
	  if (qtable)
	      free (qtable);
	  if (qpk_name)
	      free (qpk_name);
	  return 0;
      }
/* checking for duplicate / illegal column names and antialising them */
    col_cnt = 0;
    dbf_field = shp->Dbf->First;
    while (dbf_field)
      {
	  /* counting DBF fields */
	  col_cnt++;
	  dbf_field = dbf_field->Next;
      }
    col_name = malloc (sizeof (char *) * col_cnt);
    cnt = 0;
    seed = 0;
    if (pk_column != NULL)
      {
	  /* validating the Primary Key column */
	  dbf_field = shp->Dbf->First;
	  while (dbf_field)
	    {
		if (strcasecmp (pk_column, dbf_field->Name) == 0)
		  {
		      /* ok, using this field as Primary Key */
		      pk_name = pk_column;
		      pk_autoincr = 0;
		      switch (dbf_field->Type)
			{
			case 'C':
			    pk_type = SQLITE_TEXT;
			    break;
			case 'N':
			    if (dbf_field->Decimals)
				pk_type = SQLITE_FLOAT;
			    else
			      {
				  if (dbf_field->Length <= 18)
				      pk_type = SQLITE_INTEGER;
				  else
				      pk_type = SQLITE_FLOAT;
			      }
			    break;
			case 'D':
			    pk_type = SQLITE_FLOAT;
			    break;
			case 'F':
			    pk_type = SQLITE_FLOAT;
			    break;
			case 'L':
			    pk_type = SQLITE_INTEGER;
			    break;
			};
		  }
		dbf_field = dbf_field->Next;
	    }
      }
    if (pk_name == NULL)
      {
	  if (pk_column != NULL)
	      pk_name = pk_column;
	  else
	      pk_name = "PK_UID";
      }
    qpk_name = gaiaDoubleQuotedSql (pk_name);
    dbf_field = shp->Dbf->First;
    while (dbf_field)
      {
	  /* preparing column names */
	  char *xdummy = NULL;
	  if (strcasecmp (pk_name, dbf_field->Name) == 0)
	    {
		/* skipping the Primary Key field */
		dummy = dbf_field->Name;
		len = strlen (dummy);
		*(col_name + cnt) = malloc (len + 1);
		strcpy (*(col_name + cnt), dummy);
		cnt++;
		dbf_field = dbf_field->Next;
		continue;
	    }
	  dummy = dbf_field->Name;
	  dup = 0;
	  for (idup = 0; idup < cnt; idup++)
	    {
		if (strcasecmp (dummy, *(col_name + idup)) == 0)
		    dup = 1;
	    }
	  if (strcasecmp (dummy, pk_name) == 0)
	      dup = 1;
	  if (strcasecmp (dummy, geo_column) == 0)
	      dup = 1;
	  if (dup)
	    {
		xdummy = sqlite3_mprintf ("COL_%d", seed++);
		dummy = xdummy;
	    }
	  len = strlen (dummy);
	  *(col_name + cnt) = malloc (len + 1);
	  strcpy (*(col_name + cnt), dummy);
	  if (xdummy)
	      sqlite3_free (xdummy);
	  cnt++;
	  dbf_field = dbf_field->Next;
      }
    if (verbose)
	spatialite_e
	    ("========\nLoading shapefile at '%s' into SQLite table '%s'\n",
	     shp_path, table);
/* starting a transaction */
    if (verbose)
	spatialite_e ("\nBEGIN;\n");
    ret = sqlite3_exec (sqlite, "BEGIN", NULL, 0, &errMsg);
    if (ret != SQLITE_OK)
      {
	  if (!err_msg)
	      spatialite_e ("load shapefile error: <%s>\n", errMsg);
	  else
	      sprintf (err_msg, "load shapefile error: <%s>\n", errMsg);
	  sqlite3_free (errMsg);
	  sqlError = 1;
	  goto clean_up;
      }
/* creating the Table */
    gaiaOutBufferInitialize (&sql_statement);
    if (pk_type == SQLITE_TEXT)
      {
	  sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n\"%s\" "
				 "TEXT PRIMARY KEY NOT NULL", qtable, qpk_name);
      }
    else if (pk_type == SQLITE_FLOAT)
      {
	  sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n\"%s\" "
				 "DOUBLE PRIMARY KEY NOT NULL", qtable,
				 qpk_name);
      }
    else
      {
	  if (pk_autoincr)
	      sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n\"%s\" "
				     "INTEGER PRIMARY KEY AUTOINCREMENT",
				     qtable, qpk_name);
	  else
	      sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n\"%s\" "
				     "INTEGER NOT NULL PRIMARY KEY", qtable,
				     qpk_name);
      }
    gaiaAppendToOutBuffer (&sql_statement, sql);
    sqlite3_free (sql);
    cnt = 0;
    dbf_field = shp->Dbf->First;
    while (dbf_field)
      {
	  if (strcasecmp (pk_name, dbf_field->Name) == 0)
	    {
		/* skipping the Primary Key field */
		dbf_field = dbf_field->Next;
		cnt++;
		continue;
	    }
	  xname = gaiaDoubleQuotedSql (*(col_name + cnt));
	  sql = sqlite3_mprintf (",\n\"%s\"", xname);
	  free (xname);
	  gaiaAppendToOutBuffer (&sql_statement, sql);
	  sqlite3_free (sql);
	  cnt++;
	  switch (dbf_field->Type)
	    {
	    case 'C':
		gaiaAppendToOutBuffer (&sql_statement, " TEXT");
		break;
	    case 'N':
		if (dbf_field->Decimals)
		    gaiaAppendToOutBuffer (&sql_statement, " DOUBLE");
		else
		  {
		      if (dbf_field->Length <= 18)
			  gaiaAppendToOutBuffer (&sql_statement, " INTEGER");
		      else
			  gaiaAppendToOutBuffer (&sql_statement, " DOUBLE");
		  }
		break;
	    case 'D':
		gaiaAppendToOutBuffer (&sql_statement, " DOUBLE");
		break;
	    case 'F':
		gaiaAppendToOutBuffer (&sql_statement, " DOUBLE");
		break;
	    case 'L':
		gaiaAppendToOutBuffer (&sql_statement, " INTEGER");
		break;
	    };
	  dbf_field = dbf_field->Next;
      }
    if (metadata)
	gaiaAppendToOutBuffer (&sql_statement, ")");
    else
      {
	  xname = gaiaDoubleQuotedSql (geo_column);
	  sql = sqlite3_mprintf (",\n\"%s\" BLOB)", xname);
	  free (xname);
	  gaiaAppendToOutBuffer (&sql_statement, sql);
	  sqlite3_free (sql);
      }
    if (sql_statement.Error == 0 && sql_statement.Buffer != NULL)
      {
	  if (verbose)
	      spatialite_e ("%s;\n", sql_statement.Buffer);
	  ret = sqlite3_exec (sqlite, sql_statement.Buffer, NULL, 0, &errMsg);
      }
    else
	ret = SQLITE_ERROR;
    gaiaOutBufferReset (&sql_statement);
    if (ret != SQLITE_OK)
      {
	  if (!err_msg)
	      spatialite_e ("load shapefile error: <%s>\n", errMsg);
	  else
	      sprintf (err_msg, "load shapefile error: <%s>\n", errMsg);
	  sqlite3_free (errMsg);
	  sqlError = 1;
	  goto clean_up;
      }
    if (metadata)
      {
	  /* creating Geometry column */
	  switch (shp->Shape)
	    {
	    case GAIA_SHP_POINT:
	    case GAIA_SHP_POINTM:
	    case GAIA_SHP_POINTZ:
		geom_type = "POINT";
		break;
	    case GAIA_SHP_MULTIPOINT:
	    case GAIA_SHP_MULTIPOINTM:
	    case GAIA_SHP_MULTIPOINTZ:
		geom_type = "MULTIPOINT";
		break;
	    case GAIA_SHP_POLYLINE:
	    case GAIA_SHP_POLYLINEM:
	    case GAIA_SHP_POLYLINEZ:
		if (xgtype == NULL)
		  {
		      /* auto-decting if MULTILINESTRING is required */
		      gaiaShpAnalyze (shp);
		      if (shp->EffectiveType == GAIA_LINESTRING)
			  geom_type = "LINESTRING";
		      else
			  geom_type = "MULTILINESTRING";
		  }
		else
		  {
		      /* user-defined geometry type */
		      if (strcmp (xgtype, "LINESTRING") == 0)
			{
			    geom_type = "LINESTRING";
			    shp->EffectiveType = GAIA_LINESTRING;
			    shp->EffectiveDims = GAIA_XY;
			}
		      if (strcmp (xgtype, "LINESTRINGZ") == 0)
			{
			    geom_type = "LINESTRING";
			    shp->EffectiveType = GAIA_LINESTRING;
			    shp->EffectiveDims = GAIA_XY_Z;
			}
		      if (strcmp (xgtype, "LINESTRINGM") == 0)
			{
			    geom_type = "LINESTRING";
			    shp->EffectiveType = GAIA_LINESTRING;
			    shp->EffectiveDims = GAIA_XY_M;
			}
		      if (strcmp (xgtype, "LINESTRINGZM") == 0)
			{
			    geom_type = "LINESTRING";
			    shp->EffectiveType = GAIA_LINESTRING;
			    shp->EffectiveDims = GAIA_XY_Z_M;
			}
		      if (strcmp (xgtype, "MULTILINESTRING") == 0)
			{
			    geom_type = "MULTILINESTRING";
			    shp->EffectiveType = GAIA_MULTILINESTRING;
			    shp->EffectiveDims = GAIA_XY;
			}
		      if (strcmp (xgtype, "MULTILINESTRINGZ") == 0)
			{
			    geom_type = "MULTILINESTRING";
			    shp->EffectiveType = GAIA_MULTILINESTRING;
			    shp->EffectiveDims = GAIA_XY_Z;
			}
		      if (strcmp (xgtype, "MULTILINESTRINGM") == 0)
			{
			    geom_type = "MULTILINESTRING";
			    shp->EffectiveType = GAIA_MULTILINESTRING;
			    shp->EffectiveDims = GAIA_XY_M;
			}
		      if (strcmp (xgtype, "MULTILINESTRINGZM") == 0)
			{
			    geom_type = "MULTILINESTRING";
			    shp->EffectiveType = GAIA_MULTILINESTRING;
			    shp->EffectiveDims = GAIA_XY_Z_M;
			}
		  }
		break;
	    case GAIA_SHP_POLYGON:
	    case GAIA_SHP_POLYGONM:
	    case GAIA_SHP_POLYGONZ:
		if (xgtype == NULL)
		  {
		      /* auto-decting if MULTIPOLYGON is required */
		      gaiaShpAnalyze (shp);
		      if (shp->EffectiveType == GAIA_POLYGON)
			  geom_type = "POLYGON";
		      else
			  geom_type = "MULTIPOLYGON";
		  }
		else
		  {
		      /* user-defined geometry type */
		      if (strcmp (xgtype, "POLYGON") == 0)
			{
			    geom_type = "POLYGON";
			    shp->EffectiveType = GAIA_POLYGON;
			    shp->EffectiveDims = GAIA_XY;
			}
		      if (strcmp (xgtype, "POLYGONZ") == 0)
			{
			    geom_type = "POLYGON";
			    shp->EffectiveType = GAIA_POLYGON;
			    shp->EffectiveDims = GAIA_XY_Z;
			}
		      if (strcmp (xgtype, "POLYGONM") == 0)
			{
			    geom_type = "POLYGON";
			    shp->EffectiveType = GAIA_POLYGON;
			    shp->EffectiveDims = GAIA_XY_M;
			}
		      if (strcmp (xgtype, "POLYGONZM") == 0)
			{
			    geom_type = "POLYGON";
			    shp->EffectiveType = GAIA_POLYGON;
			    shp->EffectiveDims = GAIA_XY_Z_M;
			}
		      if (strcmp (xgtype, "MULTIPOLYGON") == 0)
			{
			    geom_type = "MULTIPOLYGON";
			    shp->EffectiveType = GAIA_MULTIPOLYGON;
			    shp->EffectiveDims = GAIA_XY;
			}
		      if (strcmp (xgtype, "MULTIPOLYGONZ") == 0)
			{
			    geom_type = "MULTIPOLYGON";
			    shp->EffectiveType = GAIA_MULTIPOLYGON;
			    shp->EffectiveDims = GAIA_XY_Z;
			}
		      if (strcmp (xgtype, "MULTIPOLYGONM") == 0)
			{
			    geom_type = "MULTIPOLYGON";
			    shp->EffectiveType = GAIA_MULTIPOLYGON;
			    shp->EffectiveDims = GAIA_XY_M;
			}
		      if (strcmp (xgtype, "MULTIPOLYGONZM") == 0)
			{
			    geom_type = "MULTIPOLYGON";
			    shp->EffectiveType = GAIA_MULTIPOLYGON;
			    shp->EffectiveDims = GAIA_XY_Z_M;
			}
		  }
		break;
	    };
	  if (coerce2d)
	      shp->EffectiveDims = GAIA_XY;
	  switch (shp->EffectiveDims)
	    {
	    case GAIA_XY_Z:
		txt_dims = "XYZ";
		break;
	    case GAIA_XY_M:
		txt_dims = "XYM";
		break;
	    case GAIA_XY_Z_M:
		txt_dims = "XYZM";
		break;
	    default:
		txt_dims = "XY";
		break;
	    };
	  sql = sqlite3_mprintf ("SELECT AddGeometryColumn(%Q, %Q, %d, %Q, %Q)",
				 table, geo_column, srid, geom_type, txt_dims);
	  if (verbose)
	      spatialite_e ("%s;\n", sql);
	  ret = sqlite3_exec (sqlite, sql, NULL, 0, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (!err_msg)
		    spatialite_e ("load shapefile error: <%s>\n", errMsg);
		else
		    sprintf (err_msg, "load shapefile error: <%s>\n", errMsg);
		sqlite3_free (errMsg);
		sqlError = 1;
		goto clean_up;
	    }
	  if (spatial_index)
	    {
		/* creating the Spatial Index */
		sql = sqlite3_mprintf ("SELECT CreateSpatialIndex(%Q, %Q)",
				       table, geo_column);
		ret = sqlite3_exec (sqlite, sql, NULL, 0, &errMsg);
		sqlite3_free (sql);
		if (ret != SQLITE_OK)
		  {
		      if (!err_msg)
			  spatialite_e ("load shapefile error: <%s>\n", errMsg);
		      else
			  sprintf (err_msg, "load shapefile error: <%s>\n",
				   errMsg);
		      sqlite3_free (errMsg);
		      sqlError = 1;
		      goto clean_up;
		  }
	    }
      }
    else
      {
	  /* no Metadata */
	  if (shp->Shape == GAIA_SHP_POLYLINE
	      || shp->Shape == GAIA_SHP_POLYLINEM
	      || shp->Shape == GAIA_SHP_POLYLINEZ
	      || shp->Shape == GAIA_SHP_POLYGON
	      || shp->Shape == GAIA_SHP_POLYGONM
	      || shp->Shape == GAIA_SHP_POLYGONZ)
	    {
		/* 
		   / fixing anyway the Geometry type for 
		   / LINESTRING/MULTILINESTRING or 
		   / POLYGON/MULTIPOLYGON
		 */
		gaiaShpAnalyze (shp);
	    }
      }
    /* preparing the INSERT INTO parametrerized statement */
    gaiaOutBufferInitialize (&sql_statement);
    sql = sqlite3_mprintf ("INSERT INTO \"%s\" (\"%s\",", qtable, qpk_name);
    gaiaAppendToOutBuffer (&sql_statement, sql);
    sqlite3_free (sql);
    cnt = 0;
    dbf_field = shp->Dbf->First;
    while (dbf_field)
      {
	  /* columns corresponding to some DBF attribute */
	  if (strcasecmp (pk_name, dbf_field->Name) == 0)
	    {
		/* skipping the Primary Key field */
		dbf_field = dbf_field->Next;
		cnt++;
		continue;
	    }
	  xname = gaiaDoubleQuotedSql (*(col_name + cnt++));
	  sql = sqlite3_mprintf ("\"%s\" ,", xname);
	  free (xname);
	  gaiaAppendToOutBuffer (&sql_statement, sql);
	  sqlite3_free (sql);
	  dbf_field = dbf_field->Next;
      }
    xname = gaiaDoubleQuotedSql (geo_column);	/* the GEOMETRY column */
    sql = sqlite3_mprintf ("\"%s\")\n VALUES (?", xname);
    free (xname);
    gaiaAppendToOutBuffer (&sql_statement, sql);
    sqlite3_free (sql);
    dbf_field = shp->Dbf->First;
    while (dbf_field)
      {
	  /* column values */
	  if (strcasecmp (pk_name, dbf_field->Name) == 0)
	    {
		/* skipping the Primary Key field */
		dbf_field = dbf_field->Next;
		continue;
	    }
	  gaiaAppendToOutBuffer (&sql_statement, ", ?");
	  dbf_field = dbf_field->Next;
      }
    gaiaAppendToOutBuffer (&sql_statement, ", ?)");	/* the GEOMETRY column */
    if (sql_statement.Error == 0 && sql_statement.Buffer != NULL)
	ret =
	    sqlite3_prepare_v2 (sqlite, sql_statement.Buffer,
				strlen (sql_statement.Buffer), &stmt, NULL);
    else
	ret = SQLITE_ERROR;
    gaiaOutBufferReset (&sql_statement);
    if (ret != SQLITE_OK)
      {
	  if (!err_msg)
	      spatialite_e ("load shapefile error: <%s>\n",
			    sqlite3_errmsg (sqlite));
	  else
	      sprintf (err_msg, "load shapefile error: <%s>\n",
		       sqlite3_errmsg (sqlite));
	  sqlError = 1;
	  goto clean_up;
      }
    current_row = 0;
    while (1)
      {
	  /* inserting rows from shapefile */
	  ret = gaiaReadShpEntity (shp, current_row, srid);
	  if (!ret)
	    {
		if (!(shp->LastError))	/* normal SHP EOF */
		    break;
		if (!err_msg)
		    spatialite_e ("%s\n", shp->LastError);
		else
		    sprintf (err_msg, "%s\n", shp->LastError);
		sqlError = 1;
		sqlite3_finalize (stmt);
		goto clean_up;
	    }
	  current_row++;
	  /* binding query params */
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  pk_set = 0;
	  cnt = 0;
	  dbf_field = shp->Dbf->First;
	  while (dbf_field)
	    {
		/* Primary Key value */
		if (strcasecmp (pk_name, dbf_field->Name) == 0)
		  {
		      if (pk_type == SQLITE_TEXT)
			  sqlite3_bind_text (stmt, 1,
					     dbf_field->Value->TxtValue,
					     strlen (dbf_field->
						     Value->TxtValue),
					     SQLITE_STATIC);
		      else if (pk_type == SQLITE_FLOAT)
			  sqlite3_bind_double (stmt, 1,
					       dbf_field->Value->DblValue);
		      else
			  sqlite3_bind_int64 (stmt, 1,
					      dbf_field->Value->IntValue);
		      pk_set = 1;
		  }
		dbf_field = dbf_field->Next;
	    }
	  if (!pk_set)
	      sqlite3_bind_int (stmt, 1, current_row);
	  cnt = 0;
	  dbf_field = shp->Dbf->First;
	  while (dbf_field)
	    {
		/* column values */
		if (strcasecmp (pk_name, dbf_field->Name) == 0)
		  {
		      /* skipping the Primary Key field */
		      dbf_field = dbf_field->Next;
		      continue;
		  }
		if (!(dbf_field->Value))
		    sqlite3_bind_null (stmt, cnt + 2);
		else
		  {
		      switch (dbf_field->Value->Type)
			{
			case GAIA_INT_VALUE:
			    sqlite3_bind_int64 (stmt, cnt + 2,
						dbf_field->Value->IntValue);
			    break;
			case GAIA_DOUBLE_VALUE:
			    sqlite3_bind_double (stmt, cnt + 2,
						 dbf_field->Value->DblValue);
			    break;
			case GAIA_TEXT_VALUE:
			    sqlite3_bind_text (stmt, cnt + 2,
					       dbf_field->Value->TxtValue,
					       strlen (dbf_field->Value->
						       TxtValue),
					       SQLITE_STATIC);
			    break;
			default:
			    sqlite3_bind_null (stmt, cnt + 2);
			    break;
			}
		  }
		cnt++;
		dbf_field = dbf_field->Next;
	    }
	  if (shp->Dbf->Geometry)
	    {
		if (compressed)
		    gaiaToCompressedBlobWkb (shp->Dbf->Geometry, &blob,
					     &blob_size);
		else
		    gaiaToSpatiaLiteBlobWkb (shp->Dbf->Geometry, &blob,
					     &blob_size);
		sqlite3_bind_blob (stmt, cnt + 2, blob, blob_size, free);
	    }
	  else
	    {
		/* handling a NULL-Geometry */
		sqlite3_bind_null (stmt, cnt + 2);
	    }
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		if (!err_msg)
		    spatialite_e ("load shapefile error: <%s>\n",
				  sqlite3_errmsg (sqlite));
		else
		    sprintf (err_msg, "load shapefile error: <%s>\n",
			     sqlite3_errmsg (sqlite));
		sqlite3_finalize (stmt);
		sqlError = 1;
		goto clean_up;
	    }
      }
    sqlite3_finalize (stmt);
  clean_up:
    if (qtable)
	free (qtable);
    if (qpk_name)
	free (qpk_name);
    gaiaFreeShapefile (shp);
    if (col_name)
      {
	  /* releasing memory allocation for column names */
	  for (cnt = 0; cnt < col_cnt; cnt++)
	      free (*(col_name + cnt));
	  free (col_name);
      }
    if (sqlError)
      {
	  /* some error occurred - ROLLBACK */
	  if (verbose)
	      spatialite_e ("ROLLBACK;\n");
	  ret = sqlite3_exec (sqlite, "ROLLBACK", NULL, 0, &errMsg);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("load shapefile error: <%s>\n", errMsg);
		sqlite3_free (errMsg);
	    }
	  if (rows)
	      *rows = current_row;
	  return 0;
      }
    else
      {
	  /* ok - confirming pending transaction - COMMIT */
	  if (verbose)
	      spatialite_e ("COMMIT;\n");
	  ret = sqlite3_exec (sqlite, "COMMIT", NULL, 0, &errMsg);
	  if (ret != SQLITE_OK)
	    {
		if (!err_msg)
		    spatialite_e ("load shapefile error: <%s>\n", errMsg);
		else
		    sprintf (err_msg, "load shapefile error: <%s>\n", errMsg);
		sqlite3_free (errMsg);
		return 0;
	    }
	  if (rows)
	      *rows = current_row;
	  if (verbose)
	      spatialite_e
		  ("\nInserted %d rows into '%s' from SHAPEFILE\n========\n",
		   current_row, table);
	  if (err_msg)
	      sprintf (err_msg, "Inserted %d rows into '%s' from SHAPEFILE",
		       current_row, table);
	  return 1;
      }
}

#endif /* end ICONV (SHP) */

static void
output_prj_file (sqlite3 * sqlite, char *path, char *table, char *column)
{
/* exporting [if possible] a .PRJ file */
    char **results;
    int rows;
    int columns;
    int i;
    char *errMsg = NULL;
    int srid = -1;
    char *sql;
    int ret;
    int rs_srid = 0;
    int rs_srs_wkt = 0;
    int rs_srtext = 0;
    int has_srtext = 0;
    const char *name;
    char *srsWkt = NULL;
    FILE *out;

/* step I: retrieving the SRID */
    sql = sqlite3_mprintf ("SELECT srid FROM geometry_columns WHERE "
			   "f_table_name = %Q AND f_geometry_column = %Q",
			   table, column);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("dump shapefile MetaData error: <%s>\n", errMsg);
	  sqlite3_free (errMsg);
	  return;
      }
    for (i = 1; i <= rows; i++)
      {
	  srid = atoi (results[(i * columns) + 0]);
      }
    sqlite3_free_table (results);
    if (srid <= 0)
      {
	  /* srid still undefined, so we'll read VIEWS_GEOMETRY_COLUMNS */
	  sql = sqlite3_mprintf ("SELECT srid FROM views_geometry_columns "
				 "JOIN geometry_columns USING (f_table_name, f_geometry_column) "
				 "WHERE view_name = %Q AND view_geometry = %Q",
				 table, column);
	  ret =
	      sqlite3_get_table (sqlite, sql, &results, &rows, &columns,
				 &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("dump shapefile MetaData error: <%s>\n", errMsg);
		sqlite3_free (errMsg);
		return;
	    }
	  for (i = 1; i <= rows; i++)
	    {
		srid = atoi (results[(i * columns) + 0]);
	    }
	  sqlite3_free_table (results);
      }
    if (srid <= 0)
	return;

/* step II: checking if the SRS_WKT or SRTEXT column actually exists */
    ret =
	sqlite3_get_table (sqlite, "PRAGMA table_info(spatial_ref_sys)",
			   &results, &rows, &columns, &errMsg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("dump shapefile MetaData error: <%s>\n", errMsg);
	  sqlite3_free (errMsg);
	  return;
      }
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		name = results[(i * columns) + 1];
		if (strcasecmp (name, "srid") == 0)
		    rs_srid = 1;
		if (strcasecmp (name, "srs_wkt") == 0)
		    rs_srs_wkt = 1;
		if (strcasecmp (name, "srtext") == 0)
		    rs_srtext = 1;
	    }
      }
    sqlite3_free_table (results);
    if (rs_srs_wkt == 1 || rs_srtext == 1)
	has_srtext = 1;
    if (rs_srid == 0 || has_srtext == 0)
	return;

/* step III: fetching WKT SRS */
    if (rs_srtext)
      {
	  sql = sqlite3_mprintf ("SELECT srtext FROM spatial_ref_sys "
				 "WHERE srid = %d AND srtext IS NOT NULL",
				 srid);
      }
    else
      {
	  sql = sqlite3_mprintf ("SELECT srs_wkt FROM spatial_ref_sys "
				 "WHERE srid = %d AND srs_wkt IS NOT NULL",
				 srid);
      }
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("dump shapefile MetaData error: <%s>\n", errMsg);
	  sqlite3_free (errMsg);
	  goto end;
      }
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		char *srs = results[(i * columns) + 0];
		int len = strlen (srs);
		if (srsWkt)
		    free (srsWkt);
		srsWkt = malloc (len + 1);
		strcpy (srsWkt, srs);
	    }
      }
    sqlite3_free_table (results);
    if (srsWkt == NULL)
	goto end;

/* step IV: generating the .PRJ file */
    sql = sqlite3_mprintf ("%s.prj", path);
    out = fopen (sql, "wb");
    sqlite3_free (sql);
    if (!out)
	goto end;
    fprintf (out, "%s\r\n", srsWkt);
    fclose (out);
  end:
    if (srsWkt)
	free (srsWkt);
    return;
}

#ifndef OMIT_ICONV		/* ICONV enabled: supporting SHAPEFILE and DBF */


static int
get_default_dbf_fields (sqlite3 * sqlite, const char *xtable,
			gaiaDbfListPtr * dbf_export_list)
{
/* creating DBF field definitions for an empty DBF */
    int row = 0;
    char *sql;
    sqlite3_stmt *stmt;
    int ret;
    int offset = 0;
    gaiaDbfListPtr list = NULL;

    sql = sqlite3_mprintf ("PRAGMA table_info(\"%s\")", xtable);
/*
/ compiling SQL prepared statement 
*/

    list = gaiaAllocDbfList ();
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto sql_error;
    while (1)
      {
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* processing a result set row */
		int xtype = SQLITE_TEXT;
		int length = 60;
		char *name = (char *) sqlite3_column_text (stmt, 1);
		const char *type = (const char *) sqlite3_column_text (stmt, 2);

		if (strcasecmp (type, "INT") == 0
		    || strcasecmp (type, "INTEGER") == 0
		    || strcasecmp (type, "SMALLINT") == 0
		    || strcasecmp (type, "BIGINT") == 0
		    || strcasecmp (type, "TINYINT") == 0)
		    xtype = SQLITE_INTEGER;
		if (strcasecmp (type, "DOUBLE") == 0
		    || strcasecmp (type, "REAL") == 0
		    || strcasecmp (type, "DOUBLE PRECISION") == 0
		    || strcasecmp (type, "NUMERIC") == 0
		    || strcasecmp (type, "FLOAT") == 0)
		    xtype = SQLITE_FLOAT;
		if (strncasecmp (type, "VARCHAR(", 8) == 0)
		    length = atoi (type + 8);
		if (strncasecmp (type, "CHAR(", 5) == 0)
		    length = atoi (type + 5);

		if (xtype == SQLITE_FLOAT)
		  {
		      gaiaAddDbfField (list, name, 'N', offset, 19, 6);
		      offset += 19;
		  }
		else if (xtype == SQLITE_INTEGER)
		  {
		      gaiaAddDbfField (list, name, 'N', offset, 18, 0);
		      offset += 18;
		  }
		else
		  {
		      gaiaAddDbfField (list, name, 'C', offset, length, 0);
		      offset += length;
		  }
		row++;
	    }
	  else
	      goto sql_error;
      }
    sqlite3_finalize (stmt);
    if (row == 0)
	goto sql_error;
    *dbf_export_list = list;
    return 1;
  sql_error:
    gaiaFreeDbfList (list);
    *dbf_export_list = NULL;
    return 0;
}

static gaiaVectorLayersListPtr
recover_unregistered_geometry (sqlite3 * handle, const char *table,
			       const char *geometry)
{
/* attempting to recover an unregistered Geometry */
    int len;
    int error = 0;
    gaiaVectorLayersListPtr list;
    gaiaVectorLayerPtr lyr;

/* allocating a VectorLayersList */
    list = malloc (sizeof (gaiaVectorLayersList));
    list->First = NULL;
    list->Last = NULL;
    list->Current = NULL;

    lyr = malloc (sizeof (gaiaVectorLayer));
    lyr->LayerType = GAIA_VECTOR_UNKNOWN;
    len = strlen (table);
    lyr->TableName = malloc (len + 1);
    strcpy (lyr->TableName, table);
    len = strlen (geometry);
    lyr->GeometryName = malloc (len + 1);
    strcpy (lyr->GeometryName, geometry);
    lyr->Srid = 0;
    lyr->GeometryType = GAIA_VECTOR_UNKNOWN;
    lyr->Dimensions = GAIA_VECTOR_UNKNOWN;
    lyr->SpatialIndex = GAIA_SPATIAL_INDEX_NONE;
    lyr->ExtentInfos = NULL;
    lyr->AuthInfos = NULL;
    lyr->First = NULL;
    lyr->Last = NULL;
    lyr->Next = NULL;
    list->Current = NULL;
    if (list->First == NULL)
	list->First = lyr;
    if (list->Last != NULL)
	list->Last->Next = lyr;
    list->Last = lyr;

    if (!doComputeFieldInfos
	(handle, lyr->TableName, lyr->GeometryName,
	 SPATIALITE_STATISTICS_LEGACY, lyr))
	error = 1;

    if (list->First == NULL || error)
      {
	  gaiaFreeVectorLayersList (list);
	  return NULL;
      }
    return list;
}

static int
compute_max_int_length (sqlite3_int64 min, sqlite3_int64 max)
{
/* determining the buffer size for some INT */
    int pos_len = 0;
    int neg_len = 1;
    sqlite3_int64 value = max;
    while (value != 0)
      {
	  pos_len++;
	  value /= 10;
      }
    if (min >= 0)
	return pos_len;
    value = min;
    while (value != 0)
      {
	  neg_len++;
	  value /= 10;
      }
    if (neg_len > pos_len)
	return neg_len;
    return pos_len;
}

static int
compute_max_dbl_length (double min, double max)
{
/* determining the buffer size for some DOUBLE */
    int pos_len = 0;
    int neg_len = 1;
    sqlite3_int64 value;
    if (max >= 0.0)
	value = floor (max);
    else
	value = ceil (max);
    while (value != 0)
      {
	  pos_len++;
	  value /= 10;
      }
    if (min >= 0.0)
	return pos_len + 7;
    value = ceil (min);
    while (value != 0)
      {
	  neg_len++;
	  value /= 10;
      }
    if (neg_len > pos_len)
	return neg_len + 7;
    return pos_len + 7;
}

SPATIALITE_DECLARE int
dump_shapefile (sqlite3 * sqlite, char *table, char *column, char *shp_path,
		char *charset, char *geom_type, int verbose, int *xrows,
		char *err_msg)
{
/* SHAPEFILE dump */
    char *sql;
    char *dummy;
    int shape = -1;
    int len;
    int ret;
    sqlite3_stmt *stmt;
    int n_cols = 0;
    int offset = 0;
    int i;
    int rows = 0;
    char buf[256];
    char *xtable;
    char *xcolumn;
    const void *blob_value;
    gaiaShapefilePtr shp = NULL;
    gaiaDbfListPtr dbf_list;
    gaiaDbfListPtr dbf_write;
    gaiaDbfFieldPtr dbf_field;
    gaiaVectorLayerPtr lyr = NULL;
    gaiaLayerAttributeFieldPtr fld;
    gaiaVectorLayersListPtr list;

    if (geom_type)
      {
	  /* normalizing required geometry type */
	  if (strcasecmp ((char *) geom_type, "POINT") == 0)
	      shape = GAIA_POINT;
	  if (strcasecmp ((char *) geom_type, "LINESTRING") == 0)
	      shape = GAIA_LINESTRING;
	  if (strcasecmp ((char *) geom_type, "POLYGON") == 0)
	      shape = GAIA_POLYGON;
	  if (strcasecmp ((char *) geom_type, "MULTIPOINT") == 0)
	      shape = GAIA_POINT;
      }
/* is the datasource a genuine registered Geometry ?? */
    list = gaiaGetVectorLayersList (sqlite, table, column,
				    GAIA_VECTORS_LIST_PESSIMISTIC);
    if (list == NULL)
      {
	  /* attempting to recover an unregistered Geometry */
	  list = recover_unregistered_geometry (sqlite, table, column);
      }

    if (list != NULL)
	lyr = list->First;
    if (lyr == NULL)
      {
	  gaiaFreeVectorLayersList (list);
	  if (!err_msg)
	      spatialite_e
		  ("Unable to detect GeometryType for \"%s\".\"%s\" ... sorry\n",
		   table, column);
	  else
	      sprintf (err_msg,
		       "Unable to detect GeometryType for \"%s\".\"%s\" ... sorry\n",
		       table, column);
	  return 0;
      }

    switch (lyr->GeometryType)
      {
      case GAIA_VECTOR_POINT:
	  switch (lyr->Dimensions)
	    {
	    case GAIA_XY:
		shape = GAIA_POINT;
		break;
	    case GAIA_XY_Z:
		shape = GAIA_POINTZ;
		break;
	    case GAIA_XY_M:
		shape = GAIA_POINTM;
		break;
	    case GAIA_XY_Z_M:
		shape = GAIA_POINTZM;
		break;
	    };
	  break;
      case GAIA_VECTOR_LINESTRING:
	  switch (lyr->Dimensions)
	    {
	    case GAIA_XY:
		shape = GAIA_LINESTRING;
		break;
	    case GAIA_XY_Z:
		shape = GAIA_LINESTRINGZ;
		break;
	    case GAIA_XY_M:
		shape = GAIA_LINESTRINGM;
		break;
	    case GAIA_XY_Z_M:
		shape = GAIA_LINESTRINGZM;
		break;
	    };
	  break;
      case GAIA_VECTOR_POLYGON:
	  switch (lyr->Dimensions)
	    {
	    case GAIA_XY:
		shape = GAIA_POLYGON;
		break;
	    case GAIA_XY_Z:
		shape = GAIA_POLYGONZ;
		break;
	    case GAIA_XY_M:
		shape = GAIA_POLYGONM;
		break;
	    case GAIA_XY_Z_M:
		shape = GAIA_POLYGONZM;
		break;
	    };
	  break;
      case GAIA_VECTOR_MULTIPOINT:
	  switch (lyr->Dimensions)
	    {
	    case GAIA_XY:
		shape = GAIA_MULTIPOINT;
		break;
	    case GAIA_XY_Z:
		shape = GAIA_MULTIPOINTZ;
		break;
	    case GAIA_XY_M:
		shape = GAIA_MULTIPOINTM;
		break;
	    case GAIA_XY_Z_M:
		shape = GAIA_MULTIPOINTZM;
		break;
	    };
	  break;
      case GAIA_VECTOR_MULTILINESTRING:
	  switch (lyr->Dimensions)
	    {
	    case GAIA_XY:
		shape = GAIA_MULTILINESTRING;
		break;
	    case GAIA_XY_Z:
		shape = GAIA_MULTILINESTRINGZ;
		break;
	    case GAIA_XY_M:
		shape = GAIA_MULTILINESTRINGM;
		break;
	    case GAIA_XY_Z_M:
		shape = GAIA_MULTILINESTRINGZM;
		break;
	    };
	  break;
      case GAIA_VECTOR_MULTIPOLYGON:
	  switch (lyr->Dimensions)
	    {
	    case GAIA_XY:
		shape = GAIA_MULTIPOLYGON;
		break;
	    case GAIA_XY_Z:
		shape = GAIA_MULTIPOLYGONZ;
		break;
	    case GAIA_XY_M:
		shape = GAIA_MULTIPOLYGONM;
		break;
	    case GAIA_XY_Z_M:
		shape = GAIA_MULTIPOLYGONZM;
		break;
	    };
	  break;
      };

    if (shape < 0)
      {
	  if (!err_msg)
	      spatialite_e
		  ("Unable to detect GeometryType for \"%s\".\"%s\" ... sorry\n",
		   table, column);
	  else
	      sprintf (err_msg,
		       "Unable to detect GeometryType for \"%s\".\"%s\" ... sorry\n",
		       table, column);
	  return 0;
      }
    if (verbose)
	spatialite_e
	    ("========\nDumping SQLite table '%s' into shapefile at '%s'\n",
	     table, shp_path);
    /* preparing SQL statement */
    xtable = gaiaDoubleQuotedSql (table);
    xcolumn = gaiaDoubleQuotedSql (column);
    if (shape == GAIA_LINESTRING || shape == GAIA_LINESTRINGZ
	|| shape == GAIA_LINESTRINGM || shape == GAIA_LINESTRINGZM ||
	shape == GAIA_MULTILINESTRING || shape == GAIA_MULTILINESTRINGZ
	|| shape == GAIA_MULTILINESTRINGM || shape == GAIA_MULTILINESTRINGZM)
      {
	  sql =
	      sqlite3_mprintf
	      ("SELECT * FROM \"%s\" WHERE GeometryAliasType(\"%s\") = "
	       "'LINESTRING' OR GeometryAliasType(\"%s\") = 'MULTILINESTRING' "
	       "OR \"%s\" IS NULL", xtable, xcolumn, xcolumn, xcolumn);
      }
    else if (shape == GAIA_POLYGON || shape == GAIA_POLYGONZ
	     || shape == GAIA_POLYGONM || shape == GAIA_POLYGONZM ||
	     shape == GAIA_MULTIPOLYGON || shape == GAIA_MULTIPOLYGONZ
	     || shape == GAIA_MULTIPOLYGONM || shape == GAIA_MULTIPOLYGONZM)
      {
	  sql =
	      sqlite3_mprintf
	      ("SELECT * FROM \"%s\" WHERE GeometryAliasType(\"%s\") = "
	       "'POLYGON' OR GeometryAliasType(\"%s\") = 'MULTIPOLYGON'"
	       "OR \"%s\" IS NULL", xtable, xcolumn, xcolumn, xcolumn);
      }
    else if (shape == GAIA_MULTIPOINT || shape == GAIA_MULTIPOINTZ
	     || shape == GAIA_MULTIPOINTM || shape == GAIA_MULTIPOINTZM)
      {
	  sql =
	      sqlite3_mprintf
	      ("SELECT * FROM \"%s\" WHERE GeometryAliasType(\"%s\") = "
	       "'POINT' OR GeometryAliasType(\"%s\") = 'MULTIPOINT'"
	       "OR \"%s\" IS NULL", xtable, xcolumn, xcolumn, xcolumn);
      }
    else
      {
	  sql =
	      sqlite3_mprintf
	      ("SELECT * FROM \"%s\" WHERE GeometryAliasType(\"%s\") = "
	       "'POINT' OR \"%s\" IS NULL", xtable, xcolumn, xcolumn);
      }
/* compiling SQL prepared statement */
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto sql_error;

    if (lyr->First == NULL)
      {
	  /* the datasource is probably empty - zero rows */
	  if (get_default_dbf_fields (sqlite, xtable, &dbf_list))
	      goto continue_exporting;
      }


/* preparing the DBF fields list */
    dbf_list = gaiaAllocDbfList ();
    offset = 0;
    fld = lyr->First;
    while (fld)
      {
	  int sql_type = SQLITE_NULL;
	  int max_len;
	  if (strcasecmp (fld->AttributeFieldName, column) == 0)
	    {
		/* ignoring the Geometry itself */
		fld = fld->Next;
		continue;
	    }
	  if (fld->IntegerValuesCount > 0 && fld->DoubleValuesCount == 0
	      && fld->TextValuesCount == 0)
	    {
		sql_type = SQLITE_INTEGER;
		max_len = 18;
		if (fld->IntRange)
		    max_len =
			compute_max_int_length (fld->IntRange->MinValue,
						fld->IntRange->MaxValue);
	    }
	  if (fld->DoubleValuesCount > 0 && fld->TextValuesCount == 0)
	    {
		sql_type = SQLITE_FLOAT;
		max_len = 19;
		if (fld->DoubleRange)
		    max_len =
			compute_max_dbl_length (fld->DoubleRange->MinValue,
						fld->DoubleRange->MaxValue);
	    }
	  if (fld->TextValuesCount > 0)
	    {
		sql_type = SQLITE_TEXT;
		max_len = 255;
		if (fld->MaxSize)
		    max_len = fld->MaxSize->MaxSize;
	    }
	  if (sql_type == SQLITE_NULL)
	    {
		/* considering as TEXT(1) */
		sql_type = SQLITE_TEXT;
		max_len = 1;
	    }
	  /* adding a DBF field */
	  if (sql_type == SQLITE_TEXT)
	    {
		if (max_len == 0)	/* avoiding ZERO-length fields */
		    max_len = 1;
		gaiaAddDbfField (dbf_list, fld->AttributeFieldName, 'C', offset,
				 max_len, 0);
		offset += max_len;
	    }
	  if (sql_type == SQLITE_FLOAT)
	    {
		if (max_len > 19)
		    max_len = 19;
		gaiaAddDbfField (dbf_list, fld->AttributeFieldName, 'N', offset,
				 max_len, 6);
		offset += max_len;
	    }
	  if (sql_type == SQLITE_INTEGER)
	    {
		if (max_len > 18)
		    max_len = 18;
		gaiaAddDbfField (dbf_list, fld->AttributeFieldName, 'N', offset,
				 max_len, 0);
		offset += max_len;
	    }
	  fld = fld->Next;
      }

/* resetting SQLite query */
  continue_exporting:
    ret = sqlite3_reset (stmt);
    if (ret != SQLITE_OK)
	goto sql_error;
/* trying to open shapefile files */
    shp = gaiaAllocShapefile ();
    gaiaOpenShpWrite (shp, shp_path, shape, dbf_list, "UTF-8", charset);
    if (!(shp->Valid))
	goto no_file;
/* trying to export the .PRJ file */
    output_prj_file (sqlite, shp_path, table, column);
    while (1)
      {
	  /* scrolling the result set to dump data into shapefile */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		if (n_cols == 0)
		    n_cols = sqlite3_column_count (stmt);
		rows++;
		dbf_write = gaiaCloneDbfEntity (dbf_list);
		for (i = 0; i < n_cols; i++)
		  {
		      if (strcasecmp
			  ((char *) column,
			   (char *) sqlite3_column_name (stmt, i)) == 0)
			{
			    /* this one is the internal BLOB encoded GEOMETRY to be exported */
			    if (sqlite3_column_type (stmt, i) != SQLITE_BLOB)
			      {
				  /* this one is a NULL Geometry */
				  dbf_write->Geometry = NULL;
			      }
			    else
			      {
				  blob_value = sqlite3_column_blob (stmt, i);
				  len = sqlite3_column_bytes (stmt, i);
				  dbf_write->Geometry =
				      gaiaFromSpatiaLiteBlobWkb (blob_value,
								 len);
			      }
			}
		      dummy = (char *) sqlite3_column_name (stmt, i);
		      dbf_field = getDbfField (dbf_write, dummy);
		      if (!dbf_field)
			  continue;
		      if (sqlite3_column_type (stmt, i) == SQLITE_NULL)
			{
			    /* handling NULL values */
			    gaiaSetNullValue (dbf_field);
			}
		      else
			{
			    switch (dbf_field->Type)
			      {
			      case 'N':
				  if (sqlite3_column_type (stmt, i) ==
				      SQLITE_INTEGER)
				      gaiaSetIntValue (dbf_field,
						       sqlite3_column_int64
						       (stmt, i));
				  else if (sqlite3_column_type (stmt, i) ==
					   SQLITE_FLOAT)
				      gaiaSetDoubleValue (dbf_field,
							  sqlite3_column_double
							  (stmt, i));
				  else
				      gaiaSetNullValue (dbf_field);
				  break;
			      case 'C':
				  if (sqlite3_column_type (stmt, i) ==
				      SQLITE_TEXT)
				    {
					dummy =
					    (char *)
					    sqlite3_column_text (stmt, i);
					gaiaSetStrValue (dbf_field, dummy);
				    }
				  else if (sqlite3_column_type (stmt, i) ==
					   SQLITE_INTEGER)
				    {
#if defined(_WIN32) || defined(__MINGW32__)
					/* CAVEAT - M$ runtime doesn't supports %lld for 64 bits */
					sprintf (buf, "%I64d",
						 sqlite3_column_int64 (stmt,
								       i));
#else
					sprintf (buf, "%lld",
						 sqlite3_column_int64 (stmt,
								       i));
#endif
					gaiaSetStrValue (dbf_field, buf);
				    }
				  else if (sqlite3_column_type (stmt, i) ==
					   SQLITE_FLOAT)
				    {
					sql = sqlite3_mprintf ("%1.6f",
							       sqlite3_column_double
							       (stmt, i));
					gaiaSetStrValue (dbf_field, sql);
					sqlite3_free (sql);
				    }
				  else
				      gaiaSetNullValue (dbf_field);
				  break;
			      };
			}
		  }
		if (!gaiaWriteShpEntity (shp, dbf_write))
		    spatialite_e ("shapefile write error\n");
		gaiaFreeDbfList (dbf_write);
	    }
	  else
	      goto sql_error;
      }
    sqlite3_finalize (stmt);
    gaiaFlushShpHeaders (shp);
    gaiaFreeShapefile (shp);
    free (xtable);
    free (xcolumn);
    gaiaFreeVectorLayersList (list);
    if (verbose)
	spatialite_e ("\nExported %d rows into SHAPEFILE\n========\n", rows);
    if (xrows)
	*xrows = rows;
    if (err_msg)
	sprintf (err_msg, "Exported %d rows into SHAPEFILE", rows);
    return 1;
  sql_error:
/* some SQL error occurred */
    sqlite3_finalize (stmt);
    free (xtable);
    free (xcolumn);
    gaiaFreeVectorLayersList (list);
    if (dbf_list)
	gaiaFreeDbfList (dbf_list);
    if (shp)
	gaiaFreeShapefile (shp);
    if (!err_msg)
	spatialite_e ("SELECT failed: %s", sqlite3_errmsg (sqlite));
    else
	sprintf (err_msg, "SELECT failed: %s", sqlite3_errmsg (sqlite));
    return 0;
  no_file:
/* shapefile can't be created/opened */
    free (xtable);
    free (xcolumn);
    gaiaFreeVectorLayersList (list);
    if (dbf_list)
	gaiaFreeDbfList (dbf_list);
    if (shp)
	gaiaFreeShapefile (shp);
    if (!err_msg)
	spatialite_e ("ERROR: unable to open '%s' for writing", shp_path);
    else
	sprintf (err_msg, "ERROR: unable to open '%s' for writing", shp_path);
    return 0;
}

SPATIALITE_DECLARE int
load_dbf (sqlite3 * sqlite, char *dbf_path, char *table, char *charset,
	  int verbose, int *rows, char *err_msg)
{
    return load_dbf_ex (sqlite, dbf_path, table, NULL, charset, verbose, rows,
			err_msg);
}

SPATIALITE_DECLARE int
load_dbf_ex (sqlite3 * sqlite, char *dbf_path, char *table, char *pk_column,
	     char *charset, int verbose, int *rows, char *err_msg)
{
    sqlite3_stmt *stmt;
    int ret;
    char *errMsg = NULL;
    char *sql;
    char *dummy;
    char *xname;
    int already_exists = 0;
    int sqlError = 0;
    gaiaDbfPtr dbf = NULL;
    gaiaDbfFieldPtr dbf_field;
    int cnt;
    int col_cnt;
    int seed;
    int len;
    int dup;
    int idup;
    int current_row;
    char **col_name = NULL;
    int deleted;
    char *qtable = NULL;
    char *qpk_name = NULL;
    char *pk_name = NULL;
    int pk_autoincr = 1;
    gaiaOutBuffer sql_statement;
    int pk_type = SQLITE_INTEGER;
    int pk_set;
    qtable = gaiaDoubleQuotedSql (table);
/* checking if TABLE already exists */
    sql = sqlite3_mprintf ("SELECT name FROM sqlite_master WHERE "
			   "type = 'table' AND Lower(name) = Lower(%Q)", table);
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  if (!err_msg)
	      spatialite_e ("load DBF error: <%s>\n", sqlite3_errmsg (sqlite));
	  else
	      sprintf (err_msg, "load DBF error: <%s>\n",
		       sqlite3_errmsg (sqlite));
	  if (qtable)
	      free (qtable);
	  if (qpk_name)
	      free (qpk_name);
	  return 0;
      }
    while (1)
      {
	  /* scrolling the result set */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	      already_exists = 1;
	  else
	    {
		spatialite_e ("load DBF error: <%s>\n",
			      sqlite3_errmsg (sqlite));
		break;
	    }
      }
    sqlite3_finalize (stmt);
    if (already_exists)
      {
	  if (!err_msg)
	      spatialite_e ("load DBF error: table '%s' already exists\n",
			    table);
	  else
	      sprintf (err_msg, "load DBF error: table '%s' already exists\n",
		       table);
	  if (qtable)
	      free (qtable);
	  if (qpk_name)
	      free (qpk_name);
	  return 0;
      }
    dbf = gaiaAllocDbf ();
    gaiaOpenDbfRead (dbf, dbf_path, charset, "UTF-8");
    if (!(dbf->Valid))
      {
	  if (!err_msg)
	    {
		spatialite_e ("load DBF error: cannot open '%s'\n", dbf_path);
		if (dbf->LastError)
		    spatialite_e ("\tcause: %s\n", dbf->LastError);
	    }
	  else
	    {
		char extra[512];
		*extra = '\0';
		if (dbf->LastError)
		    sprintf (extra, "\n\tcause: %s", dbf->LastError);
		sprintf (err_msg, "load DBF error: cannot open '%s'%s",
			 dbf_path, extra);
	    }
	  gaiaFreeDbf (dbf);
	  if (qtable)
	      free (qtable);
	  if (qpk_name)
	      free (qpk_name);
	  return 0;
      }
/* checking for duplicate / illegal column names and antialising them */
    col_cnt = 0;
    dbf_field = dbf->Dbf->First;
    while (dbf_field)
      {
	  /* counting DBF fields */
	  col_cnt++;
	  dbf_field = dbf_field->Next;
      }
    col_name = malloc (sizeof (char *) * col_cnt);
    cnt = 0;
    seed = 0;
    if (pk_column != NULL)
      {
	  /* validating the Primary Key column */
	  dbf_field = dbf->Dbf->First;
	  while (dbf_field)
	    {
		if (strcasecmp (pk_column, dbf_field->Name) == 0)
		  {
		      /* ok, using this field as Primary Key */
		      pk_name = pk_column;
		      pk_autoincr = 0;
		      switch (dbf_field->Type)
			{
			case 'C':
			    pk_type = SQLITE_TEXT;
			    break;
			case 'N':
			    if (dbf_field->Decimals)
				pk_type = SQLITE_FLOAT;
			    else
			      {
				  if (dbf_field->Length <= 18)
				      pk_type = SQLITE_INTEGER;
				  else
				      pk_type = SQLITE_FLOAT;
			      }
			    break;
			case 'D':
			    pk_type = SQLITE_FLOAT;
			    break;
			case 'F':
			    pk_type = SQLITE_FLOAT;
			    break;
			case 'L':
			    pk_type = SQLITE_INTEGER;
			    break;
			};
		  }
		dbf_field = dbf_field->Next;
	    }
      }
    if (pk_name == NULL)
      {
	  if (pk_column != NULL)
	      pk_name = pk_column;
	  else
	      pk_name = "PK_UID";
      }
    qpk_name = gaiaDoubleQuotedSql (pk_name);
    dbf_field = dbf->Dbf->First;
    while (dbf_field)
      {
	  /* preparing column names */
	  char *xdummy = NULL;
	  if (strcasecmp (pk_name, dbf_field->Name) == 0)
	    {
		/* skipping the Primary Key field */
		dummy = dbf_field->Name;
		len = strlen (dummy);
		*(col_name + cnt) = malloc (len + 1);
		strcpy (*(col_name + cnt), dummy);
		cnt++;
		dbf_field = dbf_field->Next;
		continue;
	    }
	  dummy = dbf_field->Name;
	  dup = 0;
	  for (idup = 0; idup < cnt; idup++)
	    {
		if (strcasecmp (dummy, *(col_name + idup)) == 0)
		    dup = 1;
	    }
	  if (dup)
	    {
		xdummy = sqlite3_mprintf ("COL_%d", seed++);
		dummy = xdummy;
	    }
	  len = strlen (dummy);
	  *(col_name + cnt) = malloc (len + 1);
	  strcpy (*(col_name + cnt), dummy);
	  if (xdummy)
	      free (xdummy);
	  cnt++;
	  dbf_field = dbf_field->Next;
      }
    if (verbose)
	spatialite_e ("========\nLoading DBF at '%s' into SQLite table '%s'\n",
		      dbf_path, table);
/* starting a transaction */
    if (verbose)
	spatialite_e ("\nBEGIN;\n");
    ret = sqlite3_exec (sqlite, "BEGIN", NULL, 0, &errMsg);
    if (ret != SQLITE_OK)
      {
	  if (!err_msg)
	      spatialite_e ("load DBF error: <%s>\n", errMsg);
	  else
	      sprintf (err_msg, "load DBF error: <%s>\n", errMsg);
	  sqlite3_free (errMsg);
	  sqlError = 1;
	  goto clean_up;
      }
/* creating the Table */
    gaiaOutBufferInitialize (&sql_statement);
    if (pk_type == SQLITE_TEXT)
      {
	  sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n\"%s\" "
				 "TEXT PRIMARY KEY NOT NULL", qtable, qpk_name);
      }
    else if (pk_type == SQLITE_FLOAT)
      {
	  sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n\"%s\" "
				 "DOUBLE PRIMARY KEY NOT NULL", qtable,
				 qpk_name);
      }
    else
      {
	  if (pk_autoincr)
	      sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n\"%s\" "
				     "INTEGER PRIMARY KEY AUTOINCREMENT",
				     qtable, qpk_name);
	  else
	      sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n\"%s\" "
				     "INTEGER NOT NULL PRIMARY KEY", qtable,
				     qpk_name);
      }
    gaiaAppendToOutBuffer (&sql_statement, sql);
    sqlite3_free (sql);
    cnt = 0;
    dbf_field = dbf->Dbf->First;
    while (dbf_field)
      {
	  if (strcasecmp (pk_name, dbf_field->Name) == 0)
	    {
		/* skipping the Primary Key field */
		dbf_field = dbf_field->Next;
		cnt++;
		continue;
	    }
	  xname = gaiaDoubleQuotedSql (*(col_name + cnt));
	  sql = sqlite3_mprintf (",\n\"%s\"", xname);
	  free (xname);
	  gaiaAppendToOutBuffer (&sql_statement, sql);
	  sqlite3_free (sql);
	  cnt++;
	  switch (dbf_field->Type)
	    {
	    case 'C':
		gaiaAppendToOutBuffer (&sql_statement, " TEXT");
		break;
	    case 'N':
		if (dbf_field->Decimals)
		    gaiaAppendToOutBuffer (&sql_statement, " DOUBLE");
		else
		  {
		      if (dbf_field->Length <= 18)
			  gaiaAppendToOutBuffer (&sql_statement, " INTEGER");
		      else
			  gaiaAppendToOutBuffer (&sql_statement, " DOUBLE");
		  }
		break;
	    case 'D':
		gaiaAppendToOutBuffer (&sql_statement, " DOUBLE");
		break;
	    case 'F':
		gaiaAppendToOutBuffer (&sql_statement, " DOUBLE");
		break;
	    case 'L':
		gaiaAppendToOutBuffer (&sql_statement, " INTEGER");
		break;
	    };
	  dbf_field = dbf_field->Next;
      }
    gaiaAppendToOutBuffer (&sql_statement, ")");
    if (sql_statement.Error == 0 && sql_statement.Buffer != NULL)
      {
	  if (verbose)
	      spatialite_e ("%s;\n", sql_statement.Buffer);
	  ret = sqlite3_exec (sqlite, sql_statement.Buffer, NULL, 0, &errMsg);
      }
    else
	ret = SQLITE_ERROR;
    gaiaOutBufferReset (&sql_statement);
    if (ret != SQLITE_OK)
      {
	  if (!err_msg)
	      spatialite_e ("load DBF error: <%s>\n", errMsg);
	  else
	      sprintf (err_msg, "load DBF error: <%s>\n", errMsg);
	  sqlite3_free (errMsg);
	  sqlError = 1;
	  goto clean_up;
      }
    /* preparing the INSERT INTO parametrerized statement */
    sql = sqlite3_mprintf ("INSERT INTO \"%s\" (\"%s\"", qtable, qpk_name);
    gaiaAppendToOutBuffer (&sql_statement, sql);
    sqlite3_free (sql);
    cnt = 0;
    dbf_field = dbf->Dbf->First;
    while (dbf_field)
      {
	  /* columns corresponding to some DBF attribute */
	  if (strcasecmp (pk_name, dbf_field->Name) == 0)
	    {
		/* skipping the Primary Key field */
		dbf_field = dbf_field->Next;
		cnt++;
		continue;
	    }
	  xname = gaiaDoubleQuotedSql (*(col_name + cnt++));
	  sql = sqlite3_mprintf (",\"%s\"", xname);
	  free (xname);
	  gaiaAppendToOutBuffer (&sql_statement, sql);
	  sqlite3_free (sql);
	  dbf_field = dbf_field->Next;
      }
    gaiaAppendToOutBuffer (&sql_statement, ")\nVALUES (?");
    dbf_field = dbf->Dbf->First;
    while (dbf_field)
      {
	  /* column values */
	  if (strcasecmp (pk_name, dbf_field->Name) == 0)
	    {
		/* skipping the Primary Key field */
		dbf_field = dbf_field->Next;
		continue;
	    }
	  gaiaAppendToOutBuffer (&sql_statement, ", ?");
	  dbf_field = dbf_field->Next;
      }
    gaiaAppendToOutBuffer (&sql_statement, ")");
    if (sql_statement.Error == 0 && sql_statement.Buffer != NULL)
	ret =
	    sqlite3_prepare_v2 (sqlite, sql_statement.Buffer,
				strlen (sql_statement.Buffer), &stmt, NULL);
    else
	ret = SQLITE_ERROR;
    gaiaOutBufferReset (&sql_statement);
    if (ret != SQLITE_OK)
      {
	  if (!err_msg)
	      spatialite_e ("load DBF error: <%s>\n", sqlite3_errmsg (sqlite));
	  else
	      sprintf (err_msg, "load DBF error: <%s>\n",
		       sqlite3_errmsg (sqlite));
	  sqlError = 1;
	  goto clean_up;
      }
    current_row = 0;
    while (1)
      {
	  /* inserting rows from DBF */
	  ret = gaiaReadDbfEntity (dbf, current_row, &deleted);
	  if (!ret)
	    {
		if (!(dbf->LastError))	/* normal DBF EOF */
		    break;
		if (!err_msg)
		    spatialite_e ("%s\n", dbf->LastError);
		else
		    sprintf (err_msg, "%s\n", dbf->LastError);
		sqlError = 1;
		goto clean_up;
	    }
	  current_row++;
	  if (deleted)
	    {
		/* skipping DBF deleted row */
		continue;
	    }
	  /* binding query params */
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  pk_set = 0;
	  cnt = 0;
	  dbf_field = dbf->Dbf->First;
	  while (dbf_field)
	    {
		/* Primary Key value */
		if (strcasecmp (pk_name, dbf_field->Name) == 0)
		  {
		      if (pk_type == SQLITE_TEXT)
			  sqlite3_bind_text (stmt, 1,
					     dbf_field->Value->TxtValue,
					     strlen (dbf_field->
						     Value->TxtValue),
					     SQLITE_STATIC);
		      else if (pk_type == SQLITE_FLOAT)
			  sqlite3_bind_double (stmt, 1,
					       dbf_field->Value->DblValue);
		      else
			  sqlite3_bind_int64 (stmt, 1,
					      dbf_field->Value->IntValue);
		      pk_set = 1;
		  }
		dbf_field = dbf_field->Next;
	    }
	  if (!pk_set)
	      sqlite3_bind_int (stmt, 1, current_row);
	  sqlite3_bind_int (stmt, 1, current_row);
	  cnt = 0;
	  dbf_field = dbf->Dbf->First;
	  while (dbf_field)
	    {
		/* column values */
		if (strcasecmp (pk_name, dbf_field->Name) == 0)
		  {
		      /* skipping the Primary Key field */
		      dbf_field = dbf_field->Next;
		      continue;
		  }
		if (!(dbf_field->Value))
		    sqlite3_bind_null (stmt, cnt + 2);
		else
		  {
		      switch (dbf_field->Value->Type)
			{
			case GAIA_INT_VALUE:
			    sqlite3_bind_int64 (stmt, cnt + 2,
						dbf_field->Value->IntValue);
			    break;
			case GAIA_DOUBLE_VALUE:
			    sqlite3_bind_double (stmt, cnt + 2,
						 dbf_field->Value->DblValue);
			    break;
			case GAIA_TEXT_VALUE:
			    sqlite3_bind_text (stmt, cnt + 2,
					       dbf_field->Value->TxtValue,
					       strlen (dbf_field->Value->
						       TxtValue),
					       SQLITE_STATIC);
			    break;
			default:
			    sqlite3_bind_null (stmt, cnt + 2);
			    break;
			}
		  }
		cnt++;
		dbf_field = dbf_field->Next;
	    }
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		if (!err_msg)
		    spatialite_e ("load DBF error: <%s>\n",
				  sqlite3_errmsg (sqlite));
		else
		    sprintf (err_msg, "load DBF error: <%s>\n",
			     sqlite3_errmsg (sqlite));
		sqlite3_finalize (stmt);
		sqlError = 1;
		goto clean_up;
	    }
      }
    sqlite3_finalize (stmt);
  clean_up:
    if (qtable)
	free (qtable);
    if (qpk_name)
	free (qpk_name);
    gaiaFreeDbf (dbf);
    if (col_name)
      {
	  /* releasing memory allocation for column names */
	  for (cnt = 0; cnt < col_cnt; cnt++)
	      free (*(col_name + cnt));
	  free (col_name);
      }
    if (sqlError)
      {
	  /* some error occurred - ROLLBACK */
	  if (verbose)
	      spatialite_e ("ROLLBACK;\n");
	  ret = sqlite3_exec (sqlite, "ROLLBACK", NULL, 0, &errMsg);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("load DBF error: <%s>\n", errMsg);
		sqlite3_free (errMsg);
	    };
	  if (rows)
	      *rows = current_row;
	  if (qtable)
	      free (qtable);
	  if (qpk_name)
	      free (qpk_name);
	  return 0;
      }
    else
      {
	  /* ok - confirming pending transaction - COMMIT */
	  if (verbose)
	      spatialite_e ("COMMIT;\n");
	  ret = sqlite3_exec (sqlite, "COMMIT", NULL, 0, &errMsg);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("load DBF error: <%s>\n", errMsg);
		sqlite3_free (errMsg);
		return 0;
	    }
	  if (rows)
	      *rows = current_row;
	  if (verbose)
	      spatialite_e ("\nInserted %d rows into '%s' from DBF\n========\n",
			    current_row, table);
	  if (err_msg)
	      sprintf (err_msg, "Inserted %d rows into '%s' from DBF",
		       current_row, table);
	  return 1;
      }
}

SPATIALITE_DECLARE int
dump_dbf (sqlite3 * sqlite, char *table, char *dbf_path, char *charset,
	  char *err_msg)
{
/* DBF dump */
    int rows;
    int i;
    char *sql;
    char *xtable;
    sqlite3_stmt *stmt;
    int row1 = 0;
    int n_cols = 0;
    int offset = 0;
    int type;
    gaiaDbfPtr dbf = NULL;
    gaiaDbfListPtr dbf_export_list = NULL;
    gaiaDbfListPtr dbf_list = NULL;
    gaiaDbfListPtr dbf_write;
    gaiaDbfFieldPtr dbf_field;
    int *max_length = NULL;
    int *sql_type = NULL;
    char *dummy;
    char buf[256];
    int len;
    int ret;
/*
/ preparing SQL statement 
*/
    xtable = gaiaDoubleQuotedSql (table);
    sql = sqlite3_mprintf ("SELECT * FROM \"%s\"", xtable);
/*
/ compiling SQL prepared statement 
*/
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto sql_error;
    rows = 0;
    while (1)
      {
	  /*
	     / Pass I - scrolling the result set to compute real DBF attributes' sizes and types 
	   */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* processing a result set row */
		row1++;
		if (n_cols == 0)
		  {
		      /* this one is the first row, so we are going to prepare the DBF Fields list */
		      n_cols = sqlite3_column_count (stmt);
		      dbf_export_list = gaiaAllocDbfList ();
		      max_length = (int *) malloc (sizeof (int) * n_cols);
		      sql_type = (int *) malloc (sizeof (int) * n_cols);
		      for (i = 0; i < n_cols; i++)
			{
			    /* initializes the DBF export fields */
			    dummy = (char *) sqlite3_column_name (stmt, i);
			    gaiaAddDbfField (dbf_export_list, dummy, '\0', 0, 0,
					     0);
			    max_length[i] = 0;
			    sql_type[i] = SQLITE_NULL;
			}
		  }
		for (i = 0; i < n_cols; i++)
		  {
		      /* update the DBF export fields analyzing fetched data */
		      type = sqlite3_column_type (stmt, i);
		      if (type == SQLITE_NULL || type == SQLITE_BLOB)
			  continue;
		      if (type == SQLITE_TEXT)
			{
			    len = sqlite3_column_bytes (stmt, i);
			    sql_type[i] = SQLITE_TEXT;
			    if (len > max_length[i])
				max_length[i] = len;
			}
		      else if (type == SQLITE_FLOAT
			       && sql_type[i] != SQLITE_TEXT)
			  sql_type[i] = SQLITE_FLOAT;	/* promoting a numeric column to be DOUBLE */
		      else if (type == SQLITE_INTEGER
			       && (sql_type[i] == SQLITE_NULL
				   || sql_type[i] == SQLITE_INTEGER))
			  sql_type[i] = SQLITE_INTEGER;	/* promoting a null column to be INTEGER */
		      if (type == SQLITE_INTEGER && max_length[i] < 18)
			  max_length[i] = 18;
		      if (type == SQLITE_FLOAT && max_length[i] < 24)
			  max_length[i] = 24;
		  }
	    }
	  else
	      goto sql_error;
      }
    if (!row1)
	goto empty_result_set;
    i = 0;
    offset = 0;
    dbf_list = gaiaAllocDbfList ();
    dbf_field = dbf_export_list->First;
    while (dbf_field)
      {
	  /* preparing the final DBF attribute list */
	  if (sql_type[i] == SQLITE_NULL || sql_type[i] == SQLITE_BLOB)
	    {
		i++;
		dbf_field = dbf_field->Next;
		continue;
	    }
	  if (sql_type[i] == SQLITE_TEXT)
	    {
		gaiaAddDbfField (dbf_list, dbf_field->Name, 'C', offset,
				 max_length[i], 0);
		offset += max_length[i];
	    }
	  if (sql_type[i] == SQLITE_FLOAT)
	    {
		gaiaAddDbfField (dbf_list, dbf_field->Name, 'N', offset, 19, 6);
		offset += 19;
	    }
	  if (sql_type[i] == SQLITE_INTEGER)
	    {
		gaiaAddDbfField (dbf_list, dbf_field->Name, 'N', offset, 18, 0);
		offset += 18;
	    }
	  i++;
	  dbf_field = dbf_field->Next;
      }
    free (max_length);
    free (sql_type);
    gaiaFreeDbfList (dbf_export_list);
    dbf_export_list = NULL;

  continue_exporting:
/* resetting SQLite query */
    ret = sqlite3_reset (stmt);
    if (ret != SQLITE_OK)
	goto sql_error;
/* trying to open the DBF file */
    dbf = gaiaAllocDbf ();
/* xfering export-list ownership */
    dbf->Dbf = dbf_list;
    dbf_list = NULL;
    gaiaOpenDbfWrite (dbf, dbf_path, "UTF-8", charset);
    if (!(dbf->Valid))
	goto no_file;
    while (1)
      {
	  /* Pass II - scrolling the result set to dump data into DBF */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		rows++;
		dbf_write = gaiaCloneDbfEntity (dbf->Dbf);
		for (i = 0; i < n_cols; i++)
		  {
		      dummy = (char *) sqlite3_column_name (stmt, i);
		      dbf_field = getDbfField (dbf_write, dummy);
		      if (!dbf_field)
			  continue;
		      if (sqlite3_column_type (stmt, i) == SQLITE_NULL
			  || sqlite3_column_type (stmt, i) == SQLITE_BLOB)
			{
			    /* handling NULL values */
			    gaiaSetNullValue (dbf_field);
			}
		      else
			{
			    switch (dbf_field->Type)
			      {
			      case 'N':
				  if (sqlite3_column_type (stmt, i) ==
				      SQLITE_INTEGER)
				      gaiaSetIntValue (dbf_field,
						       sqlite3_column_int64
						       (stmt, i));
				  else if (sqlite3_column_type (stmt, i) ==
					   SQLITE_FLOAT)
				      gaiaSetDoubleValue (dbf_field,
							  sqlite3_column_double
							  (stmt, i));
				  else
				      gaiaSetNullValue (dbf_field);
				  break;
			      case 'C':
				  if (sqlite3_column_type (stmt, i) ==
				      SQLITE_TEXT)
				    {
					dummy = (char *)
					    sqlite3_column_text (stmt, i);
					gaiaSetStrValue (dbf_field, dummy);
				    }
				  else if (sqlite3_column_type (stmt, i) ==
					   SQLITE_INTEGER)
				    {
#if defined(_WIN32) || defined(__MINGW32__)
					/* CAVEAT - M$ runtime doesn't supports %lld for 64 bits */
					sprintf (buf, "%I64d",
						 sqlite3_column_int64 (stmt,
								       i));
#else
					sprintf (buf, "%lld",
						 sqlite3_column_int64 (stmt,
								       i));
#endif
					gaiaSetStrValue (dbf_field, buf);
				    }
				  else if (sqlite3_column_type (stmt, i) ==
					   SQLITE_FLOAT)
				    {
					sql = sqlite3_mprintf ("%1.6f",
							       sqlite3_column_double
							       (stmt, i));
					gaiaSetStrValue (dbf_field, sql);
					sqlite3_free (sql);
				    }
				  else
				      gaiaSetNullValue (dbf_field);
				  break;
			      };
			}
		  }
		if (!gaiaWriteDbfEntity (dbf, dbf_write))
		    spatialite_e ("DBF write error\n");
		gaiaFreeDbfList (dbf_write);
	    }
	  else
	      goto sql_error;
      }
    sqlite3_finalize (stmt);
    gaiaFlushDbfHeader (dbf);
    gaiaFreeDbf (dbf);
    free (xtable);
    if (!err_msg)
	spatialite_e ("Exported %d rows into the DBF file\n", rows);
    else
	sprintf (err_msg, "Exported %d rows into the DBF file\n", rows);
    return 1;
  sql_error:
/* some SQL error occurred */
    free (xtable);
    sqlite3_finalize (stmt);
    if (dbf_export_list)
	gaiaFreeDbfList (dbf_export_list);
    if (dbf_list)
	gaiaFreeDbfList (dbf_list);
    if (dbf)
	gaiaFreeDbf (dbf);
    if (!err_msg)
	spatialite_e ("dump DBF file error: %s\n", sqlite3_errmsg (sqlite));
    else
	sprintf (err_msg, "dump DBF file error: %s\n", sqlite3_errmsg (sqlite));
    return 0;
  no_file:
/* DBF can't be created/opened */
    free (xtable);
    if (dbf_export_list)
	gaiaFreeDbfList (dbf_export_list);
    if (dbf_list)
	gaiaFreeDbfList (dbf_list);
    if (dbf)
	gaiaFreeDbf (dbf);
    if (!err_msg)
	spatialite_e ("ERROR: unable to open '%s' for writing\n", dbf_path);
    else
	sprintf (err_msg, "ERROR: unable to open '%s' for writing\n", dbf_path);
    return 0;
  empty_result_set:
/* the result set is empty - nothing to do */
    if (get_default_dbf_fields (sqlite, xtable, &dbf_list))
	goto continue_exporting;
    free (xtable);
    sqlite3_finalize (stmt);
    if (dbf_export_list)
	gaiaFreeDbfList (dbf_export_list);
    if (dbf_list)
	gaiaFreeDbfList (dbf_list);
    if (dbf)
	gaiaFreeDbf (dbf);
    if (!err_msg)
	spatialite_e
	    ("The SQL SELECT returned an empty result set ... there is nothing to export ...\n");
    else
	sprintf (err_msg,
		 "The SQL SELECT returned an empty result set ... there is nothing to export ...\n");
    return 0;
}

#endif /* end ICONV (SHP and DBF) */

SPATIALITE_DECLARE int
is_kml_constant (sqlite3 * sqlite, char *table, char *column)
{
/* checking a possible column name for KML dump */
    char *sql;
    char *xname;
    int ret;
    int k = 1;
    const char *name;
    char **results;
    int rows;
    int columns;
    int i;
    char *errMsg = NULL;

    xname = gaiaDoubleQuotedSql (table);
    sql = sqlite3_mprintf ("PRAGMA table_info(\"%s\")", xname);
    free (xname);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 1;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		name = results[(i * columns) + 1];
		if (strcasecmp (name, column) == 0)
		    k = 0;
	    }
      }
    sqlite3_free_table (results);
    return k;
}

SPATIALITE_DECLARE int
dump_kml (sqlite3 * sqlite, char *table, char *geom_col, char *kml_path,
	  char *name_col, char *desc_col, int precision)
{
/* dumping a  geometry table as KML */
    char *sql;
    char *xname;
    char *xdesc;
    char *xgeom_col;
    char *xtable;
    sqlite3_stmt *stmt = NULL;
    FILE *out = NULL;
    int ret;
    int rows = 0;
    int is_const = 1;

/* opening/creating the KML file */
    out = fopen (kml_path, "wb");
    if (!out)
	goto no_file;

/* preparing SQL statement */
    if (name_col == NULL)
	xname = sqlite3_mprintf ("%Q", "name");
    else
      {
	  is_const = is_kml_constant (sqlite, table, name_col);
	  if (is_const)
	      xname = sqlite3_mprintf ("%Q", name_col);
	  else
	    {
		xname = gaiaDoubleQuotedSql (name_col);
		sql = sqlite3_mprintf ("\"%s\"", xname);
		free (xname);
		xname = sql;
	    }
      }
    if (desc_col == NULL)
	xdesc = sqlite3_mprintf ("%Q", "description");
    else
      {
	  is_const = is_kml_constant (sqlite, table, desc_col);
	  if (is_const)
	      xdesc = sqlite3_mprintf ("%Q", desc_col);
	  else
	    {
		xdesc = gaiaDoubleQuotedSql (desc_col);
		sql = sqlite3_mprintf ("\"%s\"", xdesc);
		free (xdesc);
		xdesc = sql;
	    }
      }
    xgeom_col = gaiaDoubleQuotedSql (geom_col);
    xtable = gaiaDoubleQuotedSql (table);
    sql = sqlite3_mprintf ("SELECT AsKML(%s, %s, %s, %d) FROM \"%s\" "
			   "WHERE \"%s\" IS NOT NULL", xname, xdesc,
			   xgeom_col, precision, xtable, xgeom_col);
    sqlite3_free (xname);
    sqlite3_free (xdesc);
    free (xgeom_col);
    free (xtable);
/* compiling SQL prepared statement */
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto sql_error;

    while (1)
      {
	  /* scrolling the result set */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* processing a result set row */
		if (rows == 0)
		  {
		      fprintf (out,
			       "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n");
		      fprintf (out,
			       "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\r\n");
		      fprintf (out, "<Document>\r\n");
		  }
		rows++;
		fprintf (out, "\t%s\r\n", sqlite3_column_text (stmt, 0));
	    }
	  else
	      goto sql_error;
      }
    if (!rows)
	goto empty_result_set;


    fprintf (out, "</Document>\r\n");
    fprintf (out, "</kml>\r\n");
    sqlite3_finalize (stmt);
    fclose (out);
    return 1;

  sql_error:
/* some SQL error occurred */
    if (stmt)
	sqlite3_finalize (stmt);
    if (out)
	fclose (out);
    spatialite_e ("Dump KML error: %s\n", sqlite3_errmsg (sqlite));
    return 0;
  no_file:
/* KML file can't be created/opened */
    if (stmt)
	sqlite3_finalize (stmt);
    if (out)
	fclose (out);
    spatialite_e ("ERROR: unable to open '%s' for writing\n", kml_path);
    return 0;
  empty_result_set:
/* the result set is empty - nothing to do */
    if (stmt)
	sqlite3_finalize (stmt);
    if (out)
	fclose (out);
    spatialite_e
	("The SQL SELECT returned an empty result set\n... there is nothing to export ...\n");
    return 0;
}

static int
is_table (sqlite3 * sqlite, const char *table)
{
/* check if this one really is a TABLE */
    char *sql;
    int ret;
    char **results;
    int rows;
    int columns;
    char *errMsg = NULL;
    int ok = 0;

    sql = sqlite3_mprintf ("SELECT tbl_name FROM sqlite_master "
			   "WHERE type = 'table' AND Lower(tbl_name) = Lower(%Q)",
			   table);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQLite SQL error: %s\n", errMsg);
	  sqlite3_free (errMsg);
	  return ok;
      }
    if (rows < 1)
	;
    else
	ok = 1;
    sqlite3_free_table (results);
    return ok;
}

SPATIALITE_DECLARE void
check_duplicated_rows (sqlite3 * sqlite, char *table, int *dupl_count)
{
/* Checking a Table for Duplicate rows */
    char *sql;
    int first = 1;
    char *xname;
    int pk;
    int ret;
    char **results;
    int rows;
    int columns;
    int i;
    char *errMsg = NULL;
    sqlite3_stmt *stmt = NULL;
    gaiaOutBuffer sql_statement;
    gaiaOutBuffer col_list;

    *dupl_count = 0;

    if (is_table (sqlite, table) == 0)
      {
	  spatialite_e (".chkdupl %s: no such table\n", table);
	  return;
      }
/* extracting the column names (excluding any Primary Key) */
    gaiaOutBufferInitialize (&col_list);
    xname = gaiaDoubleQuotedSql (table);
    sql = sqlite3_mprintf ("PRAGMA table_info(\"%s\")", xname);
    free (xname);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQLite SQL error: %s\n", errMsg);
	  sqlite3_free (errMsg);
	  return;
      }
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		sql = results[(i * columns) + 1];
		pk = atoi (results[(i * columns) + 5]);
		if (!pk)
		  {
		      xname = gaiaDoubleQuotedSql (sql);
		      if (first)
			{
			    sql = sqlite3_mprintf ("\"%s\"", xname);
			    first = 0;
			}
		      else
			  sql = sqlite3_mprintf (", \"%s\"", xname);
		      free (xname);
		      gaiaAppendToOutBuffer (&col_list, sql);
		      sqlite3_free (sql);
		  }
	    }
      }
    sqlite3_free_table (results);
    /* preparing the SQL statement */
    gaiaOutBufferInitialize (&sql_statement);
    gaiaAppendToOutBuffer (&sql_statement,
			   "SELECT Count(*) AS \"[dupl-count]\", ");
    if (col_list.Error == 0 && col_list.Buffer != NULL)
	gaiaAppendToOutBuffer (&sql_statement, col_list.Buffer);
    xname = gaiaDoubleQuotedSql (table);
    sql = sqlite3_mprintf ("\nFROM \"%s\"\nGROUP BY ", xname);
    free (xname);
    gaiaAppendToOutBuffer (&sql_statement, sql);
    sqlite3_free (sql);
    if (col_list.Error == 0 && col_list.Buffer != NULL)
	gaiaAppendToOutBuffer (&sql_statement, col_list.Buffer);
    gaiaOutBufferReset (&col_list);
    gaiaAppendToOutBuffer (&sql_statement, "\nHAVING \"[dupl-count]\" > 1");
    gaiaAppendToOutBuffer (&sql_statement, "\nORDER BY \"[dupl-count]\" DESC");
    if (sql_statement.Error == 0 && sql_statement.Buffer != NULL)
      {
	  ret =
	      sqlite3_prepare_v2 (sqlite, sql_statement.Buffer,
				  strlen (sql_statement.Buffer), &stmt, NULL);
	  gaiaOutBufferReset (&sql_statement);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("SQL error: %s\n", sqlite3_errmsg (sqlite));
		return;
	    }
      }
    while (1)
      {
	  /* fetching the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* fetching a row */
		*dupl_count += sqlite3_column_int (stmt, 0) - 1;
	    }
	  else
	    {
		spatialite_e ("SQL error: %s", sqlite3_errmsg (sqlite));
		sqlite3_finalize (stmt);
		return;
	    }
      }
    sqlite3_finalize (stmt);
    if (*dupl_count)
	spatialite_e ("%d duplicated rows found !!!\n", *dupl_count);
    else
	spatialite_e ("No duplicated rows have been identified\n");
}

static int
do_delete_duplicates2 (sqlite3 * sqlite, sqlite3_stmt * stmt1,
		       struct dupl_row *value_list, int *count)
{
/* deleting duplicate rows [actual delete] */
    int cnt = 0;
    int row_no = 0;
    char *sql;
    char *xname;
    int ret;
    sqlite3_stmt *stmt2 = NULL;
    struct dupl_column *col;
    int first = 1;
    int qcnt = 0;
    int param = 1;
    int match;
    int n_cols;
    int col_no;
    gaiaOutBuffer sql_statement;
    gaiaOutBuffer where;
    gaiaOutBuffer condition;

    *count = 0;
    reset_query_pos (value_list);
    gaiaOutBufferInitialize (&sql_statement);
    gaiaOutBufferInitialize (&where);
    gaiaOutBufferInitialize (&condition);

/* preparing the query statement */
    gaiaAppendToOutBuffer (&sql_statement, "SELECT ROWID");
    gaiaAppendToOutBuffer (&where, "\nWHERE ");
    col = value_list->first;
    while (col)
      {
	  if (col->type == SQLITE_BLOB)
	    {
		xname = gaiaDoubleQuotedSql (col->name);
		sql = sqlite3_mprintf (", \"%s\"", xname);
		free (xname);
		gaiaAppendToOutBuffer (&sql_statement, sql);
		sqlite3_free (sql);
		col->query_pos = qcnt++;
	    }
	  else if (col->type == SQLITE_NULL)
	    {
		xname = gaiaDoubleQuotedSql (col->name);
		if (first)
		  {
		      first = 0;
		      sql = sqlite3_mprintf ("\"%s\"", xname);
		      free (xname);
		      gaiaAppendToOutBuffer (&condition, sql);
		      sqlite3_free (sql);
		  }
		else
		  {
		      sql = sqlite3_mprintf (" AND \"%s\"", xname);
		      free (xname);
		      gaiaAppendToOutBuffer (&condition, sql);
		      sqlite3_free (sql);
		  }
		gaiaAppendToOutBuffer (&condition, " IS NULL");
		gaiaAppendToOutBuffer (&where, condition.Buffer);
		gaiaOutBufferReset (&condition);
	    }
	  else
	    {
		xname = gaiaDoubleQuotedSql (col->name);
		if (first)
		  {
		      first = 0;
		      sql = sqlite3_mprintf ("\"%s\"", xname);
		      free (xname);
		      gaiaAppendToOutBuffer (&condition, sql);
		      sqlite3_free (sql);
		  }
		else
		  {
		      sql = sqlite3_mprintf (" AND \"%s\"", xname);
		      free (xname);
		      gaiaAppendToOutBuffer (&condition, sql);
		      sqlite3_free (sql);
		  }
		gaiaAppendToOutBuffer (&condition, " = ?");
		gaiaAppendToOutBuffer (&where, condition.Buffer);
		gaiaOutBufferReset (&condition);
		col->query_pos = param++;
	    }
	  col = col->next;
      }
    xname = gaiaDoubleQuotedSql (value_list->table);
    sql = sqlite3_mprintf ("\nFROM \"%s\"", xname);
    free (xname);
    gaiaAppendToOutBuffer (&sql_statement, sql);
    sqlite3_free (sql);
    gaiaAppendToOutBuffer (&sql_statement, where.Buffer);
    gaiaOutBufferReset (&where);

    if (sql_statement.Error == 0 && sql_statement.Buffer != NULL)
	ret =
	    sqlite3_prepare_v2 (sqlite, sql_statement.Buffer,
				strlen (sql_statement.Buffer), &stmt2, NULL);
    else
	ret = SQLITE_ERROR;
    gaiaOutBufferReset (&sql_statement);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", sqlite3_errmsg (sqlite));
	  goto error;
      }

    sqlite3_reset (stmt2);
    sqlite3_clear_bindings (stmt2);
    col = value_list->first;
    while (col)
      {
	  /* binding query params */
	  if (col->type == SQLITE_INTEGER)
	      sqlite3_bind_int64 (stmt2, col->query_pos, col->int_value);
	  if (col->type == SQLITE_FLOAT)
	      sqlite3_bind_double (stmt2, col->query_pos, col->dbl_value);
	  if (col->type == SQLITE_TEXT)
	      sqlite3_bind_text (stmt2, col->query_pos, col->txt_value,
				 strlen (col->txt_value), SQLITE_STATIC);
	  col = col->next;
      }

    while (1)
      {
	  /* fetching the result set rows */
	  ret = sqlite3_step (stmt2);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* fetching a row */
		match = 1;
		n_cols = sqlite3_column_count (stmt2);
		for (col_no = 1; col_no < n_cols; col_no++)
		  {
		      /* checking blob columns */
		      if (sqlite3_column_type (stmt2, col_no) == SQLITE_BLOB)
			{
			    const void *blob =
				sqlite3_column_blob (stmt2, col_no);
			    int blob_size =
				sqlite3_column_bytes (stmt2, col_no);
			    if (check_dupl_blob
				(value_list, col_no - 1, blob, blob_size) == 0)
				match = 0;
			}
		      else
			  match = 0;
		      if (match == 0)
			  break;
		  }
		if (match == 0)
		    continue;
		row_no++;
		if (row_no > 1)
		  {
		      /* deleting any duplicated row except the first one */
		      sqlite3_reset (stmt1);
		      sqlite3_clear_bindings (stmt1);
		      sqlite3_bind_int64 (stmt1, 1,
					  sqlite3_column_int64 (stmt2, 0));
		      ret = sqlite3_step (stmt1);
		      if (ret == SQLITE_DONE || ret == SQLITE_ROW)
			  cnt++;
		      else
			{
			    spatialite_e ("SQL error: %s\n",
					  sqlite3_errmsg (sqlite));
			    goto error;
			}
		  }
	    }
	  else
	    {
		spatialite_e ("SQL error: %s\n", sqlite3_errmsg (sqlite));
		goto error;
	    }
      }
    if (stmt2)
	sqlite3_finalize (stmt2);
    *count = cnt;
    return 1;

  error:
    if (stmt2)
	sqlite3_finalize (stmt2);
    *count = 0;

    return 0;
}

static int
do_delete_duplicates (sqlite3 * sqlite, const char *sql1, const char *sql2,
		      struct dupl_row *value_list, int *count)
{
/* deleting duplicate rows */
    sqlite3_stmt *stmt1 = NULL;
    sqlite3_stmt *stmt2 = NULL;
    int ret;
    int xcnt;
    int cnt = 0;
    int n_cols;
    int col_no;
    char *sql_err = NULL;

    *count = 0;

/* the complete operation is handled as an unique SQL Transaction */
    ret = sqlite3_exec (sqlite, "BEGIN", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("BEGIN TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }
/* preparing the main SELECT statement */
    ret = sqlite3_prepare_v2 (sqlite, sql1, strlen (sql1), &stmt1, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", sqlite3_errmsg (sqlite));
	  return 0;
      }
/* preparing the DELETE statement */
    ret = sqlite3_prepare_v2 (sqlite, sql2, strlen (sql2), &stmt2, NULL);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", sqlite3_errmsg (sqlite));
	  goto error;
      }

    while (1)
      {
	  /* fetching the result set rows */
	  ret = sqlite3_step (stmt1);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* fetching a row */
		sqlite3_reset (stmt2);
		sqlite3_clear_bindings (stmt2);
		n_cols = sqlite3_column_count (stmt1);
		for (col_no = 1; col_no < n_cols; col_no++)
		  {
		      /* saving column values */
		      if (sqlite3_column_type (stmt1, col_no) == SQLITE_INTEGER)
			  set_int_value (value_list, col_no - 1,
					 sqlite3_column_int64 (stmt1, col_no));
		      if (sqlite3_column_type (stmt1, col_no) == SQLITE_FLOAT)
			  set_double_value (value_list, col_no - 1,
					    sqlite3_column_double (stmt1,
								   col_no));
		      if (sqlite3_column_type (stmt1, col_no) == SQLITE_TEXT)
			{
			    const char *xtext =
				(const char *) sqlite3_column_text (stmt1,
								    col_no);
			    set_text_value (value_list, col_no - 1, xtext);
			}
		      if (sqlite3_column_type (stmt1, col_no) == SQLITE_BLOB)
			{
			    const void *blob =
				sqlite3_column_blob (stmt1, col_no);
			    int blob_size =
				sqlite3_column_bytes (stmt1, col_no);
			    set_blob_value (value_list, col_no - 1, blob,
					    blob_size);
			}
		      if (sqlite3_column_type (stmt1, col_no) == SQLITE_NULL)
			  set_null_value (value_list, col_no - 1);
		  }
		if (do_delete_duplicates2 (sqlite, stmt2, value_list, &xcnt))
		    cnt += xcnt;
		else
		    goto error;
	    }
	  else
	    {
		spatialite_e ("SQL error: %s\n", sqlite3_errmsg (sqlite));
		goto error;
	    }
      }

    sqlite3_finalize (stmt1);
    sqlite3_finalize (stmt2);

/* confirm the still pending Transaction */
    ret = sqlite3_exec (sqlite, "COMMIT", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("COMMIT TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    *count = cnt;
    return 1;

  error:
    *count = 0;
    if (stmt1)
	sqlite3_finalize (stmt1);
    if (stmt2)
	sqlite3_finalize (stmt2);

/* performing a ROLLBACK anyway */
    ret = sqlite3_exec (sqlite, "ROLLBACK", NULL, NULL, &sql_err);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("ROLLBACK TRANSACTION error: %s\n", sql_err);
	  sqlite3_free (sql_err);
	  return 0;
      }

    return 0;
}

SPATIALITE_DECLARE void
remove_duplicated_rows (sqlite3 * sqlite, char *table)
{
/* attempting to delete Duplicate rows from a table */
    struct dupl_row value_list;
    char *sql;
    char *sql2;
    int first = 1;
    char *xname;
    int pk;
    int ret;
    char **results;
    int rows;
    int columns;
    int i;
    char *errMsg = NULL;
    int count;
    gaiaOutBuffer sql_statement;
    gaiaOutBuffer col_list;

    value_list.count = 0;
    value_list.first = NULL;
    value_list.last = NULL;
    value_list.table = table;

    if (is_table (sqlite, table) == 0)
      {
	  spatialite_e (".remdupl %s: no such table\n", table);
	  return;
      }
/* extracting the column names (excluding any Primary Key) */
    gaiaOutBufferInitialize (&col_list);
    xname = gaiaDoubleQuotedSql (table);
    sql = sqlite3_mprintf ("PRAGMA table_info(\"%s\")", xname);
    free (xname);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQLite SQL error: %s\n", errMsg);
	  sqlite3_free (errMsg);
	  return;
      }
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		sql = results[(i * columns) + 1];
		pk = atoi (results[(i * columns) + 5]);
		if (!pk)
		  {
		      if (first)
			  first = 0;
		      else
			  gaiaAppendToOutBuffer (&col_list, ", ");
		      xname = gaiaDoubleQuotedSql (sql);
		      sql = sqlite3_mprintf ("\"%s\"", xname);
		      free (xname);
		      gaiaAppendToOutBuffer (&col_list, sql);
		      add_to_dupl_row (&value_list, sql);
		      sqlite3_free (sql);
		  }
	    }
      }
    sqlite3_free_table (results);
/* preparing the SQL statement (identifying duplicated rows) */
    gaiaOutBufferInitialize (&sql_statement);
    gaiaAppendToOutBuffer (&sql_statement,
			   "SELECT Count(*) AS \"[dupl-count]\", ");
    if (col_list.Error == 0 && col_list.Buffer != NULL)
	gaiaAppendToOutBuffer (&sql_statement, col_list.Buffer);
    xname = gaiaDoubleQuotedSql (table);
    sql = sqlite3_mprintf ("\nFROM \"%s\"\nGROUP BY ", xname);
    free (xname);
    gaiaAppendToOutBuffer (&sql_statement, sql);
    sqlite3_free (sql);
    if (col_list.Error == 0 && col_list.Buffer != NULL)
	gaiaAppendToOutBuffer (&sql_statement, col_list.Buffer);
    gaiaOutBufferReset (&col_list);
    gaiaAppendToOutBuffer (&sql_statement, "\nHAVING \"[dupl-count]\" > 1");
/* preparing the SQL statement [delete] */
    xname = gaiaDoubleQuotedSql (table);
    sql2 = sqlite3_mprintf ("DELETE FROM \"%s\" WHERE ROWID = ?", xname);
    free (xname);

    if (sql_statement.Error == 0 && sql_statement.Buffer != NULL)
	sql = sql_statement.Buffer;
    else
	sql = "NULL-SELECT";
    if (do_delete_duplicates (sqlite, sql, sql2, &value_list, &count))
      {
	  if (!count)
	      spatialite_e ("No duplicated rows have been identified\n");
	  else
	      spatialite_e ("%d duplicated rows deleted from: %s\n", count,
			    table);
      }
    gaiaOutBufferReset (&sql_statement);
    sqlite3_free (sql2);
    clean_dupl_row (&value_list);
}

static int
check_elementary (sqlite3 * sqlite, const char *inTable, const char *geom,
		  const char *outTable, const char *pKey, const char *multiID,
		  char *type, int *srid, char *coordDims)
{
/* preliminary check for ELEMENTARY GEOMETRIES */
    char *sql;
    char *xtable;
    int ret;
    char **results;
    int rows;
    int columns;
    char *errMsg = NULL;
    int ok = 0;
    int i;
    char *gtp;
    char *dims;
    int metadata_version = checkSpatialMetaData (sqlite);

/* fetching metadata */
    if (metadata_version == 3)
      {
	  /* current metadata style >= v.4.0.0 */
	  sql = sqlite3_mprintf ("SELECT geometry_type, srid "
				 "FROM geometry_columns WHERE Lower(f_table_name) = Lower(%Q)",
				 inTable);
      }
    else
      {
	  /* legacy metadata style <= v.3.1.0 */
	  sql = sqlite3_mprintf ("SELECT type, coord_dimension, srid "
				 "FROM geometry_columns WHERE Lower(f_table_name) = Lower(%Q)"
				 "') AND Lower(f_geometry_column) = Lower(%Q)",
				 inTable, geom);
      }
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", errMsg);
	  sqlite3_free (errMsg);
	  return 0;
      }
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		if (metadata_version == 3)
		  {
		      /* current metadata style >= v.4.0.0 */
		      gtp = "UNKNOWN";
		      dims = "UNKNOWN";
		      switch (atoi (results[(i * columns) + 0]))
			{
			case 0:
			    gtp = "GEOMETRY";
			    dims = "XY";
			    break;
			case 1:
			    gtp = "POINT";
			    dims = "XY";
			    break;
			case 2:
			    gtp = "LINESTRING";
			    dims = "XY";
			    break;
			case 3:
			    gtp = "POLYGON";
			    dims = "XY";
			    break;
			case 4:
			    gtp = "MULTIPOINT";
			    dims = "XY";
			    break;
			case 5:
			    gtp = "MULTILINESTRING";
			    dims = "XY";
			    break;
			case 6:
			    gtp = "MULTIPOLYGON";
			    dims = "XY";
			    break;
			case 7:
			    gtp = "GEOMETRYCOLLECTION";
			    dims = "XY";
			    break;
			case 1000:
			    gtp = "GEOMETRY";
			    dims = "XYZ";
			    break;
			case 1001:
			    gtp = "POINT";
			    dims = "XYZ";
			    break;
			case 1002:
			    gtp = "LINESTRING";
			    dims = "XYZ";
			    break;
			case 1003:
			    gtp = "POLYGON";
			    dims = "XYZ";
			    break;
			case 1004:
			    gtp = "MULTIPOINT";
			    dims = "XYZ";
			    break;
			case 1005:
			    gtp = "MULTILINESTRING";
			    dims = "XYZ";
			    break;
			case 1006:
			    gtp = "MULTIPOLYGON";
			    dims = "XYZ";
			    break;
			case 1007:
			    gtp = "GEOMETRYCOLLECTION";
			    dims = "XYZ";
			    break;
			case 2000:
			    gtp = "GEOMETRY";
			    dims = "XYM";
			    break;
			case 2001:
			    gtp = "POINT";
			    dims = "XYM";
			    break;
			case 2002:
			    gtp = "LINESTRING";
			    dims = "XYM";
			    break;
			case 2003:
			    gtp = "POLYGON";
			    dims = "XYM";
			    break;
			case 2004:
			    gtp = "MULTIPOINT";
			    dims = "XYM";
			    break;
			case 2005:
			    gtp = "MULTILINESTRING";
			    dims = "XYM";
			    break;
			case 2006:
			    gtp = "MULTIPOLYGON";
			    dims = "XYM";
			    break;
			case 2007:
			    gtp = "GEOMETRYCOLLECTION";
			    dims = "XYM";
			    break;
			case 3000:
			    gtp = "GEOMETRY";
			    dims = "XYZM";
			    break;
			case 3001:
			    gtp = "POINT";
			    dims = "XYZM";
			    break;
			case 3002:
			    gtp = "LINESTRING";
			    dims = "XYZM";
			    break;
			case 3003:
			    gtp = "POLYGON";
			    dims = "XYZM";
			    break;
			case 3004:
			    gtp = "MULTIPOINT";
			    dims = "XYZM";
			    break;
			case 3005:
			    gtp = "MULTILINESTRING";
			    dims = "XYZM";
			    break;
			case 3006:
			    gtp = "MULTIPOLYGON";
			    dims = "XYZM";
			    break;
			case 3007:
			    gtp = "GEOMETRYCOLLECTION";
			    dims = "XYZM";
			    break;
			};
		      *srid = atoi (results[(i * columns) + 1]);
		  }
		else
		  {
		      /* legacy metadata style <= v.3.1.0 */
		      gtp = results[(i * columns) + 0];
		      dims = results[(i * columns) + 1];
		      *srid = atoi (results[(i * columns) + 2]);
		  }
		if (strcasecmp (gtp, "POINT") == 0
		    || strcasecmp (gtp, "MULTIPOINT") == 0)
		    strcpy (type, "POINT");
		else if (strcasecmp (gtp, "LINESTRING") == 0
			 || strcasecmp (gtp, "MULTILINESTRING") == 0)
		    strcpy (type, "LINESTRING");
		else if (strcasecmp (gtp, "POLYGON") == 0
			 || strcasecmp (gtp, "MULTIPOLYGON") == 0)
		    strcpy (type, "POLYGON");
		else
		    strcpy (type, "GEOMETRY");
		strcpy (coordDims, dims);
		ok = 1;
	    }
      }
    sqlite3_free_table (results);
    if (!ok)
	return 0;

/* checking if PrimaryKey already exists */
    xtable = gaiaDoubleQuotedSql (inTable);
    sql = sqlite3_mprintf ("PRAGMA table_info(\"%s\")", xtable);
    free (xtable);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", errMsg);
	  sqlite3_free (errMsg);
	  return 0;
      }
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		if (strcasecmp (pKey, results[(i * columns) + 1]) == 0)
		    ok = 0;
	    }
      }
    sqlite3_free_table (results);
    if (!ok)
	return 0;

/* checking if MultiID already exists */
    xtable = gaiaDoubleQuotedSql (inTable);
    sql = sqlite3_mprintf ("PRAGMA table_info(\"%s\")", xtable);
    free (xtable);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", errMsg);
	  sqlite3_free (errMsg);
	  return 0;
      }
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		if (strcasecmp (multiID, results[(i * columns) + 1]) == 0)
		    ok = 0;
	    }
      }
    sqlite3_free_table (results);
    if (!ok)
	return 0;

/* cheching if Output Table already exists */
    sql = sqlite3_mprintf ("SELECT Count(*) FROM sqlite_master "
			   "WHERE type = 'table' AND Lower(tbl_name) = Lower(%Q)",
			   outTable);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", errMsg);
	  sqlite3_free (errMsg);
	  return 0;
      }
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		if (atoi (results[(i * columns) + 0]) != 0)
		    ok = 0;
	    }
      }
    sqlite3_free_table (results);

    return ok;
}

static gaiaGeomCollPtr
elemGeomFromPoint (gaiaPointPtr pt, int srid)
{
/* creating a Geometry containing a single Point */
    gaiaGeomCollPtr g = NULL;
    switch (pt->DimensionModel)
      {
      case GAIA_XY_Z_M:
	  g = gaiaAllocGeomCollXYZM ();
	  break;
      case GAIA_XY_Z:
	  g = gaiaAllocGeomCollXYZ ();
	  break;
      case GAIA_XY_M:
	  g = gaiaAllocGeomCollXYM ();
	  break;
      default:
	  g = gaiaAllocGeomColl ();
	  break;
      };
    if (!g)
	return NULL;
    g->Srid = srid;
    g->DeclaredType = GAIA_POINT;
    switch (pt->DimensionModel)
      {
      case GAIA_XY_Z_M:
	  gaiaAddPointToGeomCollXYZM (g, pt->X, pt->Y, pt->Z, pt->M);
	  break;
      case GAIA_XY_Z:
	  gaiaAddPointToGeomCollXYZ (g, pt->X, pt->Y, pt->Z);
	  break;
      case GAIA_XY_M:
	  gaiaAddPointToGeomCollXYM (g, pt->X, pt->Y, pt->M);
	  break;
      default:
	  gaiaAddPointToGeomColl (g, pt->X, pt->Y);
	  break;
      };
    return g;
}

static gaiaGeomCollPtr
elemGeomFromLinestring (gaiaLinestringPtr ln, int srid)
{
/* creating a Geometry containing a single Linestring */
    gaiaGeomCollPtr g = NULL;
    gaiaLinestringPtr ln2;
    int iv;
    double x;
    double y;
    double z;
    double m;
    switch (ln->DimensionModel)
      {
      case GAIA_XY_Z_M:
	  g = gaiaAllocGeomCollXYZM ();
	  break;
      case GAIA_XY_Z:
	  g = gaiaAllocGeomCollXYZ ();
	  break;
      case GAIA_XY_M:
	  g = gaiaAllocGeomCollXYM ();
	  break;
      default:
	  g = gaiaAllocGeomColl ();
	  break;
      };
    if (!g)
	return NULL;
    g->Srid = srid;
    g->DeclaredType = GAIA_LINESTRING;
    ln2 = gaiaAddLinestringToGeomColl (g, ln->Points);
    switch (ln->DimensionModel)
      {
      case GAIA_XY_Z_M:
	  for (iv = 0; iv < ln->Points; iv++)
	    {
		gaiaGetPointXYZM (ln->Coords, iv, &x, &y, &z, &m);
		gaiaSetPointXYZM (ln2->Coords, iv, x, y, z, m);
	    }
	  break;
      case GAIA_XY_Z:
	  for (iv = 0; iv < ln->Points; iv++)
	    {
		gaiaGetPointXYZ (ln->Coords, iv, &x, &y, &z);
		gaiaSetPointXYZ (ln2->Coords, iv, x, y, z);
	    }
	  break;
      case GAIA_XY_M:
	  for (iv = 0; iv < ln->Points; iv++)
	    {
		gaiaGetPointXYM (ln->Coords, iv, &x, &y, &m);
		gaiaSetPointXYM (ln2->Coords, iv, x, y, m);
	    }
	  break;
      default:
	  for (iv = 0; iv < ln->Points; iv++)
	    {
		gaiaGetPoint (ln->Coords, iv, &x, &y);
		gaiaSetPoint (ln2->Coords, iv, x, y);
	    }
	  break;
      };
    return g;
}

static gaiaGeomCollPtr
elemGeomFromPolygon (gaiaPolygonPtr pg, int srid)
{
/* creating a Geometry containing a single Polygon */
    gaiaGeomCollPtr g = NULL;
    gaiaPolygonPtr pg2;
    gaiaRingPtr rng;
    gaiaRingPtr rng2;
    int ib;
    int iv;
    double x;
    double y;
    double z;
    double m;
    switch (pg->DimensionModel)
      {
      case GAIA_XY_Z_M:
	  g = gaiaAllocGeomCollXYZM ();
	  break;
      case GAIA_XY_Z:
	  g = gaiaAllocGeomCollXYZ ();
	  break;
      case GAIA_XY_M:
	  g = gaiaAllocGeomCollXYM ();
	  break;
      default:
	  g = gaiaAllocGeomColl ();
	  break;
      };
    if (!g)
	return NULL;
    g->Srid = srid;
    g->DeclaredType = GAIA_POLYGON;
    rng = pg->Exterior;
    pg2 = gaiaAddPolygonToGeomColl (g, rng->Points, pg->NumInteriors);
    rng2 = pg2->Exterior;
    switch (pg->DimensionModel)
      {
      case GAIA_XY_Z_M:
	  for (iv = 0; iv < rng->Points; iv++)
	    {
		gaiaGetPointXYZM (rng->Coords, iv, &x, &y, &z, &m);
		gaiaSetPointXYZM (rng2->Coords, iv, x, y, z, m);
	    }
	  for (ib = 0; ib < pg->NumInteriors; ib++)
	    {
		rng = pg->Interiors + ib;
		rng2 = gaiaAddInteriorRing (pg2, ib, rng->Points);
		for (iv = 0; iv < rng->Points; iv++)
		  {
		      gaiaGetPointXYZM (rng->Coords, iv, &x, &y, &z, &m);
		      gaiaSetPointXYZM (rng2->Coords, iv, x, y, z, m);
		  }
	    }
	  break;
      case GAIA_XY_Z:
	  for (iv = 0; iv < rng->Points; iv++)
	    {
		gaiaGetPointXYZ (rng->Coords, iv, &x, &y, &z);
		gaiaSetPointXYZ (rng2->Coords, iv, x, y, z);
	    }
	  for (ib = 0; ib < pg->NumInteriors; ib++)
	    {
		rng = pg->Interiors + ib;
		rng2 = gaiaAddInteriorRing (pg2, ib, rng->Points);
		for (iv = 0; iv < rng->Points; iv++)
		  {
		      gaiaGetPointXYZ (rng->Coords, iv, &x, &y, &z);
		      gaiaSetPointXYZ (rng2->Coords, iv, x, y, z);
		  }
	    }
	  break;
      case GAIA_XY_M:
	  for (iv = 0; iv < rng->Points; iv++)
	    {
		gaiaGetPointXYM (rng->Coords, iv, &x, &y, &m);
		gaiaSetPointXYM (rng2->Coords, iv, x, y, m);
	    }
	  for (ib = 0; ib < pg->NumInteriors; ib++)
	    {
		rng = pg->Interiors + ib;
		rng2 = gaiaAddInteriorRing (pg2, ib, rng->Points);
		for (iv = 0; iv < rng->Points; iv++)
		  {
		      gaiaGetPointXYM (rng->Coords, iv, &x, &y, &m);
		      gaiaSetPointXYM (rng2->Coords, iv, x, y, m);
		  }
	    }
	  break;
      default:
	  for (iv = 0; iv < rng->Points; iv++)
	    {
		gaiaGetPoint (rng->Coords, iv, &x, &y);
		gaiaSetPoint (rng2->Coords, iv, x, y);
	    }
	  for (ib = 0; ib < pg->NumInteriors; ib++)
	    {
		rng = pg->Interiors + ib;
		rng2 = gaiaAddInteriorRing (pg2, ib, rng->Points);
		for (iv = 0; iv < rng->Points; iv++)
		  {
		      gaiaGetPoint (rng->Coords, iv, &x, &y);
		      gaiaSetPoint (rng2->Coords, iv, x, y);
		  }
	    }
	  break;
      };
    return g;
}

SPATIALITE_DECLARE void
elementary_geometries (sqlite3 * sqlite,
		       char *inTable, char *geometry, char *outTable,
		       char *pKey, char *multiId)
{
/* attempting to create a derived table surely containing elemetary Geoms */
    char type[128];
    int srid;
    char dims[64];
    char *sql;
    char *xname;
    char *xpk;
    char *xmulti;
    gaiaOutBuffer sql_statement;
    gaiaOutBuffer sql2;
    gaiaOutBuffer sql3;
    gaiaOutBuffer sql4;
    char *sql_geom;
    int ret;
    int comma = 0;
    char *errMsg = NULL;
    int i;
    char **results;
    int rows;
    int columns;
    int geom_idx = -1;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;
    int n_columns;
    sqlite3_int64 id = 0;

    if (check_elementary
	(sqlite, inTable, geometry, outTable, pKey, multiId, type, &srid,
	 dims) == 0)
      {
	  spatialite_e (".elemgeo: invalid args\n");
	  return;
      }

/* starts a transaction */
    ret = sqlite3_exec (sqlite, "BEGIN", NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", errMsg);
	  sqlite3_free (errMsg);
	  goto abort;
      }

    gaiaOutBufferInitialize (&sql_statement);
    gaiaOutBufferInitialize (&sql2);
    gaiaOutBufferInitialize (&sql3);
    gaiaOutBufferInitialize (&sql4);

    gaiaAppendToOutBuffer (&sql_statement, "SELECT ");
    xname = gaiaDoubleQuotedSql (outTable);
    xpk = gaiaDoubleQuotedSql (pKey);
    xmulti = gaiaDoubleQuotedSql (multiId);
    sql =
	sqlite3_mprintf ("INSERT INTO \"%s\" (\"%s\", \"%s\", ", xname, xpk,
			 xmulti);
    free (xname);
    free (xpk);
    free (xmulti);
    gaiaAppendToOutBuffer (&sql2, sql);
    sqlite3_free (sql);
    gaiaAppendToOutBuffer (&sql3, ") VALUES (NULL, ?");
    xname = gaiaDoubleQuotedSql (outTable);
    xpk = gaiaDoubleQuotedSql (pKey);
    xmulti = gaiaDoubleQuotedSql (multiId);
    sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n"
			   "\t\"%s\" INTEGER PRIMARY KEY AUTOINCREMENT"
			   ",\n\t\"%s\" INTEGER NOT NULL", xname, xpk, xmulti);
    free (xname);
    free (xpk);
    free (xmulti);
    gaiaAppendToOutBuffer (&sql4, sql);
    sqlite3_free (sql);

    xname = gaiaDoubleQuotedSql (inTable);
    sql = sqlite3_mprintf ("PRAGMA table_info(\"%s\")", xname);
    free (xname);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", errMsg);
	  sqlite3_free (errMsg);
	  goto abort;
      }
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		xname = gaiaDoubleQuotedSql (results[(i * columns) + 1]);
		if (comma)
		    sql = sqlite3_mprintf (", \"%s\"", xname);
		else
		  {
		      comma = 1;
		      sql = sqlite3_mprintf ("\"%s\"", xname);
		  }
		free (xname);
		gaiaAppendToOutBuffer (&sql_statement, sql);
		gaiaAppendToOutBuffer (&sql2, sql);
		gaiaAppendToOutBuffer (&sql3, ", ?");
		sqlite3_free (sql);

		if (strcasecmp (geometry, results[(i * columns) + 1]) == 0)
		    geom_idx = i - 1;
		else
		  {
		      xname = gaiaDoubleQuotedSql (results[(i * columns) + 1]);
		      if (atoi (results[(i * columns) + 3]) != 0)
			  sql =
			      sqlite3_mprintf (",\n\t\"%s\" %s NOT NULL", xname,
					       results[(i * columns) + 2]);
		      else
			  sql =
			      sqlite3_mprintf (",\n\t\"%s\" %s", xname,
					       results[(i * columns) + 2]);
		      free (xname);
		      gaiaAppendToOutBuffer (&sql4, sql);
		      sqlite3_free (sql);
		  }
	    }
      }
    sqlite3_free_table (results);
    if (geom_idx < 0)
	goto abort;

    xname = gaiaDoubleQuotedSql (inTable);
    sql = sqlite3_mprintf (" FROM \"%s\"", xname);
    free (xname);
    gaiaAppendToOutBuffer (&sql_statement, sql);
    sqlite3_free (sql);
    gaiaAppendToOutBuffer (&sql2, sql3.Buffer);
    gaiaAppendToOutBuffer (&sql2, ")");
    gaiaAppendToOutBuffer (&sql4, ")");
    gaiaOutBufferReset (&sql3);

    sql_geom =
	sqlite3_mprintf ("SELECT AddGeometryColumn(%Q, %Q, %d, %Q, %Q)",
			 outTable, geometry, srid, type, dims);

/* creating the output table */
    ret = sqlite3_exec (sqlite, sql4.Buffer, NULL, NULL, &errMsg);
    gaiaOutBufferReset (&sql4);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", errMsg);
	  sqlite3_free (errMsg);
	  goto abort;
      }
/* creating the output Geometry */
    ret = sqlite3_exec (sqlite, sql_geom, NULL, NULL, &errMsg);
    sqlite3_free (sql_geom);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", errMsg);
	  sqlite3_free (errMsg);
	  goto abort;
      }

/* preparing the INPUT statement */
    ret =
	sqlite3_prepare_v2 (sqlite, sql_statement.Buffer,
			    strlen (sql_statement.Buffer), &stmt_in, NULL);
    gaiaOutBufferReset (&sql_statement);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", sqlite3_errmsg (sqlite));
	  goto abort;
      }

/* preparing the OUTPUT statement */
    ret =
	sqlite3_prepare_v2 (sqlite, sql2.Buffer, strlen (sql2.Buffer),
			    &stmt_out, NULL);
    gaiaOutBufferReset (&sql2);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", sqlite3_errmsg (sqlite));
	  goto abort;
      }

/* data transfer */
    n_columns = sqlite3_column_count (stmt_in);
    while (1)
      {
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;
	  if (ret == SQLITE_ROW)
	    {
		gaiaGeomCollPtr g =
		    gaiaFromSpatiaLiteBlobWkb ((const unsigned char *)
					       sqlite3_column_blob (stmt_in,
								    geom_idx),
					       sqlite3_column_bytes (stmt_in,
								     geom_idx));
		if (!g)
		  {
		      /* NULL input geometry */
		      sqlite3_reset (stmt_out);
		      sqlite3_clear_bindings (stmt_out);
		      sqlite3_bind_int64 (stmt_out, 1, id);
		      sqlite3_bind_null (stmt_out, geom_idx + 2);

		      for (i = 0; i < n_columns; i++)
			{
			    int type = sqlite3_column_type (stmt_in, i);
			    if (i == geom_idx)
				continue;
			    switch (type)
			      {
			      case SQLITE_INTEGER:
				  sqlite3_bind_int64 (stmt_out, i + 2,
						      sqlite3_column_int
						      (stmt_in, i));
				  break;
			      case SQLITE_FLOAT:
				  sqlite3_bind_double (stmt_out, i + 2,
						       sqlite3_column_double
						       (stmt_in, i));
				  break;
			      case SQLITE_TEXT:
				  sqlite3_bind_text (stmt_out, i + 2,
						     (const char *)
						     sqlite3_column_text
						     (stmt_in, i),
						     sqlite3_column_bytes
						     (stmt_in, i),
						     SQLITE_STATIC);
				  break;
			      case SQLITE_BLOB:
				  sqlite3_bind_blob (stmt_out, i + 2,
						     sqlite3_column_blob
						     (stmt_in, i),
						     sqlite3_column_bytes
						     (stmt_in, i),
						     SQLITE_STATIC);
				  break;
			      case SQLITE_NULL:
			      default:
				  sqlite3_bind_null (stmt_out, i + 2);
				  break;
			      };
			}

		      ret = sqlite3_step (stmt_out);
		      if (ret == SQLITE_DONE || ret == SQLITE_ROW)
			  ;
		      else
			{
			    spatialite_e ("[OUT]step error: %s\n",
					  sqlite3_errmsg (sqlite));
			    goto abort;
			}
		  }
		else
		  {
		      /* separating Elementary Geoms */
		      gaiaPointPtr pt;
		      gaiaLinestringPtr ln;
		      gaiaPolygonPtr pg;
		      gaiaGeomCollPtr outGeom;
		      pt = g->FirstPoint;
		      while (pt)
			{
			    /* separating Points */
			    outGeom = elemGeomFromPoint (pt, g->Srid);
			    sqlite3_reset (stmt_out);
			    sqlite3_clear_bindings (stmt_out);
			    sqlite3_bind_int64 (stmt_out, 1, id);
			    if (!outGeom)
				sqlite3_bind_null (stmt_out, geom_idx + 2);
			    else
			      {
				  unsigned char *blob;
				  int size;
				  gaiaToSpatiaLiteBlobWkb (outGeom, &blob,
							   &size);
				  sqlite3_bind_blob (stmt_out, geom_idx + 2,
						     blob, size, free);
				  gaiaFreeGeomColl (outGeom);
			      }

			    for (i = 0; i < n_columns; i++)
			      {
				  int type = sqlite3_column_type (stmt_in, i);
				  if (i == geom_idx)
				      continue;
				  switch (type)
				    {
				    case SQLITE_INTEGER:
					sqlite3_bind_int64 (stmt_out, i + 2,
							    sqlite3_column_int
							    (stmt_in, i));
					break;
				    case SQLITE_FLOAT:
					sqlite3_bind_double (stmt_out, i + 2,
							     sqlite3_column_double
							     (stmt_in, i));
					break;
				    case SQLITE_TEXT:
					sqlite3_bind_text (stmt_out, i + 2,
							   (const char *)
							   sqlite3_column_text
							   (stmt_in, i),
							   sqlite3_column_bytes
							   (stmt_in, i),
							   SQLITE_STATIC);
					break;
				    case SQLITE_BLOB:
					sqlite3_bind_blob (stmt_out, i + 2,
							   sqlite3_column_blob
							   (stmt_in, i),
							   sqlite3_column_bytes
							   (stmt_in, i),
							   SQLITE_STATIC);
					break;
				    case SQLITE_NULL:
				    default:
					sqlite3_bind_null (stmt_out, i + 2);
					break;
				    };
			      }

			    ret = sqlite3_step (stmt_out);
			    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
				;
			    else
			      {
				  spatialite_e ("[OUT]step error: %s\n",
						sqlite3_errmsg (sqlite));
				  goto abort;
			      }
			    pt = pt->Next;
			}
		      ln = g->FirstLinestring;
		      while (ln)
			{
			    /* separating Linestrings */
			    outGeom = elemGeomFromLinestring (ln, g->Srid);
			    sqlite3_reset (stmt_out);
			    sqlite3_clear_bindings (stmt_out);
			    sqlite3_bind_int64 (stmt_out, 1, id);
			    if (!outGeom)
				sqlite3_bind_null (stmt_out, geom_idx + 2);
			    else
			      {
				  unsigned char *blob;
				  int size;
				  gaiaToSpatiaLiteBlobWkb (outGeom, &blob,
							   &size);
				  sqlite3_bind_blob (stmt_out, geom_idx + 2,
						     blob, size, free);
				  gaiaFreeGeomColl (outGeom);
			      }

			    for (i = 0; i < n_columns; i++)
			      {
				  int type = sqlite3_column_type (stmt_in, i);
				  if (i == geom_idx)
				      continue;
				  switch (type)
				    {
				    case SQLITE_INTEGER:
					sqlite3_bind_int64 (stmt_out, i + 2,
							    sqlite3_column_int
							    (stmt_in, i));
					break;
				    case SQLITE_FLOAT:
					sqlite3_bind_double (stmt_out, i + 2,
							     sqlite3_column_double
							     (stmt_in, i));
					break;
				    case SQLITE_TEXT:
					sqlite3_bind_text (stmt_out, i + 2,
							   (const char *)
							   sqlite3_column_text
							   (stmt_in, i),
							   sqlite3_column_bytes
							   (stmt_in, i),
							   SQLITE_STATIC);
					break;
				    case SQLITE_BLOB:
					sqlite3_bind_blob (stmt_out, i + 2,
							   sqlite3_column_blob
							   (stmt_in, i),
							   sqlite3_column_bytes
							   (stmt_in, i),
							   SQLITE_STATIC);
					break;
				    case SQLITE_NULL:
				    default:
					sqlite3_bind_null (stmt_out, i + 2);
					break;
				    };
			      }

			    ret = sqlite3_step (stmt_out);
			    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
				;
			    else
			      {
				  spatialite_e ("[OUT]step error: %s\n",
						sqlite3_errmsg (sqlite));
				  goto abort;
			      }
			    ln = ln->Next;
			}
		      pg = g->FirstPolygon;
		      while (pg)
			{
			    /* separating Polygons */
			    outGeom = elemGeomFromPolygon (pg, g->Srid);
			    sqlite3_reset (stmt_out);
			    sqlite3_clear_bindings (stmt_out);
			    sqlite3_bind_int64 (stmt_out, 1, id);
			    if (!outGeom)
				sqlite3_bind_null (stmt_out, geom_idx + 2);
			    else
			      {
				  unsigned char *blob;
				  int size;
				  gaiaToSpatiaLiteBlobWkb (outGeom, &blob,
							   &size);
				  sqlite3_bind_blob (stmt_out, geom_idx + 2,
						     blob, size, free);
				  gaiaFreeGeomColl (outGeom);
			      }

			    for (i = 0; i < n_columns; i++)
			      {
				  int type = sqlite3_column_type (stmt_in, i);
				  if (i == geom_idx)
				      continue;
				  switch (type)
				    {
				    case SQLITE_INTEGER:
					sqlite3_bind_int64 (stmt_out, i + 2,
							    sqlite3_column_int
							    (stmt_in, i));
					break;
				    case SQLITE_FLOAT:
					sqlite3_bind_double (stmt_out, i + 2,
							     sqlite3_column_double
							     (stmt_in, i));
					break;
				    case SQLITE_TEXT:
					sqlite3_bind_text (stmt_out, i + 2,
							   (const char *)
							   sqlite3_column_text
							   (stmt_in, i),
							   sqlite3_column_bytes
							   (stmt_in, i),
							   SQLITE_STATIC);
					break;
				    case SQLITE_BLOB:
					sqlite3_bind_blob (stmt_out, i + 2,
							   sqlite3_column_blob
							   (stmt_in, i),
							   sqlite3_column_bytes
							   (stmt_in, i),
							   SQLITE_STATIC);
					break;
				    case SQLITE_NULL:
				    default:
					sqlite3_bind_null (stmt_out, i + 2);
					break;
				    };
			      }

			    ret = sqlite3_step (stmt_out);
			    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
				;
			    else
			      {
				  spatialite_e ("[OUT]step error: %s\n",
						sqlite3_errmsg (sqlite));
				  goto abort;
			      }
			    pg = pg->Next;
			}
		      gaiaFreeGeomColl (g);
		  }
		id++;
	    }
	  else
	    {
		spatialite_e ("[IN]step error: %s\n", sqlite3_errmsg (sqlite));
		goto abort;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);

/* commits the transaction */
    ret = sqlite3_exec (sqlite, "COMMIT", NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", errMsg);
	  sqlite3_free (errMsg);
	  goto abort;
      }
    return;

  abort:
    if (stmt_in)
	sqlite3_finalize (stmt_in);
    if (stmt_out)
	sqlite3_finalize (stmt_out);
}

#ifndef OMIT_FREEXL		/* including FreeXL */

SPATIALITE_DECLARE int
load_XL (sqlite3 * sqlite, const char *path, const char *table,
	 unsigned int worksheetIndex, int first_titles, unsigned int *rows,
	 char *err_msg)
{
/* loading an XL spreadsheet as a new DB table */
    sqlite3_stmt *stmt;
    unsigned int current_row;
    int ret;
    char *errMsg = NULL;
    char *xname;
    char *dummy;
    char *xdummy;
    char *sql;
    int sqlError = 0;
    const void *xl_handle;
    unsigned int info;
    unsigned short columns;
    unsigned short col;
    gaiaOutBuffer sql_statement;
    FreeXL_CellValue cell;
    int already_exists = 0;
/* checking if TABLE already exists */
    sql =
	sqlite3_mprintf ("SELECT name FROM sqlite_master WHERE type = 'table' "
			 "AND Lower(name) = Lower(%Q)", table);
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  if (!err_msg)
	      spatialite_e ("load XL error: <%s>\n", sqlite3_errmsg (sqlite));
	  else
	      sprintf (err_msg, "load XL error: <%s>\n",
		       sqlite3_errmsg (sqlite));
	  return 0;
      }
    while (1)
      {
	  /* scrolling the result set */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	      already_exists = 1;
	  else
	    {
		spatialite_e ("load XL error: <%s>\n", sqlite3_errmsg (sqlite));
		break;
	    }
      }
    sqlite3_finalize (stmt);
    if (already_exists)
      {
	  if (!err_msg)
	      spatialite_e ("load XL error: table '%s' already exists\n",
			    table);
	  else
	      sprintf (err_msg, "load XL error: table '%s' already exists\n",
		       table);
	  return 0;
      }
/* opening the .XLS file [Workbook] */
    ret = freexl_open (path, &xl_handle);
    if (ret != FREEXL_OK)
	goto error;
/* checking if Password protected */
    ret = freexl_get_info (xl_handle, FREEXL_BIFF_PASSWORD, &info);
    if (ret != FREEXL_OK)
	goto error;
    if (info != FREEXL_BIFF_PLAIN)
	goto error;
/* Worksheet entries */
    ret = freexl_get_info (xl_handle, FREEXL_BIFF_SHEET_COUNT, &info);
    if (ret != FREEXL_OK)
	goto error;
    if (info == 0)
	goto error;
    if (worksheetIndex < info)
	;
    else
	goto error;
    ret = freexl_select_active_worksheet (xl_handle, worksheetIndex);
    if (ret != FREEXL_OK)
	goto error;
    ret = freexl_worksheet_dimensions (xl_handle, rows, &columns);
    if (ret != FREEXL_OK)
	goto error;
/* starting a transaction */
    ret = sqlite3_exec (sqlite, "BEGIN", NULL, 0, &errMsg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("load XL error: %s\n", errMsg);
	  sqlite3_free (errMsg);
	  sqlError = 1;
	  goto clean_up;
      }
/* creating the Table */
    gaiaOutBufferInitialize (&sql_statement);
    xname = gaiaDoubleQuotedSql (table);
    sql = sqlite3_mprintf ("CREATE TABLE \"%s\"", xname);
    free (xname);
    gaiaAppendToOutBuffer (&sql_statement, sql);
    sqlite3_free (sql);
    gaiaAppendToOutBuffer (&sql_statement,
			   " (\nPK_UID INTEGER PRIMARY KEY AUTOINCREMENT");
    for (col = 0; col < columns; col++)
      {
	  if (first_titles)
	    {
		/* fetching column names */
		for (col = 0; col < columns; col++)
		  {
		      ret = freexl_get_cell_value (xl_handle, 0, col, &cell);
		      if (ret != FREEXL_OK)
			  dummy = sqlite3_mprintf ("col_%d", col);
		      else
			{
			    if (cell.type == FREEXL_CELL_INT)
				dummy =
				    sqlite3_mprintf ("%d",
						     cell.value.int_value);
			    else if (cell.type == FREEXL_CELL_DOUBLE)
				dummy = sqlite3_mprintf ("%1.2f ",
							 cell.
							 value.double_value);
			    else if (cell.type == FREEXL_CELL_TEXT
				     || cell.type == FREEXL_CELL_SST_TEXT
				     || cell.type == FREEXL_CELL_DATE
				     || cell.type == FREEXL_CELL_DATETIME
				     || cell.type == FREEXL_CELL_TIME)
			      {
				  int len = strlen (cell.value.text_value);
				  if (len < 256)
				      dummy =
					  sqlite3_mprintf ("%s",
							   cell.
							   value.text_value);
				  else
				      dummy = sqlite3_mprintf ("col_%d", col);
			      }
			    else
				dummy = sqlite3_mprintf ("col_%d", col);
			}
		      xdummy = gaiaDoubleQuotedSql (dummy);
		      sqlite3_free (dummy);
		      sql = sqlite3_mprintf (", \"%s\"", xdummy);
		      free (xdummy);
		      gaiaAppendToOutBuffer (&sql_statement, sql);
		      sqlite3_free (sql);
		  }
	    }
	  else
	    {
		/* setting default column names */
		for (col = 0; col < columns; col++)
		  {
		      dummy = sqlite3_mprintf ("col_%d", col);
		      xdummy = gaiaDoubleQuotedSql (dummy);
		      sqlite3_free (dummy);
		      sql = sqlite3_mprintf (", \"%s\"", xdummy);
		      free (xdummy);
		      gaiaAppendToOutBuffer (&sql_statement, sql);
		      sqlite3_free (sql);
		  }
	    }
      }
    gaiaAppendToOutBuffer (&sql_statement, ")");
    if (sql_statement.Error == 0 && sql_statement.Buffer != NULL)
      {
	  ret = sqlite3_exec (sqlite, sql_statement.Buffer, NULL, 0, &errMsg);
	  gaiaOutBufferReset (&sql_statement);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("load XL error: %s\n", errMsg);
		sqlite3_free (errMsg);
		sqlError = 1;
		goto clean_up;
	    }
      }
/* preparing the INSERT INTO parameterized statement */
    gaiaOutBufferReset (&sql_statement);
    xname = gaiaDoubleQuotedSql (table);
    sql = sqlite3_mprintf ("INSERT INTO \"%s\" (PK_UID", xname);
    free (xname);
    gaiaAppendToOutBuffer (&sql_statement, sql);
    sqlite3_free (sql);
    for (col = 0; col < columns; col++)
      {
	  if (first_titles)
	    {
		ret = freexl_get_cell_value (xl_handle, 0, col, &cell);
		if (ret != FREEXL_OK)
		    dummy = sqlite3_mprintf ("col_%d", col);
		else
		  {
		      if (cell.type == FREEXL_CELL_INT)
			  dummy = sqlite3_mprintf ("%d", cell.value.int_value);
		      else if (cell.type == FREEXL_CELL_DOUBLE)
			  dummy =
			      sqlite3_mprintf ("%1.2f",
					       cell.value.double_value);
		      else if (cell.type == FREEXL_CELL_TEXT
			       || cell.type == FREEXL_CELL_SST_TEXT
			       || cell.type == FREEXL_CELL_DATE
			       || cell.type == FREEXL_CELL_DATETIME
			       || cell.type == FREEXL_CELL_TIME)
			{
			    int len = strlen (cell.value.text_value);
			    if (len < 256)
				dummy =
				    sqlite3_mprintf ("%s",
						     cell.value.text_value);
			    else
				dummy = sqlite3_mprintf ("col_%d", col);
			}
		      else
			  dummy = sqlite3_mprintf ("col_%d", col);
		  }
		xdummy = gaiaDoubleQuotedSql (dummy);
		sqlite3_free (dummy);
		sql = sqlite3_mprintf (", \"%s\"", xdummy);
		free (xdummy);
		gaiaAppendToOutBuffer (&sql_statement, sql);
		sqlite3_free (sql);
	    }
	  else
	    {
		/* setting default column names  */
		dummy = sqlite3_mprintf ("col_%d", col);
		xdummy = gaiaDoubleQuotedSql (dummy);
		sqlite3_free (dummy);
		sql = sqlite3_mprintf (", \"%s\"", xdummy);
		free (xdummy);
		gaiaAppendToOutBuffer (&sql_statement, sql);
		sqlite3_free (sql);
	    }
      }
    gaiaAppendToOutBuffer (&sql_statement, ")\nVALUES (NULL");
    for (col = 0; col < columns; col++)
      {
	  /* column values */
	  gaiaAppendToOutBuffer (&sql_statement, ", ?");
      }
    gaiaAppendToOutBuffer (&sql_statement, ")");
    if (sql_statement.Error == 0 && sql_statement.Buffer != NULL)
      {
	  ret =
	      sqlite3_prepare_v2 (sqlite, sql_statement.Buffer,
				  strlen (sql_statement.Buffer), &stmt, NULL);
	  gaiaOutBufferReset (&sql_statement);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("load XL error: %s\n", sqlite3_errmsg (sqlite));
		sqlError = 1;
		goto clean_up;
	    }
      }
    if (first_titles)
	current_row = 1;
    else
	current_row = 0;
    while (current_row < *rows)
      {
	  /* binding query params */
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  for (col = 0; col < columns; col++)
	    {
		/* column values */
		ret =
		    freexl_get_cell_value (xl_handle, current_row, col, &cell);
		if (ret != FREEXL_OK)
		    sqlite3_bind_null (stmt, col + 1);
		else
		  {
		      switch (cell.type)
			{
			case FREEXL_CELL_INT:
			    sqlite3_bind_int (stmt, col + 1,
					      cell.value.int_value);
			    break;
			case FREEXL_CELL_DOUBLE:
			    sqlite3_bind_double (stmt, col + 1,
						 cell.value.double_value);
			    break;
			case FREEXL_CELL_TEXT:
			case FREEXL_CELL_SST_TEXT:
			case FREEXL_CELL_DATE:
			case FREEXL_CELL_DATETIME:
			case FREEXL_CELL_TIME:
			    sqlite3_bind_text (stmt, col + 1,
					       cell.value.text_value,
					       strlen (cell.value.text_value),
					       SQLITE_STATIC);
			    break;
			default:
			    sqlite3_bind_null (stmt, col + 1);
			    break;
			};
		  }
	    }
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		spatialite_e ("load XL error: %s\n", sqlite3_errmsg (sqlite));
		sqlite3_finalize (stmt);
		sqlError = 1;
		goto clean_up;
	    }
	  current_row++;
      }
    sqlite3_finalize (stmt);
  clean_up:
    if (sqlError)
      {
	  /* some error occurred - ROLLBACK */
	  ret = sqlite3_exec (sqlite, "ROLLBACK", NULL, 0, &errMsg);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("load XL error: %s\n", errMsg);
		sqlite3_free (errMsg);
	    }
	  spatialite_e
	      ("XL not loaded\n\n\na ROLLBACK was automatically performed\n");
      }
    else
      {
	  /* ok - confirming pending transaction - COMMIT */
	  ret = sqlite3_exec (sqlite, "COMMIT", NULL, 0, &errMsg);
	  if (ret != SQLITE_OK)
	    {
		if (!err_msg)
		    spatialite_e ("load XL error: %s\n", errMsg);
		else
		    sprintf (err_msg, "load XL error: %s\n", errMsg);
		sqlite3_free (errMsg);
		return 0;
	    }
	  if (first_titles)
	      *rows = *rows - 1;	/* allow for header row */
	  spatialite_e ("XL loaded\n\n%d inserted rows\n", *rows);
      }
    freexl_close (xl_handle);
    return 1;

  error:
    freexl_close (xl_handle);
    if (!err_msg)
	spatialite_e ("XL datasource '%s' is not valid\n", path);
    else
	sprintf (err_msg, "XL datasource '%s' is not valid\n", path);
    return 0;
}

#endif /* FreeXL enabled/disabled */

SPATIALITE_DECLARE int
dump_geojson (sqlite3 * sqlite, char *table, char *geom_col, char *outfile_path,
	      int precision, int option)
{
/* dumping a  geometry table as GeoJSON - Brad Hards 2011-11-09 */
    char *sql;
    char *xgeom_col;
    char *xtable;
    sqlite3_stmt *stmt = NULL;
    FILE *out = NULL;
    int ret;
    int rows = 0;

/* opening/creating the GeoJSON output file */
    out = fopen (outfile_path, "wb");
    if (!out)
	goto no_file;

/* preparing SQL statement */
    xtable = gaiaDoubleQuotedSql (table);
    xgeom_col = gaiaDoubleQuotedSql (geom_col);
    sql =
	sqlite3_mprintf
	("SELECT AsGeoJSON(\"%s\", %d, %d) FROM \"%s\" WHERE \"%s\" IS NOT NULL",
	 xgeom_col, precision, option, xtable, xgeom_col);
    free (xtable);
    free (xgeom_col);
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto sql_error;

    while (1)
      {
	  /* scrolling the result set */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	    {
		break;		/* end of result set */
	    }
	  if (ret == SQLITE_ROW)
	    {
		rows++;
		fprintf (out, "%s\r\n", sqlite3_column_text (stmt, 0));
	    }
	  else
	    {
		goto sql_error;
	    }
      }
    if (rows == 0)
      {
	  goto empty_result_set;
      }

    sqlite3_finalize (stmt);
    fclose (out);
    return 1;

  sql_error:
/* an SQL error occurred */
    if (stmt)
      {
	  sqlite3_finalize (stmt);
      }
    if (out)
      {
	  fclose (out);
      }
    spatialite_e ("Dump GeoJSON error: %s\n", sqlite3_errmsg (sqlite));
    return 0;

  no_file:
/* Output file could not be created / opened */
    if (stmt)
      {
	  sqlite3_finalize (stmt);
      }
    if (out)
      {
	  fclose (out);
      }
    spatialite_e ("ERROR: unable to open '%s' for writing\n", outfile_path);
    return 0;

  empty_result_set:
/* the result set is empty - nothing to do */
    if (stmt)
      {
	  sqlite3_finalize (stmt);
      }
    if (out)
      {
	  fclose (out);
      }
    spatialite_e ("The SQL SELECT returned no data to export...\n");
    return 0;
}
