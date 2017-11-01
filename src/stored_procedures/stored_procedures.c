/*

 stored_procedures.c -- SpatiaLite Stored Procedures support

 version 4.5, 2017 October 22

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
 
Portions created by the Initial Developer are Copyright (C) 2017
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

#include <spatialite/sqlite.h>
#include <spatialite/gaiageo.h>
#include <spatialite/gaiaaux.h>
#include <spatialite/gg_const.h>
#include <spatialite/gg_formats.h>
#include <spatialite/stored_procedures.h>
#include <spatialite_private.h>

#ifdef _WIN32
#define strcasecmp	_stricmp
#endif /* not WIN32 */


struct sp_var_item
{
/* a variable Argument item */
    char *varname;
    short count;
    struct sp_var_item *next;
};

struct sp_var_list
{
/* a list of Variable Arguments */
    struct sp_var_item *first;
    struct sp_var_item *last;
};

static void
stored_proc_reset_error (const void *ctx)
{
/* resetting the Stored Procedures Last Error Message */
    struct splite_internal_cache *cache = (struct splite_internal_cache *) ctx;
    if (cache != NULL)
      {
	  if (cache->storedProcError != NULL)
	    {
		free (cache->storedProcError);
		cache->storedProcError = NULL;
	    }
      }
}

SPATIALITE_PRIVATE void
gaia_sql_proc_set_error (const void *ctx, const char *errmsg)
{
/* setting the Stored Procedures Last Error Message */
    struct splite_internal_cache *cache = (struct splite_internal_cache *) ctx;
    if (cache != NULL)
      {
	  int len;
	  if (cache->storedProcError != NULL)
	    {
		free (cache->storedProcError);
		cache->storedProcError = NULL;
	    }
	  if (errmsg == NULL)
	      return;

	  len = strlen (errmsg);
	  cache->storedProcError = malloc (len + 1);
	  strcpy (cache->storedProcError, errmsg);
      }
}

SQLPROC_DECLARE char *
gaia_sql_proc_get_last_error (const void *p_cache)
{
/* return the last Stored Procedures Error Message (if any) */
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;

    if (cache == NULL)
	return NULL;
    return cache->storedProcError;

}

SQLPROC_DECLARE SqlProc_VarListPtr
gaia_sql_proc_create_variables ()
{
/* allocating an empty list of Variables with Values */
    SqlProc_VarListPtr list = malloc (sizeof (SqlProc_VarList));
    if (list == NULL)
	return NULL;

    list->Error = 0;
    list->ErrMessage = NULL;
    list->First = NULL;
    list->Last = NULL;
    return list;
}

SQLPROC_DECLARE void
gaia_sql_proc_destroy_variables (SqlProc_VarListPtr list)
{
/* destroying a List of Variables with Values */
    SqlProc_VariablePtr var;
    SqlProc_VariablePtr n_var;
    if (list == NULL)
	return;

    var = list->First;
    while (var != NULL)
      {
	  n_var = var->Next;
	  if (var->Name != NULL)
	      free (var->Name);
	  if (var->Value != NULL)
	      free (var->Value);
	  free (var);
	  var = n_var;
      }
    if (list->ErrMessage != NULL)
	sqlite3_free (list->ErrMessage);
    free (list);
}

static int
parse_variable_name_value (const char *str, char **name, char **value)
{
/* attempting to parse a Variable with Value definition */
    char marker;
    int end = 0;
    int start;
    int name_len;
    int value_len;
    char *nm;
    char *val;
    int i;

    *name = NULL;
    *value = NULL;
    if (*str == '@')
	marker = '@';
    if (*str == '$')
	marker = '$';
    if (marker == '\0')
	return 0;

/* searching the closing marker - variable name */
    for (i = 1; i < (int) strlen (str); i++)
      {
	  if (str[i] == marker)
	    {
		end = i;
		break;
	    }
      }
    if (end == 0)
	return 0;

/* searching the start position of Value */
    if (end + 1 >= (int) strlen (str))
	return 0;
    if (str[end + 1] != '=')
	return 0;
    start = end + 2;

    name_len = end - 1;
    value_len = strlen (str + start);
    if (name_len == 0 || value_len == 0)
	return 0;
    nm = malloc (name_len + 1);
    memcpy (nm, str + 1, end - 1);
    *(nm + name_len) = '\0';
    val = malloc (value_len + 1);
    strcpy (val, str + start);

    *name = nm;
    *value = val;
    return 1;
}

SQLPROC_DECLARE int
gaia_sql_proc_add_variable (SqlProc_VarListPtr list, const char *str)
{
/* adding a Variable with Values to the List */
    char *name;
    char *value;
    SqlProc_VariablePtr var;
    if (list == NULL)
	return 0;

    if (!parse_variable_name_value (str, &name, &value))
      {
	  list->ErrMessage =
	      sqlite3_mprintf ("Illegal Variable with Value definition: %s",
			       str);
	  return 0;
      }

    var = list->First;
    while (var != NULL)
      {
	  /* checking for redefined variables */
	  if (strcasecmp (name, var->Name) == 0)
	    {
		list->ErrMessage =
		    sqlite3_mprintf
		    ("Duplicated Variable: @%s@ is already defined.", name);
		return 0;
	    }
	  var = var->Next;
      }

/* adding the Variable with Value */
    var = malloc (sizeof (SqlProc_Variable));
    var->Name = name;
    var->Value = value;
    var->Next = NULL;
    if (list->First == NULL)
	list->First = var;
    if (list->Last != NULL)
	list->Last->Next = var;
    list->Last = var;
    return 1;
}

SQLPROC_DECLARE int
gaia_sql_proc_is_valid_var_value (const char *str)
{
/* checking a Variable with Values for validity */
    char *name;
    char *value;

    if (!parse_variable_name_value (str, &name, &value))
	return 0;
    free (name);
    free (value);
    return 1;
}


static struct sp_var_list *
alloc_var_list ()
{
/* allocating an empty list of Variables */
    struct sp_var_list *list = malloc (sizeof (struct sp_var_list));
    list->first = NULL;
    list->last = NULL;
    return list;
}

static void
free_var_list (struct sp_var_list *list)
{
/* destroying a list of Variables */
    struct sp_var_item *item;
    struct sp_var_item *nitem;

    if (list == NULL)
	return;
    item = list->first;
    while (item != NULL)
      {
	  nitem = item->next;
	  if (item->varname != NULL)
	      free (item->varname);
	  free (item);
	  item = nitem;
      }
    free (list);
}

static void
add_variable (struct sp_var_list *list, char *varname)
{
/* adding a Variable to the List */
    struct sp_var_item *item;

    if (list == NULL)
	return;
    if (varname == NULL)
	return;

/* checking for already defined variables */
    item = list->first;
    while (item != NULL)
      {
	  if (strcasecmp (item->varname, varname) == 0)
	    {
		/* already defined */
		item->count += 1;	/* increasing the reference count */
		free (varname);
		return;
	    }
	  item = item->next;
      }

/* inserting a new variable */
    item = malloc (sizeof (struct sp_var_item));
    item->varname = varname;
    item->count = 1;
    item->next = NULL;
    if (list->first == NULL)
	list->first = item;
    if (list->last != NULL)
	list->last->next = item;
    list->last = item;
}

static void
add_variable_ex (struct sp_var_list *list, char *varname, short ref_count)
{
/* adding a Variable to the List */
    struct sp_var_item *item;

    if (list == NULL)
	return;
    if (varname == NULL)
	return;

/* inserting a new variable */
    item = malloc (sizeof (struct sp_var_item));
    item->varname = varname;
    item->count = ref_count;
    item->next = NULL;
    if (list->first == NULL)
	list->first = item;
    if (list->last != NULL)
	list->last->next = item;
    list->last = item;
}

static int
var_list_required_size (struct sp_var_list *list)
{
/* computing the size required to store the List into the BLOB */
    struct sp_var_item *item;
    int size = 0;
/* checking for already defined variables */
    item = list->first;
    while (item != NULL)
      {
	  size += strlen (item->varname) + 7;
	  item = item->next;
      }
    return size;
}

static short
var_list_count_items (struct sp_var_list *list)
{
/* counting how many variables are there */
    struct sp_var_item *item;
    short count = 0;
/* checking for already defined variables */
    item = list->first;
    while (item != NULL)
      {
	  count++;
	  item = item->next;
      }
    return count;
}

SQLPROC_DECLARE int
gaia_sql_proc_parse (const void *cache, const char *xsql,
		     const char *charset, unsigned char **blob, int *blob_sz)
{
/* attempting to parse a Stored Procedure from Text */
    int len;
    int i;
    char *sql = NULL;
    int start_line;
    int macro;
    int comment;
    int variable;
    char varMark;
    int varStart;
    struct sp_var_list *list = NULL;
    struct sp_var_item *item;
    unsigned char *stored_proc = NULL;
    unsigned char *p_out;
    int stored_proc_sz;
    short num_vars;
    int sql_len;
    int endian_arch = gaiaEndianArch ();
    stored_proc_reset_error (cache);

    if (xsql == NULL)
      {
	  const char *errmsg = "NULL SQL body\n";
	  gaia_sql_proc_set_error (cache, errmsg);
	  goto err;
      }
    len = strlen (xsql);
    if (len == 0)
      {
	  const char *errmsg = "Empty SQL body\n";
	  gaia_sql_proc_set_error (cache, errmsg);
	  goto err;
      }
    sql = sqlite3_malloc (len + 1);
    strcpy (sql, xsql);

/* converting the SQL body to UTF-8 */
    if (!gaiaConvertCharset (&sql, charset, "UTF-8"))
      {
	  char *errmsg =
	      sqlite3_mprintf
	      ("Unable to convert the SQL body from %s to UTF-8\n", charset);
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  goto err;
      }

/* parsing the SQL body */
    len = strlen (sql);
    start_line = 1;
    macro = 0;
    comment = 0;
    variable = 0;
    list = alloc_var_list ();
    for (i = 0; i < len; i++)
      {
	  if (sql[i] == '\n')
	    {
		/* EndOfLine found */
		macro = 0;
		comment = 0;
		variable = 0;
		start_line = 1;
		continue;
	    }
	  if (start_line && (sql[i] == ' ' || sql[i] == '\t'))
	    {
		/* skipping leading blanks */
		continue;
	    }
	  if (start_line && sql[i] == '.')
	      macro = 1;
	  if (start_line && sql[i] == '-')
	    {
		if (i < len - 1)
		  {
		      if (sql[i + 1] == '-')
			  comment = 1;
		  }
	    }
	  start_line = 0;
	  if (macro || comment)
	      continue;
	  if (sql[i] == '@' || sql[i] == '$')
	    {
		if (variable && sql[i] == varMark)
		  {
		      /* a variable name ends here */
		      int sz = i - varStart;
		      int j;
		      int k;
		      char *varname = malloc (sz);
		      for (k = 0, j = varStart + 1; j < i; j++, k++)
			  *(varname + k) = sql[j];
		      *(varname + k) = '\0';
		      add_variable (list, varname);
		      variable = 0;
		  }
		else
		  {
		      /* a variable name may start here */
		      variable = 1;
		      varMark = sql[i];
		      varStart = i;
		  }
	    }
      }

/* computing the BLOB size */
    stored_proc_sz = 11;
    sql_len = strlen (sql);
    stored_proc_sz += sql_len;
    stored_proc_sz += var_list_required_size (list);

/* allocating the Stored Procedure BLOB object */
    stored_proc = malloc (stored_proc_sz);
    p_out = stored_proc;

/* preparing the Stored Procedure BLOB object */
    *p_out++ = '\0';
    *p_out++ = SQLPROC_START;	/* START signature */
    *p_out++ = GAIA_LITTLE_ENDIAN;	/* byte ordering */
    *p_out++ = SQLPROC_DELIM;	/* DELIMITER signature */
    num_vars = var_list_count_items (list);
    gaiaExport16 (p_out, num_vars, 1, endian_arch);	/* Number of Variables */
    p_out += 2;
    *p_out++ = SQLPROC_DELIM;	/* DELIMITER signature */
    item = list->first;
    while (item != NULL)
      {
	  len = strlen (item->varname);
	  gaiaExport16 (p_out, len, 1, endian_arch);	/* Variable Name length */
	  p_out += 2;
	  *p_out++ = SQLPROC_DELIM;	/* DELITMITER signature */
	  memcpy (p_out, item->varname, len);
	  p_out += len;
	  *p_out++ = SQLPROC_DELIM;	/* DELITMITER signature */
	  gaiaExport16 (p_out, item->count, 1, endian_arch);	/* Variable reference count */
	  p_out += 2;
	  *p_out++ = SQLPROC_DELIM;	/* DELITMITER signature */
	  item = item->next;
      }
    gaiaExport16 (p_out, sql_len, 1, endian_arch);	/* SQL Body Length */
    p_out += 2;
    *p_out++ = SQLPROC_DELIM;	/* DELIMITER signature */
    memcpy (p_out, sql, sql_len);
    p_out += sql_len;
    *p_out = SQLPROC_STOP;	/* STOP signature */

    sqlite3_free (sql);
    free_var_list (list);
    *blob = stored_proc;
    *blob_sz = stored_proc_sz;
    return 1;

  err:
    if (sql != NULL)
	sqlite3_free (sql);
    if (list != NULL)
	free_var_list (list);
    *blob = NULL;
    *blob_sz = 0;
    return 0;
}

SQLPROC_DECLARE int
gaia_sql_proc_import (const void *cache, const char *filepath,
		      const char *charset, unsigned char **blob, int *blob_sz)
{
/* attempting to import a Stored Procedure from an external File */
    FILE *in = NULL;
    size_t size;
    char *sql = NULL;
    stored_proc_reset_error (cache);

/* opening the input file */
    in = fopen (filepath, "rb");
    if (in == NULL)
      {
	  char *errmsg = sqlite3_mprintf ("Unable to open: %s\n", filepath);
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  goto err;
      }

/* determining the file size */
    if (fseek (in, 0, SEEK_END) != 0)
      {
	  char *errmsg =
	      sqlite3_mprintf ("Unable to read from: %s\n", filepath);
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  goto err;
      }
    size = ftell (in);
    rewind (in);

/* allocating and feeding the SQL body */
    sql = malloc (size + 1);
    if (fread (sql, 1, size, in) != size)
      {
	  char *errmsg =
	      sqlite3_mprintf ("Unable to read from: %s\n", filepath);
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  goto err;
      }
    *(sql + size) = '\0';

/* attempting to parse the SQL body */
    if (!gaia_sql_proc_parse (cache, sql, charset, blob, blob_sz))
	goto err;

    free (sql);
    fclose (in);
    return 1;

  err:
    if (in != NULL)
	fclose (in);
    if (sql != NULL)
	free (sql);
    return 0;
}

SQLPROC_DECLARE int
gaia_sql_proc_is_valid (const unsigned char *blob, int blob_sz)
{
/* checking for a valid Stored Procedure BLOB Object */
    const unsigned char *p_out = blob;
    int endian;
    int endian_arch = gaiaEndianArch ();
    short size;
    short num_vars;
    short i_vars;
    if (blob == NULL)
	return 0;
    if (blob_sz < 7)
	return 0;

    if (*p_out++ != '\0')	/* first byte should alway be null */
	return 0;
    if (*p_out++ != SQLPROC_START)	/* start marker */
	return 0;
    endian = *p_out++;
    if (endian != GAIA_LITTLE_ENDIAN && endian != GAIA_BIG_ENDIAN)	/* endianness */
	return 0;
    if (*p_out++ != SQLPROC_DELIM)	/* delimiter marker */
	return 0;
    if ((p_out - blob) >= blob_sz)
	return 0;
    num_vars = gaiaImport16 (p_out, endian, endian_arch);	/* Variables Count */
    p_out += 2;
    if ((p_out - blob) >= blob_sz)
	return 0;
    if (*p_out++ != SQLPROC_DELIM)
	return 0;
    for (i_vars = 0; i_vars < num_vars; i_vars++)
      {
	  if ((p_out - blob) >= blob_sz)
	      return 0;
	  size = gaiaImport16 (p_out, endian, endian_arch);	/* Variable Name Length */
	  p_out += 2;
	  if ((p_out - blob) >= blob_sz)
	      return 0;
	  if (*p_out++ != SQLPROC_DELIM)
	      return 0;
	  p_out += size;	/* skipping the variable name */
	  if ((p_out - blob) >= blob_sz)
	      return 0;
	  if (*p_out++ != SQLPROC_DELIM)
	      return 0;
	  if ((p_out - blob) >= blob_sz)
	      return 0;
	  p_out += 2;		/* Variable Name Length */
	  if ((p_out - blob) >= blob_sz)
	      return 0;
	  if (*p_out++ != SQLPROC_DELIM)
	      return 0;
      }
    if ((p_out - blob) >= blob_sz)
	return 0;
    size = gaiaImport16 (p_out, endian, endian_arch);	/* SQL Body Length */
    p_out += 2;
    if ((p_out - blob) >= blob_sz)
	return 0;
    if (*p_out++ != SQLPROC_DELIM)
	return 0;
    p_out += size;		/* skipping the SQL body */
    if ((p_out - blob) >= blob_sz)
	return 0;
    if (*p_out != SQLPROC_STOP)
	return 0;

    return 1;
}

SQLPROC_DECLARE int
gaia_sql_proc_var_count (const unsigned char *blob, int blob_sz)
{
/* return the total count of Variabiles from a Stored Procedure BLOB Object */
    const unsigned char *p_out = blob;
    int endian;
    int endian_arch = gaiaEndianArch ();
    short num_vars;
    if (!gaia_sql_proc_is_valid (blob, blob_sz))
	return 0;

    p_out += 2;
    endian = *p_out++;
    p_out++;
    num_vars = gaiaImport16 (p_out, endian, endian_arch);	/* Variables Count */
    return num_vars;
}

SQLPROC_DECLARE char *
gaia_sql_proc_variable (const unsigned char *blob, int blob_sz, int index)
{
/* return the Name of the Nth Variabile from a Stored Procedure BLOB Object */
    const unsigned char *p_out = blob;
    int endian;
    int endian_arch = gaiaEndianArch ();
    short size;
    short num_vars;
    short i_vars;
    if (!gaia_sql_proc_is_valid (blob, blob_sz))
	return NULL;
    if (index < 0)
	return NULL;

    p_out += 2;
    endian = *p_out++;
    p_out++;
    num_vars = gaiaImport16 (p_out, endian, endian_arch);	/* Variables Count */
    p_out += 3;
    for (i_vars = 0; i_vars < num_vars; i_vars++)
      {
	  size = gaiaImport16 (p_out, endian, endian_arch);	/* Variable Name Length */
	  p_out += 3;
	  if (i_vars == index)
	    {
		char *varname = malloc (size + 3);
		*varname = '@';
		memcpy (varname + 1, p_out, size);
		*(varname + size + 1) = '@';
		*(varname + size + 2) = '\0';
		return varname;
	    }
	  p_out += size;	/* skipping the variable name */
	  p_out++;
	  p_out += 3;		/* skipping the reference count */
      }
    return NULL;
}

SQLPROC_DECLARE char *
gaia_sql_proc_all_variables (const unsigned char *blob, int blob_sz)
{
/* return a comma separated list of Variable Names from a Stored Procedure BLOB Object */
    const unsigned char *p_out = blob;
    int endian;
    int endian_arch = gaiaEndianArch ();
    short size;
    short num_vars;
    short i_vars;
    char *varname;
    char *varlist = NULL;
    if (!gaia_sql_proc_is_valid (blob, blob_sz))
	return NULL;

    p_out += 2;
    endian = *p_out++;
    p_out++;
    num_vars = gaiaImport16 (p_out, endian, endian_arch);	/* Variables Count */
    p_out += 3;
    for (i_vars = 0; i_vars < num_vars; i_vars++)
      {
	  size = gaiaImport16 (p_out, endian, endian_arch);	/* Variable Name Length */
	  p_out += 3;
	  varname = malloc (size + 3);
	  *varname = '@';
	  memcpy (varname + 1, p_out, size);
	  *(varname + size + 1) = '@';
	  *(varname + size + 2) = '\0';
	  if (varlist == NULL)
	      varlist = sqlite3_mprintf ("%s", varname);
	  else
	    {
		char *prev = varlist;
		varlist = sqlite3_mprintf ("%s %s", prev, varname);
		sqlite3_free (prev);
	    }
	  free (varname);
	  p_out += size;	/* skipping the variable name */
	  p_out++;
	  p_out += 3;		/* skipping the reference count */
      }
    return varlist;
}

SQLPROC_DECLARE char *
gaia_sql_proc_raw_sql (const unsigned char *blob, int blob_sz)
{
/* return the raw SQL body from a Stored Procedure BLOB Object */
    const unsigned char *p_out = blob;
    int endian;
    int endian_arch = gaiaEndianArch ();
    short size;
    short num_vars;
    short i_vars;
    char *sql = NULL;
    if (!gaia_sql_proc_is_valid (blob, blob_sz))
	return NULL;

    p_out += 2;
    endian = *p_out++;
    p_out++;
    num_vars = gaiaImport16 (p_out, endian, endian_arch);	/* Variables Count */
    p_out += 3;
    for (i_vars = 0; i_vars < num_vars; i_vars++)
      {
	  size = gaiaImport16 (p_out, endian, endian_arch);	/* Variable Name Length */
	  p_out += 3;
	  p_out += size;	/* skipping the variable name */
	  p_out++;
	  p_out += 3;		/* skipping the reference count */
      }
    size = gaiaImport16 (p_out, endian, endian_arch);	/* SQL Body Length */
    p_out += 3;
    sql = malloc (size + 1);
    memcpy (sql, p_out, size);
    *(sql + size) = '\0';
    return sql;
}

static struct sp_var_list *
build_var_list (const unsigned char *blob, int blob_sz)
{
/* building a list of Variables with Values and reference count */
    const unsigned char *p_out = blob;
    int endian;
    int endian_arch = gaiaEndianArch ();
    short size;
    short ref_count;
    short num_vars;
    short i_vars;
    char *varname;
    struct sp_var_list *list = NULL;
    if (!gaia_sql_proc_is_valid (blob, blob_sz))
	return NULL;

    list = alloc_var_list ();
    p_out += 2;
    endian = *p_out++;
    p_out++;
    num_vars = gaiaImport16 (p_out, endian, endian_arch);	/* Variables Count */
    p_out += 3;
    for (i_vars = 0; i_vars < num_vars; i_vars++)
      {
	  size = gaiaImport16 (p_out, endian, endian_arch);	/* Variable Name Length */
	  p_out += 3;
	  varname = malloc (size + 1);
	  memcpy (varname, p_out, size);
	  *(varname + size) = '\0';
	  p_out += size;	/* skipping the variable name */
	  p_out++;
	  ref_count = gaiaImport16 (p_out, endian, endian_arch);	/* Variable reference count */
	  p_out += 3;		/* skipping the reference count */
	  add_variable_ex (list, varname, ref_count);
      }
    return list;
}

static int
get_value_length (SqlProc_VarListPtr variables, const char *varname)
{
/* retieving a Variable replacement Value length */
    SqlProc_VariablePtr var = variables->First;
    while (var != NULL)
      {
	  if (strcasecmp (var->Name, varname) == 0)
	    {
		/* found a replacement value */
		return strlen (var->Value);
	    }
	  var = var->Next;
      }
    return 4;			/* undefined; defaults to NULL */
}

static const char *
search_replacement_value (SqlProc_VarListPtr variables, const char *varname)
{
/* searching a Variable replacement value (if any) */
    SqlProc_VariablePtr var = variables->First;
    while (var != NULL)
      {
	  if (strcasecmp (var->Name, varname) == 0)
	    {
		/* found a replacement value */
		return var->Value;
	    }
	  var = var->Next;
      }
    return NULL;
}

SQLPROC_DECLARE int
gaia_sql_proc_cooked_sql (const void *cache, const unsigned char *blob,
			  int blob_sz, SqlProc_VarListPtr variables, char **sql)
{
/* return the cooked SQL body from a raw SQL body by replacing Variable Values */
    int len;
    int i;
    int start_line;
    int macro;
    int comment;
    int variable;
    char varMark;
    int varStart;
    char *raw = NULL;
    char *cooked = NULL;
    char *p_out;
    int buf_size;
    struct sp_var_list *list = NULL;
    struct sp_var_item *item;
    stored_proc_reset_error (cache);

    *sql = NULL;
    if (variables == NULL)
      {
	  const char *errmsg = "NULL Variables List (Arguments)\n";
	  gaia_sql_proc_set_error (cache, errmsg);
	  goto err;
      }

/* retrieving the Raw SQL Body */
    raw = gaia_sql_proc_raw_sql (blob, blob_sz);
    if (raw == NULL)
      {
	  const char *errmsg = "NULL Raw SQL body\n";
	  gaia_sql_proc_set_error (cache, errmsg);
	  goto err;
      }
    len = strlen (raw);
    if (len == 0)
      {
	  const char *errmsg = "Empty Raw SQL body\n";
	  gaia_sql_proc_set_error (cache, errmsg);
	  goto err;
      }

/* building the Variables List from the BLOB */
    list = build_var_list (blob, blob_sz);
    if (list == NULL)
      {
	  const char *errmsg = "NULL Variables List (Raw SQL)\n";
	  gaia_sql_proc_set_error (cache, errmsg);
	  goto err;
      }

/* allocating the Cooked buffer */
    buf_size = strlen (raw);
    item = list->first;
    while (item != NULL)
      {
	  int value_len = get_value_length (variables, item->varname);
	  buf_size -= (strlen (item->varname) + 2) * item->count;
	  buf_size += value_len * item->count;
	  item = item->next;
      }
    cooked = malloc (buf_size + 1);
    p_out = cooked;

/* parsing the Raw SQL body */
    start_line = 1;
    macro = 0;
    comment = 0;
    variable = 0;
    for (i = 0; i < len; i++)
      {
	  if (raw[i] == '\n')
	    {
		/* EndOfLine found */
		macro = 0;
		comment = 0;
		variable = 0;
		start_line = 1;
		*p_out++ = raw[i];
		continue;
	    }
	  if (start_line && (raw[i] == ' ' || raw[i] == '\t'))
	    {
		/* skipping leading blanks */
		*p_out++ = raw[i];
		continue;
	    }
	  if (start_line && raw[i] == '.')
	      macro = 1;
	  if (start_line && raw[i] == '-')
	    {
		if (i < len - 1)
		  {
		      if (raw[i + 1] == '-')
			  comment = 1;
		  }
	    }
	  start_line = 0;
	  if (macro || comment)
	    {
		*p_out++ = raw[i];
		continue;
	    }
	  if (raw[i] == '@' || raw[i] == '$')
	    {
		if (variable && raw[i] == varMark)
		  {
		      /* a variable name ends here */
		      int sz = i - varStart;
		      int j;
		      int k;
		      const char *replacement_value;
		      char *varname = malloc (sz);
		      for (k = 0, j = varStart + 1; j < i; j++, k++)
			  *(varname + k) = raw[j];
		      *(varname + k) = '\0';
		      replacement_value =
			  search_replacement_value (variables, varname);
		      free (varname);
		      if (replacement_value == NULL)
			  replacement_value = "NULL";
		      for (k = 0; k < (int) strlen (replacement_value); k++)
			  *p_out++ = replacement_value[k];
		      variable = 0;
		  }
		else
		  {
		      /* a variable name may start here */
		      variable = 1;
		      varMark = raw[i];
		      varStart = i;
		  }
		continue;
	    }
	  if (!variable)
	      *p_out++ = raw[i];
      }
    *p_out = '\0';

    free (raw);
    free_var_list (list);
    *sql = cooked;
    return 1;

  err:
    if (raw != NULL)
	free (raw);
    if (cooked != NULL)
	free (cooked);
    if (list != NULL)
	free_var_list (list);
    return 0;
}

static int
test_stored_proc_tables (sqlite3 * handle)
{
/* testing if the Stored Procedures Tables are already defined */
    int ok_name = 0;
    int ok_title = 0;
    int ok_sql_proc = 0;
    int ok_var_value = 0;
    char sql[1024];
    int ret;
    const char *name;
    int i;
    char **results;
    int rows;
    int columns;

/* checking the STORED_PROCEDURES table */
    strcpy (sql, "PRAGMA table_info(stored_procedures)");
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		name = results[(i * columns) + 1];
		if (strcasecmp (name, "name") == 0)
		    ok_name = 1;
		if (strcasecmp (name, "title") == 0)
		    ok_title = 1;
		if (strcasecmp (name, "sql_proc") == 0)
		    ok_sql_proc = 1;
	    }
      }
    sqlite3_free_table (results);
    if (ok_name && ok_title && ok_sql_proc)
	;
    else
	return 0;

/* checking the STORED_PROCEDURES table */
    ok_name = 0;
    ok_title = 0;
    strcpy (sql, "PRAGMA table_info(stored_variables)");
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		name = results[(i * columns) + 1];
		if (strcasecmp (name, "name") == 0)
		    ok_name = 1;
		if (strcasecmp (name, "title") == 0)
		    ok_title = 1;
		if (strcasecmp (name, "var_value") == 0)
		    ok_var_value = 1;
	    }
      }
    sqlite3_free_table (results);
    if (ok_name && ok_title && ok_var_value)
	return 1;
    return 0;
}

SQLPROC_DECLARE int
gaia_stored_proc_create_tables (sqlite3 * handle, const void *cache)
{
/* attempts to create the Stored Procedures Tables */
    char sql[4192];
    char *errMsg = NULL;
    int ret;

    if (test_stored_proc_tables (handle))
	return 1;
    stored_proc_reset_error (cache);

/* creating the STORED_PROCEDURES table */
    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "stored_procedures (\n");
    strcat (sql, "name TEXT NOT NULL PRIMARY KEY,\n");
    strcat (sql, "title TEXT NOT NULL,\n");
    strcat (sql, "sql_proc BLOB NOT NULL)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
      {
	  char *errmsg =
	      sqlite3_mprintf ("gaia_stored_create \"stored_procedures\": %s",
			       sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

/* creating Triggers supporting STORED_PROCEDURES */
    sprintf (sql,
	     "CREATE TRIGGER storproc_ins BEFORE INSERT ON stored_procedures\n"
	     "FOR EACH ROW BEGIN\n"
	     "SELECT RAISE(ROLLBACK, 'Invalid \"sql_proc\": not a BLOB of the SQL Procedure type')\n"
	     "WHERE SqlProc_IsValid(NEW.sql_proc) <> 1;\nEND");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
      {
	  char *errmsg =
	      sqlite3_mprintf ("gaia_stored_create \"storproc_ins\": %s",
			       sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }
    sprintf (sql,
	     "CREATE TRIGGER storproc_upd BEFORE UPDATE OF sql_proc ON stored_procedures\n"
	     "FOR EACH ROW BEGIN\n"
	     "SELECT RAISE(ROLLBACK, 'Invalid \"sql_proc\": not a BLOB of the SQL Procedure type')\n"
	     "WHERE SqlProc_IsValid(NEW.sql_proc) <> 1;\nEND");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
      {
	  char *errmsg =
	      sqlite3_mprintf ("gaia_stored_create \"storproc_upd\": %s",
			       sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

/* creating the STORED_VALUES table */
    strcpy (sql, "CREATE TABLE IF NOT EXISTS ");
    strcat (sql, "stored_variables (\n");
    strcat (sql, "name TEXT NOT NULL PRIMARY KEY,\n");
    strcat (sql, "title TEXT NOT NULL,\n");
    strcat (sql, "var_value TEXT NOT NULL)");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
      {
	  char *errmsg =
	      sqlite3_mprintf ("gaia_stored_create \"stored_variables\": %s",
			       sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

/* creating Triggers supporting STORED_VARIABLES */
    sprintf (sql,
	     "CREATE TRIGGER storvar_ins BEFORE INSERT ON stored_variables\n"
	     "FOR EACH ROW BEGIN\n"
	     "SELECT RAISE(ROLLBACK, 'Invalid \"var_value\": not an acceptable SQL Variable Text string')\n"
	     "WHERE SqlProc_IsValidVarValue(NEW.var_value) <> 1;\nEND");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
      {
	  char *errmsg =
	      sqlite3_mprintf ("gaia_stored_create \"storvar_ins\": %s",
			       sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }
    sprintf (sql,
	     "CREATE TRIGGER storvar_upd BEFORE UPDATE OF sql_proc ON stored_variables\n"
	     "FOR EACH ROW BEGIN\n"
	     "SELECT RAISE(ROLLBACK, 'Invalid \"var_value\": not an acceptable SQL Variable Text string')\n"
	     "WHERE SqlProc_IsValidVarValue(NEW.var_value) <> 1;\nEND");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
      {
	  char *errmsg =
	      sqlite3_mprintf ("gaia_stored_create \"storvar_upd\": %s",
			       sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

    if (test_stored_proc_tables (handle))
	return 1;
    return 0;
}

SQLPROC_DECLARE int
gaia_stored_proc_store (sqlite3 * handle, const void *cache, const char *name,
			const char *title, const unsigned char *blob,
			int blob_sz)
{
/* permanently registering a Stored Procedure */
    const char *sql;
    sqlite3_stmt *stmt;
    int ret;
    stored_proc_reset_error (cache);

    sql =
	"INSERT INTO stored_procedures(name, title, sql_proc) VALUES (?, ?, ?)";
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_proc_store: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, name, strlen (name), SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, title, strlen (title), SQLITE_STATIC);
    sqlite3_bind_blob (stmt, 3, blob, blob_sz, SQLITE_STATIC);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_proc_store: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  sqlite3_finalize (stmt);
	  return 0;
      }
    sqlite3_finalize (stmt);
    return 1;
}

SQLPROC_DECLARE int
gaia_stored_proc_fetch (sqlite3 * handle, const void *cache, const char *name,
			unsigned char **blob, int *blob_sz)
{
/* will return the SQL Procedure BLOB from a given Stored Procedure */
    const char *sql;
    sqlite3_stmt *stmt;
    int ret;
    unsigned char *p_blob = NULL;
    int p_blob_sz = 0;
    stored_proc_reset_error (cache);

    sql = "SELECT sql_proc FROM stored_procedures WHERE name = ?";
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_proc_fetch: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, name, strlen (name), SQLITE_STATIC);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		if (sqlite3_column_type (stmt, 0) == SQLITE_BLOB)
		  {
		      const unsigned char *data = sqlite3_column_blob (stmt, 0);
		      p_blob_sz = sqlite3_column_bytes (stmt, 0);
		      p_blob = malloc (p_blob_sz);
		      memcpy (p_blob, data, p_blob_sz);
		  }
	    }
      }
    sqlite3_finalize (stmt);

    *blob = p_blob;
    *blob_sz = p_blob_sz;
    if (p_blob == NULL)
	return 0;
    return 1;
}

SQLPROC_DECLARE int
gaia_stored_proc_delete (sqlite3 * handle, const void *cache, const char *name)
{
/* removing a permanently registered Stored Procedure */
    const char *sql;
    sqlite3_stmt *stmt;
    int ret;
    stored_proc_reset_error (cache);

    sql = "DELETE FROM stored_procedures WHERE name = ?";
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_proc_delete: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, name, strlen (name), SQLITE_STATIC);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_proc_delete: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  sqlite3_finalize (stmt);
	  return 0;
      }
    sqlite3_finalize (stmt);
    if (sqlite3_changes (handle) == 0)
	return 0;
    return 1;
}

SQLPROC_DECLARE int
gaia_stored_proc_update_title (sqlite3 * handle, const void *cache,
			       const char *name, const char *title)
{
/* updating a permanently registered Stored Procedure - Title */
    const char *sql;
    sqlite3_stmt *stmt;
    int ret;
    stored_proc_reset_error (cache);

    sql = "UPDATE stored_procedures SET title = ? WHERE name = ?";
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_proc_update_title: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, title, strlen (title), SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, name, strlen (name), SQLITE_STATIC);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_proc_update_title: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  sqlite3_finalize (stmt);
	  return 0;
      }
    sqlite3_finalize (stmt);
    if (sqlite3_changes (handle) == 0)
	return 0;
    return 1;
}

SQLPROC_DECLARE int
gaia_stored_proc_update_sql (sqlite3 * handle, const void *cache,
			     const char *name, const unsigned char *blob,
			     int blob_sz)
{
/* updating a permanently registered Stored Procedure - SQL body */
    const char *sql;
    sqlite3_stmt *stmt;
    int ret;
    stored_proc_reset_error (cache);

    sql = "UPDATE stored_procedures SET sql_proc = ? WHERE name = ?";
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_proc_update_sql: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_blob (stmt, 1, blob, blob_sz, SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, name, strlen (name), SQLITE_STATIC);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_proc_update_sql: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  sqlite3_finalize (stmt);
	  return 0;
      }
    sqlite3_finalize (stmt);
    if (sqlite3_changes (handle) == 0)
	return 0;
    return 1;
}

SQLPROC_DECLARE int
gaia_stored_var_store (sqlite3 * handle, const void *cache, const char *name,
		       const char *title, const char *value)
{
/* permanently registering a Stored Variable */
    const char *sql;
    sqlite3_stmt *stmt;
    int ret;
    stored_proc_reset_error (cache);

    sql =
	"INSERT INTO stored_variables(name, title, var_value) VALUES (?, ?, ?)";
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_var_store: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, name, strlen (name), SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, title, strlen (title), SQLITE_STATIC);
    sqlite3_bind_text (stmt, 3, value, strlen (value), SQLITE_STATIC);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_var_store: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  sqlite3_finalize (stmt);
	  return 0;
      }
    sqlite3_finalize (stmt);
    return 1;
}

SQLPROC_DECLARE int
gaia_stored_var_fetch (sqlite3 * handle, const void *cache, const char *name,
		       char **value)
{
/* will return a Variable with Value string from a given Stored Variable */
    const char *sql;
    sqlite3_stmt *stmt;
    int ret;
    char *p_value = NULL;
    stored_proc_reset_error (cache);

    sql = "SELECT var_value FROM stored_variables WHERE name = ?";
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_var_fetch: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, name, strlen (name), SQLITE_STATIC);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		if (sqlite3_column_type (stmt, 0) == SQLITE_TEXT)
		  {
		      const char *data =
			  (const char *) sqlite3_column_text (stmt, 0);
		      p_value = malloc (strlen (data) + 1);
		      strcpy (p_value, data);
		  }
	    }
      }
    sqlite3_finalize (stmt);

    *value = p_value;;
    if (p_value == NULL)
	return 0;
    return 1;
}

SQLPROC_DECLARE int
gaia_stored_var_delete (sqlite3 * handle, const void *cache, const char *name)
{
/* removing a permanently registered Stored Variable */
    const char *sql;
    sqlite3_stmt *stmt;
    int ret;
    stored_proc_reset_error (cache);

    sql = "DELETE FROM stored_variables WHERE name = ?";
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_var_delete: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, name, strlen (name), SQLITE_STATIC);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_var_delete: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  sqlite3_finalize (stmt);
	  return 0;
      }
    sqlite3_finalize (stmt);
    if (sqlite3_changes (handle) == 0)
	return 0;
    return 1;
}

SQLPROC_DECLARE int
gaia_stored_var_update_title (sqlite3 * handle, const void *cache,
			      const char *name, const char *title)
{
/* updating a permanently registered Stored Variable - Title */
    const char *sql;
    sqlite3_stmt *stmt;
    int ret;
    stored_proc_reset_error (cache);

    sql = "UPDATE stored_variables SET title = ? WHERE name = ?";
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_var_update_title: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, title, strlen (title), SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, name, strlen (name), SQLITE_STATIC);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_var_update_title: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  sqlite3_finalize (stmt);
	  return 0;
      }
    sqlite3_finalize (stmt);
    if (sqlite3_changes (handle) == 0)
	return 0;
    return 1;
}

SQLPROC_DECLARE int
gaia_stored_var_update_value (sqlite3 * handle, const void *cache,
			      const char *name, const char *value)
{
/* updating a permanently registered Stored Variable - Variable with Value */
    const char *sql;
    sqlite3_stmt *stmt;
    int ret;
    stored_proc_reset_error (cache);

    sql = "UPDATE stored_variables SET var_value = ? WHERE name = ?";
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_var_update_value: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  return 0;
      }

    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, value, strlen (value), SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, name, strlen (name), SQLITE_STATIC);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
      {
	  char *errmsg = sqlite3_mprintf ("gaia_stored_var_update_value: %s",
					  sqlite3_errmsg (handle));
	  gaia_sql_proc_set_error (cache, errmsg);
	  sqlite3_free (errmsg);
	  sqlite3_finalize (stmt);
	  return 0;
      }
    sqlite3_finalize (stmt);
    if (sqlite3_changes (handle) == 0)
	return 0;
    return 1;
}

static const char *
consume_empty_sql (const char *ptr)
{
/* testing for an empty SQL string */
    int comment_marker = 0;
    const char *p = ptr;
    while (1)
      {
	  char c = *p;
	  if (c == '\0')
	      break;
	  if (c == ' ' || c == '\0' || c == '\t' || c == '\r' || c == '\n')
	  {
		  p++;
	      continue;		/* ignoring leading blanks */
	  }
	  if (c == '.')
	  {
		      while (1)
			{
				/* consuming a dot-macro until the end of the line */
			    c = *p;
			    if (c == '\n' || c == '\0')
				break;
				p++;
			}
			if (c != '\0')
			p++;
			continue;
	  }
	  if (c == '-')
	    {
		if (comment_marker)
		  {
		      while (1)
			{
				/* consuming a comment until the end of the line */
			    c = *p;
			    if (c == '\n' || c == '\0')
				break;
				p++;
			}
		      comment_marker = 0;
			if (c != '\0')
		      p++;
		      continue;
		  }
		comment_marker = 1;
		p++;
		continue;
	    }
	  break;
      }
    return p;
}

SQLPROC_DECLARE int
gaia_sql_proc_execute (sqlite3 * handle, const void *cache, const char *sql)
{
/* executing an already cooked SQL Body */
    const char *pSql = sql;
    sqlite3_stmt *stmt;

    while (1)
      {
	  const char *sql_tail;
	  int ret;
	  
	  pSql = consume_empty_sql (pSql);
	  if (strlen(pSql) == 0)
	      break;
	  ret = sqlite3_prepare_v2 (handle, pSql, strlen (pSql), &stmt,
				    &sql_tail);
	  pSql = sql_tail;
	  while (1)
	    {
		/* executing an SQL statement */
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE)
		    break;
		else if (ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *errmsg =
			  sqlite3_mprintf ("gaia_sql_proc_execute: %s",
					   sqlite3_errmsg (handle));
		      gaia_sql_proc_set_error (cache, errmsg);
		      sqlite3_free (errmsg);
		      return 0;
		  }
	    }
	  sqlite3_finalize (stmt);
      }
    return 1;
}
