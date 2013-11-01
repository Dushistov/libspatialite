/*
 alloc_cache.c -- Gaia spatial support for SQLite

 version 4.1, 2013 May 8

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
 
Portions created by the Initial Developer are Copyright (C) 2013
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

#include <spatialite/sqlite.h>
#include <spatialite/debug.h>

#include <spatialite.h>
#include <spatialite_private.h>
#include <spatialite/gg_advanced.h>

#ifdef ENABLE_LWGEOM		/* enabling LWGEOM support */
#include <liblwgeom.h>
#endif /* end enabling LWGEOM support */

#include "cache_aux_1.h"

/* GLOBAL variables */
extern char *gaia_geos_error_msg;
extern char *gaia_geos_warning_msg;

static void
setGeosErrorMsg (int pool_index, const char *msg)
{
/* setting the latest GEOS error message */
    struct splite_connection *p = &(splite_connection_pool[pool_index]);
    int len;
    if (p->gaia_geos_error_msg != NULL)
	free (p->gaia_geos_error_msg);
    p->gaia_geos_error_msg = NULL;
    if (msg == NULL)
	return;
    len = strlen (msg);
    p->gaia_geos_error_msg = malloc (len + 1);
    strcpy (p->gaia_geos_error_msg, msg);
}

static void
setGeosWarningMsg (int pool_index, const char *msg)
{
/* setting the latest GEOS error message */
    struct splite_connection *p = &(splite_connection_pool[pool_index]);
    int len;
    if (p->gaia_geos_warning_msg != NULL)
	free (p->gaia_geos_warning_msg);
    p->gaia_geos_warning_msg = NULL;
    if (msg == NULL)
	return;
    len = strlen (msg);
    p->gaia_geos_warning_msg = malloc (len + 1);
    strcpy (p->gaia_geos_warning_msg, msg);
}

static void
setGeosAuxErrorMsg (int pool_index, const char *msg)
{
/* setting the latest GEOS (auxiliary) error message */
    struct splite_connection *p = &(splite_connection_pool[pool_index]);
    int len;
    if (p->gaia_geosaux_error_msg != NULL)
	free (p->gaia_geosaux_error_msg);
    p->gaia_geosaux_error_msg = NULL;
    if (msg == NULL)
	return;
    len = strlen (msg);
    p->gaia_geosaux_error_msg = malloc (len + 1);
    strcpy (p->gaia_geosaux_error_msg, msg);
}

static void
geos_error_r (int pool_index, const char *fmt, va_list ap)
{
/* reporting some GEOS error - thread safe */
    char *msg;
    msg = sqlite3_vmprintf (fmt, ap);
    if (msg)
      {
	  spatialite_e ("GEOS error: %s\n", msg);
	  setGeosErrorMsg (pool_index, msg);
	  sqlite3_free (msg);
      }
    else
	setGeosErrorMsg (pool_index, NULL);
}

static void
geos_warning_r (int pool_index, const char *fmt, va_list ap)
{
/* reporting some GEOS warning - thread safe */
    char *msg;
    msg = sqlite3_vmprintf (fmt, ap);
    if (msg)
      {
	  spatialite_e ("GEOS warning: %s\n", msg);
	  setGeosWarningMsg (pool_index, msg);
	  sqlite3_free (msg);
      }
    else
	setGeosWarningMsg (pool_index, NULL);
}

#include "cache_aux_2.h"

static int
find_free_connection ()
{
    int i;
    for (i = 0; i < SPATIALITE_MAX_CONNECTIONS; i++)
      {
	  struct splite_connection *p = &(splite_connection_pool[i]);
	  if (p->in_use == 0)
	    {
		p->in_use = -1;
		return i;
	    }
      }
    spatialite_e ("ERROR: Too much connections: max %d\n",
		  SPATIALITE_MAX_CONNECTIONS);
    return -1;
}

static void
confirm (int i)
{
/* marking the slot as definitely reserved */
    struct splite_connection *p = &(splite_connection_pool[i]);
    p->in_use = 1;
}

static void
invalidate (int i)
{
/* definitely releasing the slot */
    struct splite_connection *p = &(splite_connection_pool[i]);
    if (p->gaia_geos_error_msg != NULL)
	free (p->gaia_geos_error_msg);
    if (p->gaia_geos_warning_msg != NULL)
	free (p->gaia_geos_warning_msg);
    if (p->gaia_geosaux_error_msg != NULL)
	free (p->gaia_geosaux_error_msg);
    p->gaia_geos_error_msg = NULL;
    p->gaia_geos_warning_msg = NULL;
    p->gaia_geosaux_error_msg = NULL;
    p->in_use = 0;
}

SPATIALITE_DECLARE void *
spatialite_alloc_connection ()
{
/* allocating and initializing an empty internal cache */
    gaiaOutBufferPtr out;
    int i;
    struct splite_internal_cache *cache;
    struct splite_geos_cache_item *p;
    struct splite_xmlSchema_cache_item *p_xmlSchema;
    int pool_index = find_free_connection ();

    if (pool_index < 0)
	return NULL;

    cache = malloc (sizeof (struct splite_internal_cache));
    if (cache == NULL)
      {
	  invalidate (pool_index);
	  return NULL;
      }
    cache->magic1 = SPATIALITE_CACHE_MAGIC1;
    cache->magic2 = SPATIALITE_CACHE_MAGIC2;
    cache->GEOS_handle = NULL;
    cache->pool_index = pool_index;
    confirm (pool_index);
/* initializing the XML error buffers */
    out = malloc (sizeof (gaiaOutBuffer));
    gaiaOutBufferInitialize (out);
    cache->xmlParsingErrors = out;
    out = malloc (sizeof (gaiaOutBuffer));
    gaiaOutBufferInitialize (out);
    cache->xmlSchemaValidationErrors = out;
    out = malloc (sizeof (gaiaOutBuffer));
    gaiaOutBufferInitialize (out);
    cache->xmlXPathErrors = out;
/* initializing the GEOS cache */
    p = &(cache->cacheItem1);
    memset (p->gaiaBlob, '\0', 64);
    p->gaiaBlobSize = 0;
    p->crc32 = 0;
    p->geosGeom = NULL;
    p->preparedGeosGeom = NULL;
    p = &(cache->cacheItem2);
    memset (p->gaiaBlob, '\0', 64);
    p->gaiaBlobSize = 0;
    p->crc32 = 0;
    p->geosGeom = NULL;
    p->preparedGeosGeom = NULL;
    for (i = 0; i < MAX_XMLSCHEMA_CACHE; i++)
      {
	  /* initializing the XmlSchema cache */
	  p_xmlSchema = &(cache->xmlSchemaCache[i]);
	  p_xmlSchema->timestamp = 0;
	  p_xmlSchema->schemaURI = NULL;
	  p_xmlSchema->schemaDoc = NULL;
	  p_xmlSchema->parserCtxt = NULL;
	  p_xmlSchema->schema = NULL;
      }

#include "cache_aux_3.h"

    return cache;
}

SPATIALITE_PRIVATE void
free_internal_cache (struct splite_internal_cache *cache)
{
/* freeing an internal cache */
    struct splite_geos_cache_item *p;
#ifdef ENABLE_LIBXML2
    int i;
    struct splite_xmlSchema_cache_item *p_xmlSchema;
#endif
/* freeing the XML error buffers */
    gaiaOutBufferReset (cache->xmlParsingErrors);
    gaiaOutBufferReset (cache->xmlSchemaValidationErrors);
    gaiaOutBufferReset (cache->xmlXPathErrors);
    free (cache->xmlParsingErrors);
    free (cache->xmlSchemaValidationErrors);
    free (cache->xmlXPathErrors);

/* freeing the GEOS cache */
    p = &(cache->cacheItem1);
    splite_free_geos_cache_item_r (cache, p);
    p = &(cache->cacheItem2);
    splite_free_geos_cache_item_r (cache, p);
#ifdef ENABLE_LIBXML2
    for (i = 0; i < MAX_XMLSCHEMA_CACHE; i++)
      {
	  /* freeing the XmlSchema cache */
	  p_xmlSchema = &(cache->xmlSchemaCache[i]);
	  splite_free_xml_schema_cache_item (p_xmlSchema);
      }
#endif

/* releasing the connection pool object */
    invalidate (cache->pool_index);

/* freeing the cache itself */
    free (cache);
}

GAIAGEO_DECLARE void
gaiaResetGeosMsg_r (const void *p_cache)
{
/* resets the GEOS error and warning messages */
    struct splite_connection *p = NULL;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache != NULL)
      {
	  if (cache->magic1 == SPATIALITE_CACHE_MAGIC1
	      || cache->magic2 == SPATIALITE_CACHE_MAGIC2)
	      p = &(splite_connection_pool[cache->pool_index]);
      }
    if (p == NULL)
	return;
    if (p->gaia_geos_error_msg != NULL)
	free (p->gaia_geos_error_msg);
    if (p->gaia_geos_warning_msg != NULL)
	free (p->gaia_geos_warning_msg);
    if (p->gaia_geosaux_error_msg != NULL)
	free (p->gaia_geosaux_error_msg);
    p->gaia_geos_error_msg = NULL;
    p->gaia_geos_warning_msg = NULL;
    p->gaia_geosaux_error_msg = NULL;
}

GAIAGEO_DECLARE const char *
gaiaGetGeosErrorMsg_r (const void *p_cache)
{
/* return the latest GEOS error message */
    struct splite_connection *p = NULL;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache != NULL)
      {
	  if (cache->magic1 == SPATIALITE_CACHE_MAGIC1
	      || cache->magic2 == SPATIALITE_CACHE_MAGIC2)
	      p = &(splite_connection_pool[cache->pool_index]);
      }
    if (p == NULL)
	return NULL;
    return p->gaia_geos_error_msg;
}

GAIAGEO_DECLARE const char *
gaiaGetGeosWarningMsg_r (const void *p_cache)
{
/* return the latest GEOS error message */
    struct splite_connection *p = NULL;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache != NULL)
      {
	  if (cache->magic1 == SPATIALITE_CACHE_MAGIC1
	      || cache->magic2 == SPATIALITE_CACHE_MAGIC2)
	      p = &(splite_connection_pool[cache->pool_index]);
      }
    if (p == NULL)
	return NULL;
    return p->gaia_geos_warning_msg;
}

GAIAGEO_DECLARE const char *
gaiaGetGeosAuxErrorMsg_r (const void *p_cache)
{
/* return the latest GEOS (auxialiary) error message */
    struct splite_connection *p = NULL;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache != NULL)
      {
	  if (cache->magic1 == SPATIALITE_CACHE_MAGIC1
	      || cache->magic2 == SPATIALITE_CACHE_MAGIC2)
	      p = &(splite_connection_pool[cache->pool_index]);
      }
    if (p == NULL)
	return NULL;
    return p->gaia_geosaux_error_msg;
}

GAIAGEO_DECLARE void
gaiaSetGeosErrorMsg_r (const void *p_cache, const char *msg)
{
/* setting the latest GEOS error message */
    int len;
    struct splite_connection *p = NULL;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache != NULL)
      {
	  if (cache->magic1 == SPATIALITE_CACHE_MAGIC1
	      || cache->magic2 == SPATIALITE_CACHE_MAGIC2)
	      p = &(splite_connection_pool[cache->pool_index]);
      }
    if (p == NULL)
	return;
    if (p->gaia_geos_error_msg != NULL)
	free (p->gaia_geos_error_msg);
    p->gaia_geos_error_msg = NULL;
    if (msg == NULL)
	return;
    len = strlen (msg);
    p->gaia_geos_error_msg = malloc (len + 1);
    strcpy (p->gaia_geos_error_msg, msg);
}

GAIAGEO_DECLARE void
gaiaSetGeosWarningMsg_r (const void *p_cache, const char *msg)
{
/* setting the latest GEOS error message */
    int len;
    struct splite_connection *p = NULL;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache != NULL)
      {
	  if (cache->magic1 == SPATIALITE_CACHE_MAGIC1
	      || cache->magic2 == SPATIALITE_CACHE_MAGIC2)
	      p = &(splite_connection_pool[cache->pool_index]);
      }
    if (p == NULL)
	return;
    if (p->gaia_geos_warning_msg != NULL)
	free (p->gaia_geos_warning_msg);
    p->gaia_geos_warning_msg = NULL;
    if (msg == NULL)
	return;
    len = strlen (msg);
    p->gaia_geos_warning_msg = malloc (len + 1);
    strcpy (p->gaia_geos_warning_msg, msg);
}

GAIAGEO_DECLARE void
gaiaSetGeosAuxErrorMsg_r (const void *p_cache, const char *msg)
{
/* setting the latest GEOS (auxiliary) error message */
    int len;
    struct splite_connection *p = NULL;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache != NULL)
      {
	  if (cache->magic1 == SPATIALITE_CACHE_MAGIC1
	      || cache->magic2 == SPATIALITE_CACHE_MAGIC2)
	      p = &(splite_connection_pool[cache->pool_index]);
      }
    if (p == NULL)
	return;
    if (p->gaia_geosaux_error_msg != NULL)
	free (p->gaia_geosaux_error_msg);
    p->gaia_geosaux_error_msg = NULL;
    if (msg == NULL)
	return;
    len = strlen (msg);
    p->gaia_geosaux_error_msg = malloc (len + 1);
    strcpy (p->gaia_geosaux_error_msg, msg);
}

static char *
parse_number_from_msg (const char *str)
{
/* attempting to parse a number from a string */
    int sign = 0;
    int point = 0;
    int digit = 0;
    int err = 0;
    int len;
    char *res;
    const char *p = str;
    while (1)
      {
	  if (*p == '+' || *p == '-')
	    {
		sign++;
		p++;
		continue;
	    }
	  if (*p == '.')
	    {
		point++;
		p++;
		continue;
	    }
	  if (*p >= '0' && *p <= '9')
	    {
		p++;
		digit++;
		continue;
	    }
	  break;
      }
    if (sign > 1)
	err = 1;
    if (sign == 1 && *str != '+' && *str != '-')
	err = 1;
    if (point > 1)
	err = 1;
    if (!digit)
	err = 1;
    if (err)
	return NULL;
    len = p - str;
    res = malloc (len + 1);
    memcpy (res, str, len);
    *(res + len) = '\0';
    return res;
}

static int
check_geos_critical_point (const char *msg, double *x, double *y)
{
/* attempts to extract an [X,Y] Point coords from within a string */
    char *px;
    char *py;
    const char *ref = " at or near point ";
    const char *ref2 = " conflict at ";
    const char *p = strstr (msg, ref);
    if (p != NULL)
	goto ok_ref;
    p = strstr (msg, ref2);
    if (p == NULL)
	return 0;
    p += strlen (ref2);
    goto ok_ref2;
  ok_ref:
    p += strlen (ref);
  ok_ref2:
    px = parse_number_from_msg (p);
    if (px == NULL)
	return 0;
    p += strlen (px) + 1;
    py = parse_number_from_msg (p);
    if (py == NULL)
      {
	  free (px);
	  return 0;
      }
    *x = atof (px);
    *y = atof (py);
    free (px);
    free (py);
    return 1;
}

GAIAGEO_DECLARE gaiaGeomCollPtr
gaiaCriticalPointFromGEOSmsg (void)
{
/*
/ Attempts to return a Point Geometry extracted from the latest GEOS 
/ error / warning message
*/
    double x;
    double y;
    gaiaGeomCollPtr geom;
    const char *msg = gaia_geos_error_msg;
    if (msg == NULL)
	msg = gaia_geos_warning_msg;
    if (msg == NULL)
	return NULL;
    if (!check_geos_critical_point (msg, &x, &y))
	return NULL;
    geom = gaiaAllocGeomColl ();
    gaiaAddPointToGeomColl (geom, x, y);
    return geom;
}

GAIAGEO_DECLARE gaiaGeomCollPtr
gaiaCriticalPointFromGEOSmsg_r (const void *p_cache)
{
/*
/ Attempts to return a Point Geometry extracted from the latest GEOS 
/ error / warning message
*/
    double x;
    double y;
    gaiaGeomCollPtr geom;
    const char *msg;
    struct splite_connection *p = NULL;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache != NULL)
      {
	  if (cache->magic1 == SPATIALITE_CACHE_MAGIC1
	      || cache->magic2 == SPATIALITE_CACHE_MAGIC2)
	      p = &(splite_connection_pool[cache->pool_index]);
      }
    if (p == NULL)
	return NULL;
    msg = p->gaia_geos_error_msg;
    if (msg == NULL)
	msg = p->gaia_geos_warning_msg;
    if (msg == NULL)
	return NULL;
    if (!check_geos_critical_point (msg, &x, &y))
	return NULL;
    geom = gaiaAllocGeomColl ();
    gaiaAddPointToGeomColl (geom, x, y);
    return geom;
}
