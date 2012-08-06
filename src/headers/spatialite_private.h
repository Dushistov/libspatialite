/* 
 spatialite.h -- Gaia spatial support for SQLite 
  
 version 4.0, 2012 August 6

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
 
Portions created by the Initial Developer are Copyright (C) 2008-2012
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

/**
 \file spatialite_private.h

 SpatiaLite private header file
 */
#ifndef DOXYGEN_SHOULD_SKIP_THIS
#ifdef _WIN32
#ifdef DLL_EXPORT
#define SPATIALITE_PRIVATE
#else
#define SPATIALITE_PRIVATE
#endif
#else
#define SPATIALITE_PRIVATE __attribute__ ((visibility("hidden")))
#endif
#endif

#ifndef _SPATIALITE_PRIVATE_H
#ifndef DOXYGEN_SHOULD_SKIP_THIS
#define _SPATIALITE_PRIVATE_H
#endif

#ifdef __cplusplus
extern "C"
{
#endif

/** spatial_ref_sys_init2: will create the "spatial_ref_sys" table
 and will populate this table with any supported EPSG SRID definition */
#define GAIA_EPSG_ANY -9999
/** spatial_ref_sys_init2: will create the "spatial_ref_sys" table
 and will populate this table only inserting WGS84-related definitions */
#define GAIA_EPSG_WGS84_ONLY -9998
/** spatial_ref_sys_init2: will create the "spatial_ref_sys" table
 but will avoid to insert any row at all */
#define GAIA_EPSG_NONE -9997

    struct epsg_defs
    {
	int srid;
	char *auth_name;
	int auth_srid;
	char *ref_sys_name;
	char *proj4text;
	char *srs_wkt;
	struct epsg_defs *next;
    };

    SPATIALITE_PRIVATE struct epsg_defs *add_epsg_def (int filter_srid,
						       struct epsg_defs **first,
						       struct epsg_defs **last,
						       int srid,
						       const char *auth_name,
						       int auth_srid,
						       const char
						       *ref_sys_name);

    SPATIALITE_PRIVATE void
	add_proj4text (struct epsg_defs *p, int count, const char *text);

    SPATIALITE_PRIVATE void
	add_srs_wkt (struct epsg_defs *p, int count, const char *text);

    SPATIALITE_PRIVATE void
	initialize_epsg (int filter, struct epsg_defs **first,
			 struct epsg_defs **last);

    SPATIALITE_PRIVATE int checkSpatialMetaData (const void *sqlite);

#ifdef __cplusplus
}
#endif

#endif				/* _SPATIALITE_PRIVATE_H */
