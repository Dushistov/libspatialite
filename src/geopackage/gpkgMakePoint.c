/*

    GeoPackage extensions for SpatiaLite / SQLite
 
Version: MPL 1.1/GPL 2.0/LGPL 2.1

The contents of this file are subject to the Mozilla Public License Version
1.1 (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
http://www.mozilla.org/MPL/
 
Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the
License.

The Original Code is GeoPackage Extensions

The Initial Developer of the Original Code is Brad Hards (bradh@frogmouth.net)
 
Portions created by the Initial Developer are Copyright (C) 2012
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

#include "spatialite/geopackage.h"
#include "config.h"
#include "geopackage_internal.h"

#ifdef ENABLE_GEOPACKAGE

#define GEOPACKAGE_UNUSED() if (argc || argv) argc = argc;

#define GEOPACKAGE_HEADER_LEN 8
#define GEOPACKAGE_2D_ENVELOPE_LEN 32
#define GEOPACKAGE_MAGIC1 0x47;
#define GEOPACKAGE_MAGIC2 0x50;
#define GEOPACKAGE_VERSION 0x00;
#define GEOPACKAGE_WKB_LITTLEENDIAN 0x01
#define GEOPACKAGE_2D_ENVELOPE 0x01
#define GEOPACKAGE_FLAGS_2D_LITTLEENDIAN ((GEOPACKAGE_2D_ENVELOPE << 1) | GEOPACKAGE_WKB_LITTLEENDIAN)
#define GEOPACKAGE_WKB_POINT 1
#define GEOPACKAGE_WKB_HEADER_LEN  ((sizeof(char) + sizeof(int)))


static void
gpkgMakePoint (double x, double y, int srid, unsigned char **result, unsigned int *size)
{
/* build a Blob encoded Geometry representing a POINT */
    unsigned char *ptr;
    int endian_arch = gaiaEndianArch ();
/* computing the Blob size and then allocating it */
    *size = GEOPACKAGE_HEADER_LEN + GEOPACKAGE_2D_ENVELOPE_LEN;
    *size += GEOPACKAGE_WKB_HEADER_LEN;
    *size += (sizeof(double) * 2); /* [x,y] coords */
    *result = malloc (*size);
    memset(*result, 0xD9, *size);
    ptr = *result;
/* setting the Blob value */
    *ptr = GEOPACKAGE_MAGIC1;
    *(ptr + 1) = GEOPACKAGE_MAGIC2;
    *(ptr + 2) = GEOPACKAGE_VERSION;
    *(ptr + 3) = GEOPACKAGE_FLAGS_2D_LITTLEENDIAN;
    gaiaExport32 (ptr + 4, srid, 1, endian_arch);	/* the SRID */
    gaiaExport64 (ptr + GEOPACKAGE_HEADER_LEN, x, 1, endian_arch);	/* MBR - minimum X */
    gaiaExport64 (ptr + GEOPACKAGE_HEADER_LEN + sizeof(double), x, 1, endian_arch);	/* MBR - maximum x */
    gaiaExport64 (ptr + GEOPACKAGE_HEADER_LEN + 2 * sizeof(double), y, 1, endian_arch);	/* MBR - minimum Y */
    gaiaExport64 (ptr + GEOPACKAGE_HEADER_LEN + 3 * sizeof(double), y, 1, endian_arch);	/* MBR - maximum Y */
    *(ptr + GEOPACKAGE_HEADER_LEN + GEOPACKAGE_2D_ENVELOPE_LEN) = GEOPACKAGE_WKB_LITTLEENDIAN;
    gaiaExport32 (ptr + GEOPACKAGE_HEADER_LEN + GEOPACKAGE_2D_ENVELOPE_LEN + 1, GEOPACKAGE_WKB_POINT, 1, endian_arch);
    gaiaExport64 (ptr + GEOPACKAGE_HEADER_LEN + GEOPACKAGE_2D_ENVELOPE_LEN + GEOPACKAGE_WKB_HEADER_LEN, x, 1, endian_arch);
    gaiaExport64 (ptr + GEOPACKAGE_HEADER_LEN + GEOPACKAGE_2D_ENVELOPE_LEN + GEOPACKAGE_WKB_HEADER_LEN + sizeof(double), y, 1, endian_arch);
}

GEOPACKAGE_DECLARE void
fnct_gpkgMakePoint(sqlite3_context * context, int argc UNUSED, sqlite3_value ** argv)
{
/* SQL function:
/ gpkgMakePoint(x, y)
/
/ Creates a GeoPackage geometry POINT
/
/ returns nothing on success, raises exception on error
*/
    unsigned int len;
    int int_value;
    unsigned char *p_result = NULL;
    double x;
    double y;
    GEOPACKAGE_UNUSED ();	/* LCOV_EXCL_LINE */
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
    {
	x = sqlite3_value_double (argv[0]);
    }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
    {
	int_value = sqlite3_value_int (argv[0]);
	x = int_value;
    }
    else
    {
	sqlite3_result_null (context);
	return;
    }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
    {
	y = sqlite3_value_double (argv[1]);
    }
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
    {
	int_value = sqlite3_value_int (argv[1]);
	y = int_value;
    }
    else
    {
	sqlite3_result_null (context);
	return;
    }
    gpkgMakePoint (x, y, 0, &p_result, &len);
    if (!p_result)
    {
	sqlite3_result_null (context);
    }
    else
    {
	sqlite3_result_blob (context, p_result, len, free);
    }
}

GEOPACKAGE_DECLARE void
fnct_gpkgMakePointWithSRID(sqlite3_context * context, int argc UNUSED, sqlite3_value ** argv)
{
/* SQL function:
/ gpkgMakePointWithSRID(x, y, srid)
/
/ Creates a GeoPackage geometry POINT
/
/ returns nothing on success, raises exception on error
*/
    int len;
    int int_value;
    unsigned char *p_result = NULL;
    double x;
    double y;
    int srid;
    GEOPACKAGE_UNUSED ();	/* LCOV_EXCL_LINE */
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
    {
	x = sqlite3_value_double (argv[0]);
    }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
    {
	int_value = sqlite3_value_int (argv[0]);
	x = int_value;
    }
    else
    {
	sqlite3_result_null (context);
	return;
    }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
    {
	y = sqlite3_value_double (argv[1]);
    }
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
    {
	int_value = sqlite3_value_int (argv[1]);
	y = int_value;
    }
    else
    {
	sqlite3_result_null (context);
	return;
    }
    if (sqlite3_value_type (argv[2]) != SQLITE_INTEGER)
    {
        sqlite3_result_null (context);
	return;
    }
    srid = sqlite3_value_int (argv[2]);
    
    gpkgMakePoint (x, y, srid, &p_result, &len);
    if (!p_result)
    {
	sqlite3_result_null (context);
    }
    else
    {
	sqlite3_result_blob (context, p_result, len, free);
    }
}
#endif
