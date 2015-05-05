/*

 gg_matrix.c -- Gaia Affine Transform Matrix support
    
 version 4.3, 2015 April 28

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
 
Portions created by the Initial Developer are Copyright (C) 2012-2013
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

#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#if defined(_WIN32) && !defined(__MINGW32__)
#include "config-msvc.h"
#else
#include "config.h"
#endif

#include <spatialite/sqlite.h>

#include <spatialite/gaiageo.h>
#include <spatialite/gaiamatrix.h>

#define MATRIX_MAGIC_START		0x00
#define MATRIX_MAGIC_DELIMITER	0x3a
#define MATRIX_MAGIC_END		0xb3

struct at_matrix
{
/* 3D Affine Transform Matrix */
    double xx;
    double xy;
    double xz;
    double xoff;
    double yx;
    double yy;
    double yz;
    double yoff;
    double zx;
    double zy;
    double zz;
    double zoff;
    double w1;
    double w2;
    double w3;
    double w4;
};

static int
blob_matrix_encode (struct at_matrix *matrix, unsigned char **blob,
		    int *blob_sz)
{
/* creating a BLOB-Matrix object */
    unsigned char *xblob = NULL;
    unsigned char *ptr;
    int xblob_sz = 146;
    int endian_arch = gaiaEndianArch ();

    *blob = NULL;
    *blob_sz = 0;

    xblob = malloc (xblob_sz);
    if (xblob == NULL)
	return 0;
    ptr = xblob;
/* encoding the BLOB */
    *ptr = MATRIX_MAGIC_START;	/* START signature */
    *(ptr + 1) = 1;		/* LITTLE ENDIAN */
    gaiaExport64 (ptr + 2, matrix->xx, 1, endian_arch);
    *(ptr + 10) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 11, matrix->xy, 1, endian_arch);
    *(ptr + 19) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 20, matrix->xz, 1, endian_arch);
    *(ptr + 28) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 29, matrix->xoff, 1, endian_arch);
    *(ptr + 37) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 38, matrix->yx, 1, endian_arch);
    *(ptr + 46) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 47, matrix->yy, 1, endian_arch);
    *(ptr + 55) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 56, matrix->yz, 1, endian_arch);
    *(ptr + 64) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 65, matrix->yoff, 1, endian_arch);
    *(ptr + 73) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 74, matrix->zx, 1, endian_arch);
    *(ptr + 82) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 83, matrix->zy, 1, endian_arch);
    *(ptr + 91) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 92, matrix->zz, 1, endian_arch);
    *(ptr + 100) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 101, matrix->zoff, 1, endian_arch);
    *(ptr + 109) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 110, matrix->w1, 1, endian_arch);
    *(ptr + 118) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 119, matrix->w2, 1, endian_arch);
    *(ptr + 127) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 128, matrix->w3, 1, endian_arch);
    *(ptr + 136) = MATRIX_MAGIC_DELIMITER;
    gaiaExport64 (ptr + 137, matrix->w4, 1, endian_arch);
    *(ptr + 145) = MATRIX_MAGIC_END;

    *blob = xblob;
    *blob_sz = xblob_sz;
    return 1;
}

static int
blob_matrix_decode (struct at_matrix *matrix, const unsigned char *blob,
		    int blob_sz)
{
/* decoding a BLOB-Matrix object */
    int endian;
    int endian_arch = gaiaEndianArch ();
    const unsigned char *ptr = blob;
    if (blob == NULL)
	return 0;
    if (blob_sz != 146)
	return 0;

    if (*ptr != MATRIX_MAGIC_START)
	return 0;
    if (*(ptr + 1) == 1)
	endian = 1;
    else if (*(ptr + 1) == 0)
	endian = 0;
    else
	return 0;
    matrix->xx = gaiaImport64 (ptr + 2, endian, endian_arch);
    matrix->xy = gaiaImport64 (ptr + 11, endian, endian_arch);
    matrix->xz = gaiaImport64 (ptr + 20, endian, endian_arch);
    matrix->xoff = gaiaImport64 (ptr + 29, endian, endian_arch);
    matrix->yx = gaiaImport64 (ptr + 38, endian, endian_arch);
    matrix->yy = gaiaImport64 (ptr + 47, endian, endian_arch);
    matrix->yz = gaiaImport64 (ptr + 56, endian, endian_arch);
    matrix->yoff = gaiaImport64 (ptr + 65, endian, endian_arch);
    matrix->zx = gaiaImport64 (ptr + 74, endian, endian_arch);
    matrix->zy = gaiaImport64 (ptr + 83, endian, endian_arch);
    matrix->zz = gaiaImport64 (ptr + 92, endian, endian_arch);
    matrix->zoff = gaiaImport64 (ptr + 101, endian, endian_arch);
    matrix->w1 = gaiaImport64 (ptr + 110, endian, endian_arch);
    matrix->w2 = gaiaImport64 (ptr + 119, endian, endian_arch);
    matrix->w3 = gaiaImport64 (ptr + 128, endian, endian_arch);
    matrix->w4 = gaiaImport64 (ptr + 137, endian, endian_arch);
    if (*(ptr + 145) != MATRIX_MAGIC_END)
	return 0;
    return 1;
}

GAIAMATRIX_DECLARE int
gaia_matrix_create (double a, double b, double c, double d, double e, double f,
		    double g, double h, double i, double xoff, double yoff,
		    double zoff, unsigned char **blob, int *blob_sz)
{
/*
* creating a BLOB-serialized Affine Transform Matrix
* iniziatialized with explicit values
*/
    struct at_matrix matrix;
    matrix.xx = a;
    matrix.xy = b;
    matrix.xz = c;
    matrix.xoff = xoff;
    matrix.yx = d;
    matrix.yy = e;
    matrix.yz = f;
    matrix.yoff = yoff;
    matrix.zx = g;
    matrix.zy = h;
    matrix.zz = i;
    matrix.zoff = zoff;
    matrix.w1 = 0.0;
    matrix.w2 = 0.0;
    matrix.w3 = 0.0;
    matrix.w4 = 1.0;
    return blob_matrix_encode (&matrix, blob, blob_sz);
}

static void
matrix_multiply (struct at_matrix *result, struct at_matrix *matrix1,
		 struct at_matrix *matrix2)
{
/* multiplying MatrixA by MatrixB */
    result->xx =
	(matrix1->xx * matrix2->xx) + (matrix1->xy * matrix2->yx) +
	(matrix1->xz * matrix2->zx) + (matrix1->xoff * matrix2->w1);
    result->xy =
	(matrix1->xx * matrix2->xy) + (matrix1->xy * matrix2->yy) +
	(matrix1->xz * matrix2->zy) + (matrix1->xoff * matrix2->w2);
    result->xz =
	(matrix1->xx * matrix2->xz) + (matrix1->xy * matrix2->yz) +
	(matrix1->xz * matrix2->zz) + (matrix1->xoff * matrix2->w3);
    result->xoff =
	(matrix1->xx * matrix2->xoff) + (matrix1->xy * matrix2->yoff) +
	(matrix1->xz * matrix2->zoff) + (matrix1->xoff * matrix2->w4);

    result->yx =
	(matrix1->yx * matrix2->xx) + (matrix1->yy * matrix2->yx) +
	(matrix1->yz * matrix2->zx) + (matrix1->yoff * matrix2->w1);
    result->yy =
	(matrix1->yx * matrix2->xy) + (matrix1->yy * matrix2->yy) +
	(matrix1->yz * matrix2->zy) + (matrix1->yoff * matrix2->w2);
    result->yz =
	(matrix1->yx * matrix2->xz) + (matrix1->yy * matrix2->yz) +
	(matrix1->yz * matrix2->zz) + (matrix1->yoff * matrix2->w3);
    result->yoff =
	(matrix1->yx * matrix2->xoff) + (matrix1->yy * matrix2->yoff) +
	(matrix1->yz * matrix2->zoff) + (matrix1->yoff * matrix2->w4);

    result->zx =
	(matrix1->zx * matrix2->xx) + (matrix1->zy * matrix2->yx) +
	(matrix1->zz * matrix2->zx) + (matrix1->zoff * matrix2->w1);
    result->zy =
	(matrix1->zx * matrix2->xy) + (matrix1->zy * matrix2->yy) +
	(matrix1->zz * matrix2->zy) + (matrix1->zoff * matrix2->w2);
    result->zz =
	(matrix1->zx * matrix2->xz) + (matrix1->zy * matrix2->yz) +
	(matrix1->zz * matrix2->zz) + (matrix1->zoff * matrix2->w3);
    result->zoff =
	(matrix1->zx * matrix2->xoff) + (matrix1->zy * matrix2->yoff) +
	(matrix1->zz * matrix2->zoff) + (matrix1->zoff * matrix2->w4);

    result->w1 =
	(matrix1->w1 * matrix2->xx) + (matrix1->w2 * matrix2->yx) +
	(matrix1->w3 * matrix2->zx) + (matrix1->w4 * matrix2->w1);
    result->w2 =
	(matrix1->w1 * matrix2->xy) + (matrix1->w2 * matrix2->yy) +
	(matrix1->w3 * matrix2->zy) + (matrix1->w4 * matrix2->w2);
    result->w3 =
	(matrix1->w1 * matrix2->xz) + (matrix1->w2 * matrix2->yz) +
	(matrix1->w3 * matrix2->zz) + (matrix1->w4 * matrix2->w3);
    result->w4 =
	(matrix1->w1 * matrix2->xoff) + (matrix1->w2 * matrix2->yoff) +
	(matrix1->w3 * matrix2->zoff) + (matrix1->w4 * matrix2->w4);
}

GAIAMATRIX_DECLARE int
gaia_matrix_multiply (const unsigned char *iblob1, int iblob1_sz,
		      const unsigned char *iblob2, int iblob2_sz,
		      unsigned char **blob, int *blob_sz)
{
/*
* creating a BLOB-serialized Affine Transform Matrix
* by multiplying MatrixA by MatrixB
*/
    struct at_matrix matrix1;
    struct at_matrix matrix2;
    struct at_matrix result;

    *blob = NULL;
    *blob_sz = 0;
    if (!blob_matrix_decode (&matrix1, iblob1, iblob1_sz))
	return 0;
    if (!blob_matrix_decode (&matrix2, iblob2, iblob2_sz))
	return 0;
    matrix_multiply (&result, &matrix1, &matrix2);
    return blob_matrix_encode (&result, blob, blob_sz);
}

GAIAMATRIX_DECLARE int
gaia_matrix_create_multiply (const unsigned char *iblob, int iblob_sz, double a,
			     double b, double c, double d, double e, double f,
			     double g, double h, double i, double xoff,
			     double yoff, double zoff, unsigned char **blob,
			     int *blob_sz)
{
/*
* creating a BLOB-serialized Affine Transform Matrix
* iniziatialized with explicit values
*/
    struct at_matrix old_matrix;
    struct at_matrix matrix;
    struct at_matrix result;
    matrix.xx = a;
    matrix.xy = b;
    matrix.xz = c;
    matrix.xoff = xoff;
    matrix.yx = d;
    matrix.yy = e;
    matrix.yz = f;
    matrix.yoff = yoff;
    matrix.zx = g;
    matrix.zy = h;
    matrix.zz = i;
    matrix.zoff = zoff;
    matrix.w1 = 0.0;
    matrix.w2 = 0.0;
    matrix.w3 = 0.0;
    matrix.w4 = 1.0;
    *blob = NULL;
    *blob_sz = 0;
    if (!blob_matrix_decode (&old_matrix, iblob, iblob_sz))
	return 0;
    matrix_multiply (&result, &matrix, &old_matrix);
    return blob_matrix_encode (&result, blob, blob_sz);
}

GAIAMATRIX_DECLARE int
gaia_matrix_is_valid (const unsigned char *blob, int blob_sz)
{
/* checking a BLOB-ATM object for validity */
    const unsigned char *ptr = blob;
    if (blob == NULL)
	return 0;
    if (blob_sz != 146)
	return 0;

    if (*ptr != MATRIX_MAGIC_START)
	return 0;
    if (*(ptr + 1) == 1 || *(ptr + 1) == 0)
	;
    else
	return 0;
    if (*(ptr + 10) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 19) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 28) != MATRIX_MAGIC_DELIMITER)
	return 0;;
    if (*(ptr + 37) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 46) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 55) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 64) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 73) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 82) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 91) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 100) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 109) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 118) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 127) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 136) != MATRIX_MAGIC_DELIMITER)
	return 0;
    if (*(ptr + 145) != MATRIX_MAGIC_END)
	return 0;
    return 1;
}

GAIAMATRIX_DECLARE char *
gaia_matrix_as_text (const unsigned char *blob, int blob_sz)
{
/* printing a BLOB-AMT object as a text string */
    char *text;
    struct at_matrix matrix;
    if (!gaia_matrix_is_valid (blob, blob_sz))
	return NULL;
    if (!blob_matrix_decode (&matrix, blob, blob_sz))
	return NULL;

/* printing the AT Matrix as text */
    text =
	sqlite3_mprintf
	("%1.10f %1.10f %1.10f %1.10f\n%1.10f %1.10f %1.10f %1.10f\n"
	 "%1.10f %1.10f %1.10f %1.10f\n%1.10f %1.10f %1.10f %1.10f\n",
	 matrix.xx, matrix.xy, matrix.xz, matrix.xoff, matrix.yx, matrix.yy,
	 matrix.yz, matrix.yoff, matrix.zx, matrix.zy, matrix.zz, matrix.zoff,
	 matrix.w1, matrix.w2, matrix.w3, matrix.w4);
    return text;
}

static void
gaia_point_transform3D (struct at_matrix *matrix, double *x, double *y,
			double *z)
{
/* Affine Transform 3D */
    double x0 = *x;
    double y0 = *y;
    double z0 = *z;
    *x = (matrix->xx * x0) + (matrix->xy * y0) + (matrix->xz * z0) +
	matrix->xoff;
    *y = (matrix->yx * x0) + (matrix->yy * y0) + (matrix->yz * z0) +
	matrix->yoff;
    *z = (matrix->zx * x0) + (matrix->zy * y0) + (matrix->zz * z0) +
	matrix->zoff;
}


static void
gaia_point_transform2D (struct at_matrix *matrix, double *x, double *y)
{
/* Affine Transform 2D */
    double x0 = *x;
    double y0 = *y;
    *x = (matrix->xx * x0) + (matrix->xy * y0) + matrix->xoff;
    *y = (matrix->yx * x0) + (matrix->yy * y0) + matrix->yoff;
}

GAIAMATRIX_DECLARE gaiaGeomCollPtr
gaia_matrix_transform_geometry (gaiaGeomCollPtr geom,
				const unsigned char *blob, int blob_sz)
{
/* transforming a Geometry by applying an Affine Transform Matrix */
    int iv;
    int ib;
    double x;
    double y;
    double z;
    double m;
    gaiaPointPtr point;
    gaiaLinestringPtr line;
    gaiaLinestringPtr new_line;
    gaiaPolygonPtr polyg;
    gaiaPolygonPtr new_polyg;
    gaiaGeomCollPtr new_geom;
    gaiaRingPtr i_ring;
    gaiaRingPtr o_ring;
    struct at_matrix matrix;
    if (!gaia_matrix_is_valid (blob, blob_sz))
	return NULL;
    if (!blob_matrix_decode (&matrix, blob, blob_sz))
	return NULL;
    if (geom == NULL)
	return NULL;

/* creating the output Geometry */
    if (geom->DimensionModel == GAIA_XY_Z)
	new_geom = gaiaAllocGeomCollXYZ ();
    else if (geom->DimensionModel == GAIA_XY_M)
	new_geom = gaiaAllocGeomCollXYM ();
    else if (geom->DimensionModel == GAIA_XY_Z_M)
	new_geom = gaiaAllocGeomCollXYZM ();
    else
	new_geom = gaiaAllocGeomColl ();
    new_geom->Srid = geom->Srid;
    new_geom->DeclaredType = geom->DeclaredType;

/* cloning and transforming all individual items */
    point = geom->FirstPoint;
    while (point)
      {
	  /* copying POINTs */
	  if (geom->DimensionModel == GAIA_XY_Z)
	    {
		x = point->X;
		y = point->Y;
		z = point->Z;
		gaia_point_transform3D (&matrix, &x, &y, &z);
		gaiaAddPointToGeomCollXYZ (new_geom, x, y, z);
	    }
	  else if (geom->DimensionModel == GAIA_XY_M)
	    {
		x = point->X;
		y = point->Y;
		m = point->M;
		gaia_point_transform2D (&matrix, &x, &y);
		gaiaAddPointToGeomCollXYM (new_geom, x, y, m);
	    }
	  else if (geom->DimensionModel == GAIA_XY_Z_M)
	    {
		x = point->X;
		y = point->Y;
		z = point->Z;
		m = point->M;
		gaia_point_transform3D (&matrix, &x, &y, &z);
		gaiaAddPointToGeomCollXYZM (new_geom, x, y, z, m);
	    }
	  else
	    {
		x = point->X;
		y = point->Y;
		gaia_point_transform2D (&matrix, &x, &y);
		gaiaAddPointToGeomColl (new_geom, x, y);
	    }
	  point = point->Next;
      }

    line = geom->FirstLinestring;
    while (line)
      {
	  /* copying LINESTRINGs */
	  new_line = gaiaAddLinestringToGeomColl (new_geom, line->Points);
	  for (iv = 0; iv < line->Points; iv++)
	    {
		z = 0.0;
		m = 0.0;
		if (line->DimensionModel == GAIA_XY_Z)
		  {
		      gaiaGetPointXYZ (line->Coords, iv, &x, &y, &z);
		  }
		else if (line->DimensionModel == GAIA_XY_M)
		  {
		      gaiaGetPointXYM (line->Coords, iv, &x, &y, &m);
		  }
		else if (line->DimensionModel == GAIA_XY_Z_M)
		  {
		      gaiaGetPointXYZM (line->Coords, iv, &x, &y, &z, &m);
		  }
		else
		  {
		      gaiaGetPoint (line->Coords, iv, &x, &y);
		  }
		if (new_line->DimensionModel == GAIA_XY_Z
		    || new_line->DimensionModel == GAIA_XY_Z_M)
		    gaia_point_transform3D (&matrix, &x, &y, &z);
		else
		    gaia_point_transform2D (&matrix, &x, &y);
		if (new_line->DimensionModel == GAIA_XY_Z)
		  {
		      gaiaSetPointXYZ (new_line->Coords, iv, x, y, z);
		  }
		else if (new_line->DimensionModel == GAIA_XY_M)
		  {
		      gaiaSetPointXYM (new_line->Coords, iv, x, y, m);
		  }
		else if (new_line->DimensionModel == GAIA_XY_Z_M)
		  {
		      gaiaSetPointXYZM (new_line->Coords, iv, x, y, z, m);
		  }
		else
		  {
		      gaiaSetPoint (new_line->Coords, iv, x, y);
		  }
	    }
	  line = line->Next;
      }

    polyg = geom->FirstPolygon;
    while (polyg)
      {
	  /* copying POLYGONs */
	  i_ring = polyg->Exterior;
	  new_polyg =
	      gaiaAddPolygonToGeomColl (new_geom, i_ring->Points,
					polyg->NumInteriors);
	  o_ring = new_polyg->Exterior;
	  /* copying points for the EXTERIOR RING */
	  for (iv = 0; iv < o_ring->Points; iv++)
	    {
		z = 0.0;
		m = 0.0;
		if (i_ring->DimensionModel == GAIA_XY_Z)
		  {
		      gaiaGetPointXYZ (i_ring->Coords, iv, &x, &y, &z);
		  }
		else if (i_ring->DimensionModel == GAIA_XY_M)
		  {
		      gaiaGetPointXYM (i_ring->Coords, iv, &x, &y, &m);
		  }
		else if (i_ring->DimensionModel == GAIA_XY_Z_M)
		  {
		      gaiaGetPointXYZM (i_ring->Coords, iv, &x, &y, &z, &m);
		  }
		else
		  {
		      gaiaGetPoint (i_ring->Coords, iv, &x, &y);
		  }
		if (o_ring->DimensionModel == GAIA_XY_Z
		    || o_ring->DimensionModel == GAIA_XY_Z_M)
		    gaia_point_transform3D (&matrix, &x, &y, &z);
		else
		    gaia_point_transform2D (&matrix, &x, &y);
		if (o_ring->DimensionModel == GAIA_XY_Z)
		  {
		      gaiaSetPointXYZ (o_ring->Coords, iv, x, y, z);
		  }
		else if (o_ring->DimensionModel == GAIA_XY_M)
		  {
		      gaiaSetPointXYM (o_ring->Coords, iv, x, y, m);
		  }
		else if (o_ring->DimensionModel == GAIA_XY_Z_M)
		  {
		      gaiaSetPointXYZM (o_ring->Coords, iv, x, y, z, m);
		  }
		else
		  {
		      gaiaSetPoint (o_ring->Coords, iv, x, y);
		  }
	    }
	  for (ib = 0; ib < new_polyg->NumInteriors; ib++)
	    {
		/* copying each INTERIOR RING [if any] */
		i_ring = polyg->Interiors + ib;
		o_ring = gaiaAddInteriorRing (new_polyg, ib, i_ring->Points);
		for (iv = 0; iv < o_ring->Points; iv++)
		  {
		      z = 0.0;
		      m = 0.0;
		      if (i_ring->DimensionModel == GAIA_XY_Z)
			{
			    gaiaGetPointXYZ (i_ring->Coords, iv, &x, &y, &z);
			}
		      else if (i_ring->DimensionModel == GAIA_XY_M)
			{
			    gaiaGetPointXYM (i_ring->Coords, iv, &x, &y, &m);
			}
		      else if (i_ring->DimensionModel == GAIA_XY_Z_M)
			{
			    gaiaGetPointXYZM (i_ring->Coords, iv, &x, &y, &z,
					      &m);
			}
		      else
			{
			    gaiaGetPoint (i_ring->Coords, iv, &x, &y);
			}
		      if (o_ring->DimensionModel == GAIA_XY_Z
			  || o_ring->DimensionModel == GAIA_XY_Z_M)
			  gaia_point_transform3D (&matrix, &x, &y, &z);
		      else
			  gaia_point_transform2D (&matrix, &x, &y);
		      if (o_ring->DimensionModel == GAIA_XY_Z)
			{
			    gaiaSetPointXYZ (o_ring->Coords, iv, x, y, z);
			}
		      else if (o_ring->DimensionModel == GAIA_XY_M)
			{
			    gaiaSetPointXYM (o_ring->Coords, iv, x, y, m);
			}
		      else if (o_ring->DimensionModel == GAIA_XY_Z_M)
			{
			    gaiaSetPointXYZM (o_ring->Coords, iv, x, y, z, m);
			}
		      else
			{
			    gaiaSetPoint (o_ring->Coords, iv, x, y);
			}
		  }
	    }
	  polyg = polyg->Next;
      }
    return new_geom;
}
