/*

 gaia_control_points.c -- Gaia implementation of RMSE and TPS Control Points
    
 version 4.3, 2015 May 5

 Author: Sandro Furieri a.furieri@lqt.it

 ------------------------------------------------------------------------------
 DISCLAIMER: this source is simply intemded as an interface supporting the
             sources from Grass GIS
			 NOTE: accordingly to the initial license this file is released
			 under GPL2+ terms
 ------------------------------------------------------------------------------
 
 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License
 as published by the Free Software Foundation; either version 2
 of the License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 
*/

#include <stdlib.h>
#include <stdio.h>

#if defined(_WIN32) && !defined(__MINGW32__)
#include "config-msvc.h"
#else
#include "config.h"
#endif

#ifdef ENABLE_CONTROL_POINTS	/* only if ControlPoints enabled */

#include <spatialite_private.h>
#include <spatialite/control_points.h>

#include "grass_crs.h"

GAIACP_DECLARE GaiaControlPointsPtr
gaiaCreateControlPoints (int allocation_incr, int has3d, int tps)
{
/* creating a Control Point set container */
    struct gaia_control_points *cp =
	malloc (sizeof (struct gaia_control_points));
    if (cp == NULL)
	return NULL;
    cp->has3d = has3d;
    cp->tps = tps;
    cp->affine_valid = 0;
    if (allocation_incr < 64)
	allocation_incr = 64;
    cp->allocation_incr = allocation_incr;
    cp->allocated_items = allocation_incr;
    cp->count = 0;
    cp->x0 = malloc (sizeof (double) * allocation_incr);
    cp->y0 = malloc (sizeof (double) * allocation_incr);
    cp->x1 = malloc (sizeof (double) * allocation_incr);
    cp->y1 = malloc (sizeof (double) * allocation_incr);
    if (has3d)
      {
	  cp->z0 = malloc (sizeof (double) * allocation_incr);
	  cp->z1 = malloc (sizeof (double) * allocation_incr);
      }
    else
      {
	  cp->z0 = NULL;
	  cp->z1 = NULL;
      }
    if (cp->x0 == NULL || cp->y0 == NULL || cp->x1 == NULL || cp->y1 == NULL)
	goto error;
    if (has3d && (cp->z0 == NULL || cp->z1 == NULL))
	goto error;
    return (GaiaControlPointsPtr) cp;

  error:
    if (cp->x0 != NULL)
	free (cp->x0);
    if (cp->y0 != NULL)
	free (cp->y0);
    if (cp->z0 != NULL)
	free (cp->z0);
    if (cp->x1 != NULL)
	free (cp->x1);
    if (cp->y1 != NULL)
	free (cp->y1);
    if (cp->z1 != NULL)
	free (cp->z1);
    return NULL;
}

GAIACP_DECLARE int
gaiaAddControlPoint3D (GaiaControlPointsPtr cp_handle, double x0, double y0,
		       double z0, double x1, double y1, double z1)
{
/* inserting a Control Point 3D into the aggregate container */
    struct gaia_control_points *cp = (struct gaia_control_points *) cp_handle;
    if (cp == NULL)
	return 0;
    if (cp->has3d == 0)
	return 0;
    if (cp->allocated_items == cp->count)
      {
	  /* increasing the size of coord arrays */
	  cp->allocated_items += cp->allocation_incr;
	  cp->x0 = realloc (cp->x0, sizeof (double) * cp->allocated_items);
	  cp->y0 = realloc (cp->y0, sizeof (double) * cp->allocated_items);
	  cp->z0 = realloc (cp->z0, sizeof (double) * cp->allocated_items);
	  cp->x1 = realloc (cp->x1, sizeof (double) * cp->allocated_items);
	  cp->y1 = realloc (cp->y1, sizeof (double) * cp->allocated_items);
	  cp->z1 = realloc (cp->z1, sizeof (double) * cp->allocated_items);
      }
    if (cp->x0 == NULL || cp->y0 == NULL || cp->x1 == NULL || cp->y1 == NULL
	|| cp->z0 == NULL || cp->z1 == NULL)
	return 0;
    cp->x0[cp->count] = x0;
    cp->y0[cp->count] = y0;
    cp->z0[cp->count] = z0;
    cp->x1[cp->count] = x1;
    cp->y1[cp->count] = y1;
    cp->z1[cp->count] = z1;
    cp->count += 1;
    return 1;
}

GAIACP_DECLARE int
gaiaAddControlPoint2D (GaiaControlPointsPtr cp_handle, double x0, double y0,
		       double x1, double y1)
{
/* inserting a Control Point 2D into the aggregate container */
    struct gaia_control_points *cp = (struct gaia_control_points *) cp_handle;
    if (cp == NULL)
	return 0;
    if (cp->has3d)
	return 0;
    if (cp->allocated_items == cp->count)
      {
	  /* increasing the size of coord arrays */
	  cp->allocated_items += 1024;
	  cp->x0 = realloc (cp->x0, sizeof (double) * cp->allocated_items);
	  cp->y0 = realloc (cp->y0, sizeof (double) * cp->allocated_items);
	  cp->x1 = realloc (cp->x1, sizeof (double) * cp->allocated_items);
	  cp->y1 = realloc (cp->y1, sizeof (double) * cp->allocated_items);
      }
    if (cp->x0 == NULL || cp->y0 == NULL || cp->x1 == NULL || cp->y1 == NULL)
	return 0;
    cp->x0[cp->count] = x0;
    cp->y0[cp->count] = y0;
    cp->x1[cp->count] = x1;
    cp->y1[cp->count] = y1;
    cp->count += 1;
    return 1;
}

GAIACP_DECLARE void
gaiaFreeControlPoints (GaiaControlPointsPtr cp_handle)
{
/* memory cleanup */
    struct gaia_control_points *cp = (struct gaia_control_points *) cp_handle;
    if (cp == NULL)
	return;
    free (cp->x0);
    free (cp->y0);
    free (cp->x1);
    free (cp->y1);
    if (cp->has3d)
      {
	  free (cp->z0);
	  free (cp->z1);
      }
    free (cp);
}


static void
copy_control_points_2d (struct gaia_control_points *gaia_cp,
			struct Control_Points *cp)
{
/* initializing Grass 2D Control Points */
    int i;
    cp->count = gaia_cp->count;
    cp->e1 = malloc (sizeof (double) * cp->count);
    cp->e2 = malloc (sizeof (double) * cp->count);
    cp->n1 = malloc (sizeof (double) * cp->count);
    cp->n2 = malloc (sizeof (double) * cp->count);
    cp->status = malloc (sizeof (double) * cp->count);
    for (i = 0; i < cp->count; i++)
      {
	  cp->e1[i] = gaia_cp->x0[i];
	  cp->e2[i] = gaia_cp->x1[i];
	  cp->n1[i] = gaia_cp->y0[i];
	  cp->n2[i] = gaia_cp->y1[i];
	  cp->status[i] = 1;
      }
}

static void
copy_control_points_3d (struct gaia_control_points *gaia_cp,
			struct Control_Points_3D *cp)
{
/* initializing Grass 2D Control Points */
    int i;
    cp->count = gaia_cp->count;
    cp->e1 = malloc (sizeof (double) * cp->count);
    cp->e2 = malloc (sizeof (double) * cp->count);
    cp->n1 = malloc (sizeof (double) * cp->count);
    cp->n2 = malloc (sizeof (double) * cp->count);
    cp->z1 = malloc (sizeof (double) * cp->count);
    cp->z2 = malloc (sizeof (double) * cp->count);
    cp->status = malloc (sizeof (double) * cp->count);
    for (i = 0; i < cp->count; i++)
      {
	  cp->e1[i] = gaia_cp->x0[i];
	  cp->e2[i] = gaia_cp->x1[i];
	  cp->n1[i] = gaia_cp->y0[i];
	  cp->n2[i] = gaia_cp->y1[i];
	  cp->z1[i] = gaia_cp->z0[i];
	  cp->z2[i] = gaia_cp->z1[i];
	  cp->status[i] = 1;
      }
}

static void
free_control_points_2d (struct Control_Points *cp)
{
/* freeing Grass 2D Control Points */
    if (cp->e1 != NULL)
	free (cp->e1);
    if (cp->e2 != NULL)
	free (cp->e2);
    if (cp->n1 != NULL)
	free (cp->n1);
    if (cp->n2 != NULL)
	free (cp->n2);
    if (cp->status != NULL)
	free (cp->status);
}

static void
free_control_points_3d (struct Control_Points_3D *cp)
{
/* freeing Grass 3D Control Points */
    if (cp->e1 != NULL)
	free (cp->e1);
    if (cp->e2 != NULL)
	free (cp->e2);
    if (cp->n1 != NULL)
	free (cp->n1);
    if (cp->n2 != NULL)
	free (cp->n2);
    if (cp->z1 != NULL)
	free (cp->z1);
    if (cp->z2 != NULL)
	free (cp->z2);
    if (cp->status != NULL)
	free (cp->status);
}

GAIACP_DECLARE int
gaiaAffineFromControlPoints (GaiaControlPointsPtr cp_handle)
{
/* creating an Affine Transform from the Control Points */
    struct Control_Points cp;
    struct Control_Points_3D cp3;
    int ret = 0;
    int use3d;
    int orthorot = 0;
    int order = 1;
    int order_pnts[2][3] = { {3, 6, 10}, {4, 10, 20} };

    double E12[20];
    double N12[20];
    double Z12[20];
    double E21[20];
    double N21[20];
    double Z21[20];
    double *E12_t = NULL;
    double *N12_t = NULL;
    double *E21_t = NULL;
    double *N21_t = NULL;

    struct gaia_control_points *gaia_cp =
	(struct gaia_control_points *) cp_handle;
    if (gaia_cp == NULL)
	return 0;

    if (gaia_cp == NULL)
	return 0;

    cp.count = 0;
    cp.e1 = NULL;
    cp.e2 = NULL;
    cp.n1 = NULL;
    cp.n2 = NULL;
    cp.status = NULL;

    cp3.count = 0;
    cp3.e1 = NULL;
    cp3.e2 = NULL;
    cp3.n1 = NULL;
    cp3.n2 = NULL;
    cp3.z1 = NULL;
    cp3.z2 = NULL;
    cp3.status = NULL;

    use3d = gaia_cp->has3d;
    if (use3d)
      {
	  /* 3D control points */
	  copy_control_points_3d (gaia_cp, &cp3);
	  ret =
	      CRS_compute_georef_equations_3d (&cp3, E12, N12, Z12, E21, N21,
					       Z21, order);
      }
    else
      {
	  /* 2D control points */
	  copy_control_points_2d (gaia_cp, &cp);
	  if (gaia_cp->tps)
	      ret =
		  I_compute_georef_equations_tps (&cp, &E12_t, &N12_t, &E21_t,
						  &N21_t);
	  else
	      ret = I_compute_georef_equations (&cp, E12, N12, E21, N21, order);
      }
fprintf(stderr, "ret=%d tps=%d\n", ret, gaia_cp->tps);

    switch (ret)
      {
      case 0:
	  fprintf (stderr,
		   "Not enough active control points for current order, %d are required.\n",
		   (orthorot ? 3 : order_pnts[use3d != 0][order - 1]));
	  break;
      case -1:
	  fprintf (stderr,
		   "Poorly placed control points.\nCan not generate the transformation equation.\n");
	  break;
      case -2:
	  fprintf (stderr,
		   "Not enough memory to solve for transformation equation\n");
	  break;
      case -3:
	  fprintf (stderr, "Invalid order\n");
	  break;
      default:
	  break;
      }

    if (use3d)
	free_control_points_3d (&cp3);
    else
	free_control_points_2d (&cp);

    if (ret > 0)
      {
	  if (use3d)
	    {
		gaia_cp->a = E12[1];
		gaia_cp->b = E12[2];
		gaia_cp->c = E12[3];
		gaia_cp->d = N12[1];
		gaia_cp->e = N12[2];
		gaia_cp->f = N12[3];
		gaia_cp->g = Z12[1];
		gaia_cp->h = Z12[2];
		gaia_cp->i = Z12[3];
		gaia_cp->xoff = E12[0];
		gaia_cp->yoff = N12[0];
		gaia_cp->zoff = Z12[0];
	    }
	  else
	    {
		if (gaia_cp->tps)
		  {
		      gaia_cp->a = E12_t[1];
		      gaia_cp->b = E12_t[2];
		      gaia_cp->d = N12_t[1];
		      gaia_cp->e = N12_t[2];
		      gaia_cp->xoff = E12_t[0];
		      gaia_cp->yoff = N12_t[0];
		      gaia_cp->affine_valid = 1;
fprintf(stderr, "pl\n");
		  }
		else
		  {
		      gaia_cp->a = E12[1];
		      gaia_cp->b = E12[2];
		      gaia_cp->d = N12[1];
		      gaia_cp->e = N12[2];
		      gaia_cp->xoff = E12[0];
		      gaia_cp->yoff = N12[0];
		      gaia_cp->affine_valid = 1;
		  }
	    }
      }

    if (E12_t != NULL)
	free (E12_t);
    if (N12_t != NULL)
	free (N12_t);
    if (E21_t != NULL)
	free (E21_t);
    if (N21_t != NULL)
	free (N21_t);

    if (ret > 0)
	return 1;
    return 0;
}

#endif	/* end including CONTROL_POINTS */
