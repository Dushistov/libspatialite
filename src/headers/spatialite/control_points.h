/* 
 control_points.h -- Gaia implementation of RMSE and TPS Control Points
  
 version 4.3, 2015 May 5

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

/**
 \file control_points.h

 Auxiliary/helper functions
 */
#ifndef DOXYGEN_SHOULD_SKIP_THIS
#ifdef DLL_EXPORT
#define GAIACP_DECLARE __declspec(dllexport)
#else
#define GAIACP_DECLARE extern
#endif
#endif

#ifndef _GAIACP_H
#ifndef DOXYGEN_SHOULD_SKIP_THIS
#define _GAIACP_H
#endif

#ifdef __cplusplus
extern "C"
{
#endif

    typedef struct opaque_control_points GaiaControlPoints;
    typedef GaiaControlPoints *GaiaControlPointsPtr;

/* function prototypes */

/**
 Creates a Control Points container (opaque object)

 \param allocation_incr how many Control Points should be allocated
 every time that necessity arises to increment the internal storage
 \param has3d true if the Control Points are all expected to be 3D
 \param tps true if the solution method must be Thin Plate Spline

 \return the handle of the container object, or NULL on failure

 \sa gaiaFreeControlPoints, gaiaAddControlPoint3D, gaiaAddControlPoint2D,
  gaiaAffineFromControlPoints

 \note you must properly destroy the container object when it 
 isn't any longer used.
 */
    GAIACP_DECLARE GaiaControlPointsPtr gaiaCreateControlPoints (int
								 allocation_incr,
								 int has3d,
								 int tps);

/**
 Destroys a Control Points container (opaque object)

 \param cp_handle the handle identifying the container object  
 (returned by a previous call to gaiaCreateControlPoints).

 \sa gaiaCreateControlPoints
 */
    GAIACP_DECLARE void gaiaFreeControlPoints (GaiaControlPointsPtr cp_handle);

/**
 Add a further Control Point 3D to the container (opaque object)

 \param cp_handle the handle identifying the container object  
 (returned by a previous call to gaiaCreateControlPoints).
 \param x0 X coordinate of the first Point.
 \param y0 Y coordinate of the first Point.
 \param z0 Z coordinate of the first Point.
 \param x1 X coordinate of the second Point.
 \param y1 Y coordinate of the second Point.
 \param z1 Z coordinate of the second Point.
 
 \return true on succes, false on failure

 \sa gaiaCreateControlPoints, gaiaAddControlPoint2D
 */
    GAIACP_DECLARE int gaiaAddControlPoint3D (GaiaControlPointsPtr cp_handle,
					      double x0, double y0, double z0,
					      double x1, double y1, double z1);

/**
 Add a further Control Point 2D to the container (opaque object)

 \param cp_handle the handle identifying the container object  
 (returned by a previous call to gaiaCreateControlPoints).
 \param x0 X coordinate of the first Point.
 \param y0 Y coordinate of the first Point.
 \param x1 X coordinate of the second Point.
 \param y1 Y coordinate of the second Point.
 
 \return true on succes, false on failure

 \sa gaiaCreateControlPoints, gaiaAddControlPoint3D
 */
    GAIACP_DECLARE int gaiaAddControlPoint2D (GaiaControlPointsPtr cp_handle,
					      double x0, double y0, double x1,
					      double y1);

/**
 Resolves a Control Point set by computing an Affine Transform Matrix

 \param cp_handle the handle identifying the container object  
 (returned by a previous call to gaiaCreateControlPoints).
 
 \return true on succes, false on failure

 \sa gaiaCreateControlPoints
 */
    GAIACP_DECLARE int gaiaAffineFromControlPoints (GaiaControlPointsPtr
						    cp_handle);

#ifdef __cplusplus
}
#endif

#endif				/* _GAIACP_H */
