/*

 gaia_auxtopo.c -- implementation of the Topology module methods
    
 version 4.3, 2015 July 15

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

/*
 
CREDITS:

this module has been completely funded by:
Regione Toscana - Settore Sistema Informativo Territoriale ed Ambientale
(Topology support) 

*/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <float.h>

#if defined(_WIN32) && !defined(__MINGW32__)
#include "config-msvc.h"
#else
#include "config.h"
#endif

#ifdef POSTGIS_2_2		/* only if TOPOLOGY is enabled */

#include <spatialite/sqlite.h>
#include <spatialite/debug.h>
#include <spatialite/gaiageo.h>
#include <spatialite/gaia_topology.h>
#include <spatialite/gaia_network.h>
#include <spatialite/gaiaaux.h>

#include <spatialite.h>
#include <spatialite_private.h>

#include <liblwgeom.h>
#include <liblwgeom_topo.h>
#include <geos_c.h>

#include <lwn_network.h>

#include "topology_private.h"
#include "network_private.h"

#define GAIA_UNUSED() if (argc || argv) argc = argc;

struct aux_split_outer_edge
{
/* helper struct for split Edge (outer) */
    gaiaLinestringPtr edge;
    struct aux_split_outer_edge *next;
};

struct aux_split_inner_edge_candidate
{
/* helper struct for split Edge (inner candidates) */
    struct aux_split_node *end;
    double length;
    int done;
    struct aux_split_inner_edge_candidate *next;
};

struct aux_split_node
{
/* helper struct for split Nodes */
    int visited;
    int has_z;
    double x;
    double y;
    double z;
    struct aux_split_inner_edge_candidate *first;
    struct aux_split_inner_edge_candidate *last;
    struct aux_split_node *next;
};

struct aux_split_inner_edge
{
/* helper struct for split Edge (inner) */
    struct aux_split_node *start;
    struct aux_split_node *end;
    gaiaLinestringPtr edge;
    double length;
    int confirmed;
    struct aux_split_inner_edge *next;
};

struct aux_split_polygon
{
/* helper struct for split Polygon */
    int has_z;
    struct aux_split_outer_edge *first_outer;
    struct aux_split_outer_edge *last_outer;
    struct aux_split_node *first_node;
    struct aux_split_node *last_node;
    struct aux_split_inner_edge *first_inner;
    struct aux_split_inner_edge *last_inner;
};

struct aux_split_outer_edge *
create_aux_split_outer_edge (gaiaLinestringPtr edge)
{
/* creating an helper struct for split Edge (outer) */
    struct aux_split_outer_edge *outer =
	malloc (sizeof (struct aux_split_outer_edge));
    outer->edge = edge;
    outer->next = NULL;
    return outer;
}

static void
destroy_aux_split_outer_edge (struct aux_split_outer_edge *outer)
{
/* destroying an helper struct for split Edge (outer) */
    if (outer == NULL)
	return;
    if (outer->edge != NULL)
	gaiaFreeLinestring (outer->edge);
    free (outer);
}

static struct aux_split_inner_edge_candidate *
create_aux_split_inner_edge_candidate (struct aux_split_node *end,
				       double length)
{
/* creating an helper struct for split Edge (inner candidate) */
    struct aux_split_inner_edge_candidate *inner =
	malloc (sizeof (struct aux_split_inner_edge_candidate));
    inner->end = end;
    inner->length = length;
    inner->done = 0;
    inner->next = NULL;
    return inner;
}

static void
destroy_aux_split_inner_edge_candidate (struct aux_split_inner_edge_candidate
					*inner)
{
/* destroying an helper struct for split Edge (inner candidate) */
    if (inner == NULL)
	return;
    free (inner);
}

static struct aux_split_node *
create_aux_split_node2D (double x, double y)
{
/* creating an helper struct for split Node 2D */
    struct aux_split_node *node = malloc (sizeof (struct aux_split_node));
    node->visited = 0;
    node->has_z = 0;
    node->x = x;
    node->y = y;
    node->first = NULL;
    node->last = NULL;
    node->next = NULL;
    return node;
}

static struct aux_split_node *
create_aux_split_node3D (double x, double y, double z)
{
/* creating an helper struct for split Node 3D */
    struct aux_split_node *node = malloc (sizeof (struct aux_split_node));
    node->visited = 0;
    node->has_z = 1;
    node->x = x;
    node->y = y;
    node->z = z;
    node->first = NULL;
    node->last = NULL;
    node->next = NULL;
    return node;
}

static void
destroy_aux_split_node (struct aux_split_node *node)
{
/* destroying an helper struct for split Node */
    struct aux_split_inner_edge_candidate *pc;
    struct aux_split_inner_edge_candidate *pc_n;
    if (node == NULL)
	return;

    pc = node->first;
    while (pc != NULL)
      {
	  pc_n = pc->next;
	  destroy_aux_split_inner_edge_candidate (pc);
	  pc = pc_n;
      }
    free (node);
}

static int
aux_split_node_equals (struct aux_split_node *n1, struct aux_split_node *n2)
{
/* testing if two helper Nodes are the same */
    if (n1->has_z)
      {
	  if (n1->x == n2->x && n1->y == n2->y && n1->z == n2->z)
	      return 1;
      }
    else
      {
	  if (n1->x == n2->x && n1->y == n2->y)
	      return 1;
      }
    return 0;
}

static void
aux_split_polygon_add_inner_edge_candidate (struct aux_split_node *start,
					    struct aux_split_node *end,
					    double length)
{
/* adding an inner Edge to an helpèr struct for split Polygon */
    struct aux_split_inner_edge_candidate *edge;
    if (end == NULL || start == NULL)
	return;

/* adding the Edge to the StartNode's linked list */
    edge = create_aux_split_inner_edge_candidate (end, length);
    if (start->first == NULL)
	start->first = edge;
    if (start->last != NULL)
	start->last->next = edge;
    start->last = edge;
}

static struct aux_split_inner_edge *
create_aux_split_inner_edge (struct aux_split_node *start,
			     struct aux_split_node *end, gaiaLinestringPtr edge,
			     double length)
{
/* creating an helper struct for split Edge (inner) */
    struct aux_split_inner_edge *inner =
	malloc (sizeof (struct aux_split_inner_edge));
    inner->start = start;
    inner->end = end;
    inner->edge = edge;
    inner->length = length;
    inner->confirmed = 0;
    inner->next = NULL;
    return inner;
}

static void
destroy_aux_split_inner_edge (struct aux_split_inner_edge *inner)
{
/* destroying an helper struct for split Edge (inner) */
    if (inner == NULL)
	return;
    if (inner->edge != NULL)
	gaiaFreeLinestring (inner->edge);
    free (inner);
}

static struct aux_split_polygon *
create_aux_split_polygon (int has_z)
{
/* creating an helper struct for split Polygon */
    struct aux_split_polygon *aux = malloc (sizeof (struct aux_split_polygon));
    aux->has_z = has_z;
    aux->first_outer = NULL;
    aux->last_outer = NULL;
    aux->first_node = NULL;
    aux->last_node = NULL;
    aux->first_inner = NULL;
    aux->last_inner = NULL;
    return aux;
}

static void
destroy_aux_split_polygon (struct aux_split_polygon *aux)
{
/* destroying an helper struct for split Polygon */
    struct aux_split_outer_edge *po;
    struct aux_split_outer_edge *po_n;
    struct aux_split_node *pn;
    struct aux_split_node *pn_n;
    struct aux_split_inner_edge *pi;
    struct aux_split_inner_edge *pi_n;

    if (aux == NULL)
	return;
    po = aux->first_outer;
    while (po != NULL)
      {
	  po_n = po->next;
	  destroy_aux_split_outer_edge (po);
	  po = po_n;
      }
    pn = aux->first_node;
    while (pn != NULL)
      {
	  pn_n = pn->next;
	  destroy_aux_split_node (pn);
	  pn = pn_n;
      }
    pi = aux->first_inner;
    while (pi != NULL)
      {
	  pi_n = pi->next;
	  destroy_aux_split_inner_edge (pi);
	  pi = pi_n;
      }
    free (aux);
}

static void
aux_split_polygon_add_node2D (struct aux_split_polygon *aux, double x, double y)
{
/* adding a Node 2D to an helpèr struct for split Polygon */
    struct aux_split_node *pn;
    struct aux_split_node *node;
    if (aux == NULL)
	return;

    node = create_aux_split_node2D (x, y);
    pn = aux->first_node;
    while (pn)
      {
	  if (aux_split_node_equals (pn, node) == 1)
	    {
		/* already defined - avoiding duplitcates */
		destroy_aux_split_node (node);
		return;
	    }
	  pn = pn->next;
      }

/* adding the Node to the linked list */
    if (aux->first_node == NULL)
	aux->first_node = node;
    if (aux->last_node != NULL)
	aux->last_node->next = node;
    aux->last_node = node;
}

static void
aux_split_polygon_add_node3D (struct aux_split_polygon *aux, double x, double y,
			      double z)
{
/* adding a Node 3D to an helpèr struct for split Polygon */
    struct aux_split_node *pn;
    struct aux_split_node *node;
    if (aux == NULL)
	return;

    node = create_aux_split_node3D (x, y, z);
    pn = aux->first_node;
    while (pn)
      {
	  if (aux_split_node_equals (pn, node) == 1)
	    {
		/* already defined - avoiding duplitcates */
		destroy_aux_split_node (node);
		return;
	    }
	  pn = pn->next;
      }

/* adding the Node to the linked list */
    if (aux->first_node == NULL)
	aux->first_node = node;
    if (aux->last_node != NULL)
	aux->last_node->next = node;
    aux->last_node = node;
}

static void
aux_split_polygon_add_outer_edge (struct aux_split_polygon *aux,
				  gaiaLinestringPtr ln)
{
/* adding an outer Edge to an helpèr struct for split Polygon */
    struct aux_split_outer_edge *edge;
    double x;
    double y;
    double z;
    double m;
    int last;
    if (aux == NULL || ln == NULL)
	return;

/* extracting the Start Node */
    if (ln->DimensionModel == GAIA_XY_Z)
      {
	  gaiaGetPointXYZ (ln->Coords, 0, &x, &y, &z);
      }
    else if (ln->DimensionModel == GAIA_XY_M)
      {
	  gaiaGetPointXYM (ln->Coords, 0, &x, &y, &m);
      }
    else if (ln->DimensionModel == GAIA_XY_Z_M)
      {
	  gaiaGetPointXYZM (ln->Coords, 0, &x, &y, &z, &m);
      }
    else
      {
	  gaiaGetPoint (ln->Coords, 0, &x, &y);
      }
    if (aux->has_z)
	aux_split_polygon_add_node3D (aux, x, y, z);
    else
	aux_split_polygon_add_node2D (aux, x, y);

/* extracting the End Node */
    last = ln->Points - 1;
    if (ln->DimensionModel == GAIA_XY_Z)
      {
	  gaiaGetPointXYZ (ln->Coords, last, &x, &y, &z);
      }
    else if (ln->DimensionModel == GAIA_XY_M)
      {
	  gaiaGetPointXYM (ln->Coords, last, &x, &y, &m);
      }
    else if (ln->DimensionModel == GAIA_XY_Z_M)
      {
	  gaiaGetPointXYZM (ln->Coords, last, &x, &y, &z, &m);
      }
    else
      {
	  gaiaGetPoint (ln->Coords, last, &x, &y);
      }
    if (aux->has_z)
	aux_split_polygon_add_node3D (aux, x, y, z);
    else
	aux_split_polygon_add_node2D (aux, x, y);

/* adding the Edge to the linked list */
    edge = create_aux_split_outer_edge (ln);
    if (aux->first_outer == NULL)
	aux->first_outer = edge;
    if (aux->last_outer != NULL)
	aux->last_outer->next = edge;
    aux->last_outer = edge;
}

static void
aux_split_polygon_add_inner_edge (struct aux_split_polygon *aux,
				  struct aux_split_node *start,
				  struct aux_split_node *end,
				  gaiaLinestringPtr ln, double length)
{
/* adding an inner Edge to an helpèr struct for split Polygon */
    struct aux_split_inner_edge *edge;
    if (aux == NULL || ln == NULL)
	return;

/* adding the Edge to the linked list */
    edge = create_aux_split_inner_edge (start, end, ln, length);
    if (aux->first_inner == NULL)
	aux->first_inner = edge;
    if (aux->last_inner != NULL)
	aux->last_inner->next = edge;
    aux->last_inner = edge;
}

static void
aux_split_polygon_split_ring (struct aux_split_polygon *aux, gaiaRingPtr rng,
			      int ring_max_points)
{
/* splitting a Ring into many shortest Edges) */
    gaiaLinestringPtr ln;
    int num_lines;
    int mean_points;
    int points;
    int iv;
    int pout = 0;

    num_lines = rng->Points / ring_max_points;
    if ((num_lines * ring_max_points) < rng->Points)
	num_lines++;
    points = rng->Points / num_lines;
    if ((points * num_lines) < rng->Points)
	points++;
    mean_points = points;
    if (rng->DimensionModel == GAIA_XY_Z || rng->DimensionModel == GAIA_XY_Z_M)
	ln = gaiaAllocLinestringXYZ (points);
    else
	ln = gaiaAllocLinestring (points);

    for (iv = 0; iv <= rng->Points; iv++)
      {
	  double x;
	  double y;
	  double z = 0.0;
	  double m = 0.0;
	  if (rng->DimensionModel == GAIA_XY_Z)
	    {
		gaiaGetPointXYZ (rng->Coords, iv, &x, &y, &z);
	    }
	  else if (rng->DimensionModel == GAIA_XY_M)
	    {
		gaiaGetPointXYM (rng->Coords, iv, &x, &y, &m);
	    }
	  else if (rng->DimensionModel == GAIA_XY_Z_M)
	    {
		gaiaGetPointXYZM (rng->Coords, iv, &x, &y, &z, &m);
	    }
	  else
	    {
		gaiaGetPoint (rng->Coords, iv, &x, &y);
	    }
	  if (rng->DimensionModel == GAIA_XY_Z
	      || rng->DimensionModel == GAIA_XY_Z_M)
	    {
		gaiaSetPointXYZ (ln->Coords, pout, x, y, z);
	    }
	  else
	    {
		gaiaSetPoint (ln->Coords, pout, x, y);
	    }
	  pout++;
	  if (pout < points)
	      continue;

	  /* adding the Edge to the outer list */
	  aux_split_polygon_add_outer_edge (aux, ln);

	  points = rng->Points - iv;
	  if (points <= 1)
	      break;
	  if (points <= ring_max_points)
	    {
		if (rng->DimensionModel == GAIA_XY_Z
		    || rng->DimensionModel == GAIA_XY_Z_M)
		    ln = gaiaAllocLinestringXYZ (points);
		else
		    ln = gaiaAllocLinestring (points);
	    }
	  else
	    {
		if ((mean_points * ring_max_points) < rng->Points)
		    points = mean_points + 1;
		else
		    points = mean_points;
		if (rng->DimensionModel == GAIA_XY_Z
		    || rng->DimensionModel == GAIA_XY_Z_M)
		    ln = gaiaAllocLinestringXYZ (points);
		else
		    ln = gaiaAllocLinestring (points);
	    }
	  pout = 0;
	  if (rng->DimensionModel == GAIA_XY_Z
	      || rng->DimensionModel == GAIA_XY_Z_M)
	    {
		gaiaSetPointXYZ (ln->Coords, pout, x, y, z);
	    }
	  else
	    {
		gaiaSetPoint (ln->Coords, pout, x, y);
	    }
	  pout++;
      }
}

static void
aux_split_polygon_inner_edges (struct gaia_topology *topo,
			       struct aux_split_polygon *aux, gaiaRingPtr rng,
			       struct splite_internal_cache *cache)
{
/* preparing all Inner Edges (so to split the Ring area into many smaller Faces) */
    gaiaGeomCollPtr geom;
    gaiaLinestringPtr ln;
    gaiaPolygonPtr pg;
    GEOSContextHandle_t handle = cache->GEOS_handle;
    GEOSGeometry *candidate;
    GEOSGeometry *polygon;
    const GEOSPreparedGeometry *gPrep;
    int i;
    int count;
    struct aux_split_node *node1;
    struct aux_split_node *node2;

    node1 = aux->first_node;
    while (node1 != NULL)
      {
	  node2 = node1->next;
	  while (node2 != NULL)
	    {
		/* looping on Node pairs so to identify all Inner Edge Candidates */
		double length =
		    sqrt (((node1->x - node2->x) * (node1->x - node2->x)) +
			  ((node1->y - node2->y) * (node1->y - node2->y)));
		aux_split_polygon_add_inner_edge_candidate (node1, node2,
							    length);
		node2 = node2->next;
	    }
	  node1 = node1->next;
      }

/* preparing a GEOS Polygon Geometry */
    if (aux->has_z)
	geom = gaiaAllocGeomCollXYZ ();
    else
	geom = gaiaAllocGeomColl ();
    geom->Srid = topo->srid;
    pg = malloc (sizeof (gaiaPolygon));
    pg->Exterior = rng;
    pg->NumInteriors = 0;
    pg->Interiors = NULL;
    pg->Next = NULL;
    geom->FirstPolygon = pg;
    geom->LastPolygon = pg;
    gaiaMbrGeometry (geom);
    polygon = gaiaToGeos_r (cache, geom);
    pg->Exterior = NULL;	/* releasing ownership on Ring */
    gaiaFreeGeomColl (geom);
    gPrep = GEOSPrepare_r (handle, polygon);

    node1 = aux->first_node;
    while (node1 != NULL)
      {
	  if (node1->visited >= 2)
	    {
		node1 = node1->next;
		continue;
	    }
	  node1->visited += 1;
	  while (1)
	    {
		/* searching the longest inner edge candidate completely within the polygon */
		double maxlen = 0.0;
		struct aux_split_inner_edge_candidate *max = NULL;
		struct aux_split_inner_edge_candidate *inner = node1->first;
		while (inner != NULL)
		  {
		      if (inner->done == 0 && inner->end->visited < 2
			  && inner->length > maxlen)
			{
			    maxlen = inner->length;
			    max = inner;
			}
		      inner = inner->next;
		  }
		if (max == NULL)
		    break;
		max->done = 1;

		/* building the Inner Edge to be checked */
		if (aux->has_z)
		    geom = gaiaAllocGeomCollXYZ ();
		else
		    geom = gaiaAllocGeomColl ();
		geom->Srid = topo->srid;
		ln = gaiaAddLinestringToGeomColl (geom, 2);
		if (aux->has_z)
		  {
		      gaiaSetPointXYZ (ln->Coords, 0, node1->x, node1->y,
				       node1->z);
		      gaiaSetPointXYZ (ln->Coords, 1, max->end->x, max->end->y,
				       max->end->z);
		  }
		else
		  {
		      gaiaSetPoint (ln->Coords, 0, node1->x, node1->y);
		      gaiaSetPoint (ln->Coords, 1, max->end->x, max->end->y);
		  }
		gaiaMbrGeometry (geom);
		candidate = gaiaToGeos_r (cache, geom);
		geom->FirstLinestring = NULL;	/* releasing ownership on Linestring */
		geom->LastLinestring = NULL;
		gaiaFreeGeomColl (geom);

		if (GEOSPreparedCovers_r (handle, gPrep, candidate) == 1)
		  {
		      /* candidate confirmed (completely covered by the polygon): adding to the list */
		      aux_split_polygon_add_inner_edge (aux, node1, max->end,
							ln, max->length);
		      max->end->visited += 1;
		      GEOSGeom_destroy_r (handle, candidate);
		      break;
		  }
		else
		    gaiaFreeLinestring (ln);
		GEOSGeom_destroy_r (handle, candidate);
	    }
	  node1 = node1->next;
      }

/* releasing the GEOS Polygon Geometry */
    GEOSPreparedGeom_destroy (gPrep);
    GEOSGeom_destroy_r (handle, polygon);

/* avoiding to insert too many inner edges */
    count = 0;
    i = 1;
    while (i)
      {
	  double maxlen = 0.0;
	  struct aux_split_inner_edge *max = NULL;
	  struct aux_split_inner_edge *inner = aux->first_inner;
	  i = 0;
	  while (inner != NULL)
	    {
		if (inner->confirmed)
		  {
		      /* skipping already visited Inner Edges */
		      inner = inner->next;
		      continue;
		  }
		if (inner->length > maxlen)
		  {
		      maxlen = inner->length;
		      max = inner;
		  }
		inner = inner->next;
	    }
	  if (max != NULL)
	    {
		max->confirmed = 1;
		i = 1;
		count++;
	    }
	  if (count > 8)
	      break;
      }
}

SPATIALITE_PRIVATE void
free_internal_cache_topologies (void *firstTopology)
{
/* destroying all Topologies registered into the Internal Connection Cache */
    struct gaia_topology *p_topo = (struct gaia_topology *) firstTopology;
    struct gaia_topology *p_topo_n;

    while (p_topo != NULL)
      {
	  p_topo_n = p_topo->next;
	  gaiaTopologyDestroy ((GaiaTopologyAccessorPtr) p_topo);
	  p_topo = p_topo_n;
      }
}

static int
do_create_topologies (sqlite3 * handle)
{
/* attempting to create the Topologies table (if not already existing) */
    const char *sql;
    char *err_msg = NULL;
    int ret;

    sql = "CREATE TABLE IF NOT EXISTS topologies (\n"
	"\ttopology_name TEXT NOT NULL PRIMARY KEY,\n"
	"\tsrid INTEGER NOT NULL,\n"
	"\ttolerance DOUBLE NOT NULL,\n"
	"\thas_z INTEGER NOT NULL,\n"
	"\tnext_edge_id INTEGER NOT NULL DEFAULT 1,\n"
	"\tCONSTRAINT topo_srid_fk FOREIGN KEY (srid) "
	"REFERENCES spatial_ref_sys (srid))";
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE TABLE topologies - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating Topologies triggers */
    sql = "CREATE TRIGGER IF NOT EXISTS topology_name_insert\n"
	"BEFORE INSERT ON 'topologies'\nFOR EACH ROW BEGIN\n"
	"SELECT RAISE(ABORT,'insert on topologies violates constraint: "
	"topology_name value must not contain a single quote')\n"
	"WHERE NEW.topology_name LIKE ('%''%');\n"
	"SELECT RAISE(ABORT,'insert on topologies violates constraint: "
	"topology_name value must not contain a double quote')\n"
	"WHERE NEW.topology_name LIKE ('%\"%');\n"
	"SELECT RAISE(ABORT,'insert on topologies violates constraint: "
	"topology_name value must be lower case')\n"
	"WHERE NEW.topology_name <> lower(NEW.topology_name);\nEND";
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }
    sql = "CREATE TRIGGER IF NOT EXISTS topology_name_update\n"
	"BEFORE UPDATE OF 'topology_name' ON 'topologies'\nFOR EACH ROW BEGIN\n"
	"SELECT RAISE(ABORT,'update on topologies violates constraint: "
	"topology_name value must not contain a single quote')\n"
	"WHERE NEW.topology_name LIKE ('%''%');\n"
	"SELECT RAISE(ABORT,'update on topologies violates constraint: "
	"topology_name value must not contain a double quote')\n"
	"WHERE NEW.topology_name LIKE ('%\"%');\n"
	"SELECT RAISE(ABORT,'update on topologies violates constraint: "
	"topology_name value must be lower case')\n"
	"WHERE NEW.topology_name <> lower(NEW.topology_name);\nEND";
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SQL error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

    return 1;
}

static int
check_new_topology (sqlite3 * handle, const char *topo_name)
{
/* testing if some already defined DB object forbids creating the new Topology */
    char *sql;
    char *prev;
    char *table;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    const char *value;
    int error = 0;

/* testing if the same Topology is already defined */
    sql = sqlite3_mprintf ("SELECT Count(*) FROM MAIN.topologies WHERE "
			   "Lower(topology_name) = Lower(%Q)", topo_name);
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = results[(i * columns) + 0];
		if (atoi (value) != 0)
		    error = 1;
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;

/* testing if some table/geom is already defined in geometry_columns */
    sql = sqlite3_mprintf ("SELECT Count(*) FROM geometry_columns WHERE");
    prev = sql;
    table = sqlite3_mprintf ("%s_node", topo_name);
    sql =
	sqlite3_mprintf
	("%s (Lower(f_table_name) = Lower(%Q) AND f_geometry_column = 'geom')",
	 prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("%s_edge", topo_name);
    sql =
	sqlite3_mprintf
	("%s OR (Lower(f_table_name) = Lower(%Q) AND f_geometry_column = 'geom')",
	 prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = results[(i * columns) + 0];
		if (atoi (value) != 0)
		    error = 1;
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;

/* testing if some table is already defined */
    sql = sqlite3_mprintf ("SELECT Count(*) FROM sqlite_master WHERE");
    prev = sql;
    table = sqlite3_mprintf ("%s_node", topo_name);
    sql = sqlite3_mprintf ("%s Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("%s_edge", topo_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("%s_face", topo_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("idx_%s_node_geom", topo_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("idx_%s_edge_geom", topo_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("idx_%s_face_rtree", topo_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = results[(i * columns) + 0];
		if (atoi (value) != 0)
		    error = 1;
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;

    return 1;
}

static int
do_create_face (sqlite3 * handle, const char *topo_name)
{
/* attempting to create the Topology Face table */
    char *sql;
    char *table;
    char *xtable;
    char *trigger;
    char *xtrigger;
    char *rtree;
    char *xrtree;
    char *err_msg = NULL;
    int ret;

/* creating the main table */
    table = sqlite3_mprintf ("%s_face", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n"
			   "\tface_id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
			   "\tmin_x DOUBLE,\n\tmin_y DOUBLE,\n"
			   "\tmax_x DOUBLE,\n\tmax_y DOUBLE)", xtable);
    free (xtable);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE TABLE topology-FACE - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating the exotic spatial index */
    table = sqlite3_mprintf ("idx_%s_face_rtree", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("CREATE VIRTUAL TABLE \"%s\" USING RTree "
			   "(id_face, x_min, x_max, y_min, y_max)", xtable);
    free (xtable);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE topology-FACE Spatiale index - error: %s\n",
			err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* adding the "face_insert" trigger */
    trigger = sqlite3_mprintf ("%s_face_insert", topo_name);
    xtrigger = gaiaDoubleQuotedSql (trigger);
    sqlite3_free (trigger);
    table = sqlite3_mprintf ("%s_face", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    rtree = sqlite3_mprintf ("idx_%s_face_rtree", topo_name);
    xrtree = gaiaDoubleQuotedSql (rtree);
    sqlite3_free (rtree);
    sql = sqlite3_mprintf ("CREATE TRIGGER \"%s\" AFTER INSERT ON \"%s\"\n"
			   "FOR EACH ROW BEGIN\n"
			   "\tINSERT OR REPLACE INTO \"%s\" (id_face, x_min, x_max, y_min, y_max) "
			   "VALUES (NEW.face_id, NEW.min_x, NEW.max_x, NEW.min_y, NEW.max_y);\n"
			   "DELETE FROM \"%s\" WHERE id_face = NEW.face_id AND NEW.face_id = 0;\n"
			   "END", xtrigger, xtable, xrtree, xrtree);
    free (xtrigger);
    free (xtable);
    free (xrtree);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("CREATE TRIGGER topology-FACE next INSERT - error: %s\n",
	       err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* adding the "face_update_mbr" trigger */
    trigger = sqlite3_mprintf ("%s_face_update_mbr", topo_name);
    xtrigger = gaiaDoubleQuotedSql (trigger);
    sqlite3_free (trigger);
    table = sqlite3_mprintf ("%s_face", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    rtree = sqlite3_mprintf ("idx_%s_face_rtree", topo_name);
    xrtree = gaiaDoubleQuotedSql (rtree);
    sqlite3_free (rtree);
    sql =
	sqlite3_mprintf
	("CREATE TRIGGER \"%s\" AFTER UPDATE OF min_x, min_y, max_x, max_y ON \"%s\"\n"
	 "FOR EACH ROW BEGIN\n"
	 "\tINSERT OR REPLACE INTO \"%s\" (id_face, x_min, x_max, y_min, y_max) "
	 "VALUES (NEW.face_id, NEW.min_x, NEW.max_x, NEW.min_y, NEW.max_y);\n"
	 "DELETE FROM \"%s\" WHERE id_face = NEW.face_id AND NEW.face_id = 0;\n"
	 "END", xtrigger, xtable, xrtree, xrtree);
    free (xtrigger);
    free (xtable);
    free (xrtree);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("CREATE TRIGGER topology-FACE next UPDATE - error: %s\n",
	       err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* adding the "face_delete" trigger */
    trigger = sqlite3_mprintf ("%s_face_delete", topo_name);
    xtrigger = gaiaDoubleQuotedSql (trigger);
    sqlite3_free (trigger);
    table = sqlite3_mprintf ("%s_face", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    rtree = sqlite3_mprintf ("idx_%s_face_rtree", topo_name);
    xrtree = gaiaDoubleQuotedSql (rtree);
    sqlite3_free (rtree);
    sql = sqlite3_mprintf ("CREATE TRIGGER \"%s\" AFTER DELETE ON \"%s\"\n"
			   "FOR EACH ROW BEGIN\n"
			   "\tDELETE FROM \"%s\" WHERE id_face = OLD.face_id;\n"
			   "END", xtrigger, xtable, xrtree);
    free (xtrigger);
    free (xtable);
    free (xrtree);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("CREATE TRIGGER topology-FACE next UPDATE - error: %s\n",
	       err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* inserting the default World Face */
    table = sqlite3_mprintf ("%s_face", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("INSERT INTO MAIN.\"%s\" VALUES (0, NULL, NULL, NULL, NULL)", xtable);
    free (xtable);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("INSERT WorldFACE - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

    return 1;
}

static int
do_create_node (sqlite3 * handle, const char *topo_name, int srid, int has_z)
{
/* attempting to create the Topology Node table */
    char *sql;
    char *table;
    char *xtable;
    char *xconstraint;
    char *xmother;
    char *err_msg = NULL;
    int ret;

/* creating the main table */
    table = sqlite3_mprintf ("%s_node", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_node_face_fk", topo_name);
    xconstraint = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_face", topo_name);
    xmother = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n"
			   "\tnode_id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
			   "\tcontaining_face INTEGER,\n"
			   "\tCONSTRAINT \"%s\" FOREIGN KEY (containing_face) "
			   "REFERENCES \"%s\" (face_id))", xtable, xconstraint,
			   xmother);
    free (xtable);
    free (xconstraint);
    free (xmother);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE TABLE topology-NODE - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating the Node Geometry */
    table = sqlite3_mprintf ("%s_node", topo_name);
    sql =
	sqlite3_mprintf
	("SELECT AddGeometryColumn(%Q, 'geom', %d, 'POINT', %Q, 1)", table,
	 srid, has_z ? "XYZ" : "XY");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("AddGeometryColumn topology-NODE - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating a Spatial Index supporting Node Geometry */
    table = sqlite3_mprintf ("%s_node", topo_name);
    sql = sqlite3_mprintf ("SELECT CreateSpatialIndex(%Q, 'geom')", table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("CreateSpatialIndex topology-NODE - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating an Index supporting "containing_face" */
    table = sqlite3_mprintf ("%s_node", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("idx_%s_node_contface", topo_name);
    xconstraint = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf ("CREATE INDEX \"%s\" ON \"%s\" (containing_face)",
			 xconstraint, xtable);
    free (xtable);
    free (xconstraint);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE INDEX node-contface - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

    return 1;
}

static int
do_create_edge (sqlite3 * handle, const char *topo_name, int srid, int has_z)
{
/* attempting to create the Topology Edge table */
    char *sql;
    char *table;
    char *xtable;
    char *xconstraint1;
    char *xconstraint2;
    char *xconstraint3;
    char *xconstraint4;
    char *xnodes;
    char *xfaces;
    char *trigger;
    char *xtrigger;
    char *err_msg = NULL;
    int ret;

/* creating the main table */
    table = sqlite3_mprintf ("%s_edge", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_edge_node_start_fk", topo_name);
    xconstraint1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_edge_node_end_fk", topo_name);
    xconstraint2 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_edge_face_left_fk", topo_name);
    xconstraint3 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_edge_face_right_fk", topo_name);
    xconstraint4 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_node", topo_name);
    xnodes = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_face", topo_name);
    xfaces = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("CREATE TABLE \"%s\" (\n"
			   "\tedge_id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
			   "\tstart_node INTEGER NOT NULL,\n"
			   "\tend_node INTEGER NOT NULL,\n"
			   "\tnext_left_edge INTEGER NOT NULL,\n"
			   "\tnext_right_edge INTEGER NOT NULL,\n"
			   "\tleft_face INTEGER NOT NULL,\n"
			   "\tright_face INTEGER NOT NULL,\n"
			   "\tCONSTRAINT \"%s\" FOREIGN KEY (start_node) "
			   "REFERENCES \"%s\" (node_id),\n"
			   "\tCONSTRAINT \"%s\" FOREIGN KEY (end_node) "
			   "REFERENCES \"%s\" (node_id),\n"
			   "\tCONSTRAINT \"%s\" FOREIGN KEY (left_face) "
			   "REFERENCES \"%s\" (face_id),\n"
			   "\tCONSTRAINT \"%s\" FOREIGN KEY (right_face) "
			   "REFERENCES \"%s\" (face_id))",
			   xtable, xconstraint1, xnodes, xconstraint2, xnodes,
			   xconstraint3, xfaces, xconstraint4, xfaces);
    free (xtable);
    free (xconstraint1);
    free (xconstraint2);
    free (xconstraint3);
    free (xconstraint4);
    free (xnodes);
    free (xfaces);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE TABLE topology-EDGE - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* adding the "next_edge_ins" trigger */
    trigger = sqlite3_mprintf ("%s_edge_next_ins", topo_name);
    xtrigger = gaiaDoubleQuotedSql (trigger);
    sqlite3_free (trigger);
    table = sqlite3_mprintf ("%s_edge", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("CREATE TRIGGER \"%s\" AFTER INSERT ON \"%s\"\n"
			   "FOR EACH ROW BEGIN\n"
			   "\tUPDATE topologies SET next_edge_id = NEW.edge_id + 1 "
			   "WHERE Lower(topology_name) = Lower(%Q) AND next_edge_id < NEW.edge_id + 1;\n"
			   "END", xtrigger, xtable, topo_name);
    free (xtrigger);
    free (xtable);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("CREATE TRIGGER topology-EDGE next INSERT - error: %s\n",
	       err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* adding the "next_edge_upd" trigger */
    trigger = sqlite3_mprintf ("%s_edge_next_upd", topo_name);
    xtrigger = gaiaDoubleQuotedSql (trigger);
    sqlite3_free (trigger);
    table = sqlite3_mprintf ("%s_edge", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("CREATE TRIGGER \"%s\" AFTER UPDATE OF edge_id ON \"%s\"\n"
	 "FOR EACH ROW BEGIN\n"
	 "\tUPDATE topologies SET next_edge_id = NEW.edge_id + 1 "
	 "WHERE Lower(topology_name) = Lower(%Q) AND next_edge_id < NEW.edge_id + 1;\n"
	 "END", xtrigger, xtable, topo_name);
    free (xtrigger);
    free (xtable);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("CREATE TRIGGER topology-EDGE next UPDATE - error: %s\n",
	       err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating the Edge Geometry */
    table = sqlite3_mprintf ("%s_edge", topo_name);
    sql =
	sqlite3_mprintf
	("SELECT AddGeometryColumn(%Q, 'geom', %d, 'LINESTRING', %Q, 1)",
	 table, srid, has_z ? "XYZ" : "XY");
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("AddGeometryColumn topology-EDGE - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating a Spatial Index supporting Edge Geometry */
    table = sqlite3_mprintf ("%s_edge", topo_name);
    sql = sqlite3_mprintf ("SELECT CreateSpatialIndex(%Q, 'geom')", table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("CreateSpatialIndex topology-EDGE - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating an Index supporting "start_node" */
    table = sqlite3_mprintf ("%s_edge", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("idx_%s_start_node", topo_name);
    xconstraint1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf ("CREATE INDEX \"%s\" ON \"%s\" (start_node)",
			 xconstraint1, xtable);
    free (xtable);
    free (xconstraint1);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE INDEX edge-startnode - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating an Index supporting "end_node" */
    table = sqlite3_mprintf ("%s_edge", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("idx_%s_end_node", topo_name);
    xconstraint1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf ("CREATE INDEX \"%s\" ON \"%s\" (end_node)",
			 xconstraint1, xtable);
    free (xtable);
    free (xconstraint1);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE INDEX edge-endnode - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating an Index supporting "left_face" */
    table = sqlite3_mprintf ("%s_edge", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("idx_%s_edge_leftface", topo_name);
    xconstraint1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf ("CREATE INDEX \"%s\" ON \"%s\" (left_face)",
			 xconstraint1, xtable);
    free (xtable);
    free (xconstraint1);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE INDEX edge-leftface - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* creating an Index supporting "right_face" */
    table = sqlite3_mprintf ("%s_edge", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("idx_%s_edge_rightface", topo_name);
    xconstraint1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf ("CREATE INDEX \"%s\" ON \"%s\" (right_face)",
			 xconstraint1, xtable);
    free (xtable);
    free (xconstraint1);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("CREATE INDEX edge-rightface - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

    return 1;
}

GAIATOPO_DECLARE int
gaiaTopologyCreate (sqlite3 * handle, const char *topo_name, int srid,
		    double tolerance, int has_z)
{
/* attempting to create a new Topology */
    int ret;
    char *sql;

/* creating the Topologies table (just in case) */
    if (!do_create_topologies (handle))
	return 0;

/* testing for forbidding objects */
    if (!check_new_topology (handle, topo_name))
	return 0;

/* creating the Topology own Tables */
    if (!do_create_face (handle, topo_name))
	goto error;
    if (!do_create_node (handle, topo_name, srid, has_z))
	goto error;
    if (!do_create_edge (handle, topo_name, srid, has_z))
	goto error;

/* registering the Topology */
    sql = sqlite3_mprintf ("INSERT INTO MAIN.topologies (topology_name, "
			   "srid, tolerance, has_z) VALUES (Lower(%Q), %d, %f, %d)",
			   topo_name, srid, tolerance, has_z);
    ret = sqlite3_exec (handle, sql, NULL, NULL, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto error;

    return 1;

  error:
    return 0;
}

static int
check_existing_topology (sqlite3 * handle, const char *topo_name,
			 int full_check)
{
/* testing if a Topology is already defined */
    char *sql;
    char *prev;
    char *table;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    const char *value;
    int error = 0;

/* testing if the Topology is already defined */
    sql = sqlite3_mprintf ("SELECT Count(*) FROM MAIN.topologies WHERE "
			   "Lower(topology_name) = Lower(%Q)", topo_name);
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = results[(i * columns) + 0];
		if (atoi (value) != 1)
		    error = 1;
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;
    if (!full_check)
	return 1;

/* testing if all table/geom are correctly defined in geometry_columns */
    sql = sqlite3_mprintf ("SELECT Count(*) FROM geometry_columns WHERE");
    prev = sql;
    table = sqlite3_mprintf ("%s_node", topo_name);
    sql =
	sqlite3_mprintf
	("%s (Lower(f_table_name) = Lower(%Q) AND f_geometry_column = 'geom')",
	 prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("%s_edge", topo_name);
    sql =
	sqlite3_mprintf
	("%s OR (Lower(f_table_name) = Lower(%Q) AND f_geometry_column = 'geom')",
	 prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = results[(i * columns) + 0];
		if (atoi (value) != 2)
		    error = 1;
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;


/* testing if all tables are already defined */
    sql =
	sqlite3_mprintf
	("SELECT Count(*) FROM sqlite_master WHERE type = 'table' AND (");
    prev = sql;
    table = sqlite3_mprintf ("%s_node", topo_name);
    sql = sqlite3_mprintf ("%s Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("%s_edge", topo_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("%s_face", topo_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("idx_%s_node_geom", topo_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("idx_%s_edge_geom", topo_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q)", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    prev = sql;
    table = sqlite3_mprintf ("idx_%s_face_rtree", topo_name);
    sql = sqlite3_mprintf ("%s OR Lower(name) = Lower(%Q))", prev, table);
    sqlite3_free (table);
    sqlite3_free (prev);
    ret = sqlite3_get_table (handle, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		value = results[(i * columns) + 0];
		if (atoi (value) != 6)
		    error = 1;
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;

    return 1;
}

static int
do_drop_topo_face (sqlite3 * handle, const char *topo_name)
{
/* attempting to drop the Topology-Face table */
    char *sql;
    char *table;
    char *xtable;
    char *err_msg = NULL;
    int ret;

/* dropping the main table */
    table = sqlite3_mprintf ("%s_face", topo_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("DROP TABLE IF EXISTS \"%s\"", xtable);
    free (xtable);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("DROP topology-face - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* dropping the corresponding Spatial Index */
    table = sqlite3_mprintf ("idx_%s_face_rtree", topo_name);
    sql = sqlite3_mprintf ("DROP TABLE IF EXISTS \"%s\"", table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("DROP SpatialIndex topology-face - error: %s\n", err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

    return 1;
}

static int
do_drop_topo_table (sqlite3 * handle, const char *topo_name, const char *which)
{
/* attempting to drop some Topology table */
    char *sql;
    char *table;
    char *xtable;
    char *err_msg = NULL;
    int ret;

    if (strcmp (which, "face") == 0)
	return do_drop_topo_face (handle, topo_name);

/* disabling the corresponding Spatial Index */
    table = sqlite3_mprintf ("%s_%s", topo_name, which);
    sql = sqlite3_mprintf ("SELECT DisableSpatialIndex(%Q, 'geom')", table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("DisableSpatialIndex topology-%s - error: %s\n", which, err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* discarding the Geometry column */
    table = sqlite3_mprintf ("%s_%s", topo_name, which);
    sql = sqlite3_mprintf ("SELECT DiscardGeometryColumn(%Q, 'geom')", table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("DisableGeometryColumn topology-%s - error: %s\n", which,
	       err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* dropping the main table */
    table = sqlite3_mprintf ("%s_%s", topo_name, which);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("DROP TABLE IF EXISTS \"%s\"", xtable);
    free (xtable);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("DROP topology-%s - error: %s\n", which, err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

/* dropping the corresponding Spatial Index */
    table = sqlite3_mprintf ("idx_%s_%s_geom", topo_name, which);
    sql = sqlite3_mprintf ("DROP TABLE IF EXISTS \"%s\"", table);
    ret = sqlite3_exec (handle, sql, NULL, NULL, &err_msg);
    sqlite3_free (table);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e
	      ("DROP SpatialIndex topology-%s - error: %s\n", which, err_msg);
	  sqlite3_free (err_msg);
	  return 0;
      }

    return 1;
}

static int
do_get_topology (sqlite3 * handle, const char *topo_name, char **topology_name,
		 int *srid, double *tolerance, int *has_z)
{
/* retrieving a Topology configuration */
    char *sql;
    int ret;
    sqlite3_stmt *stmt = NULL;
    int ok = 0;
    char *xtopology_name = NULL;
    int xsrid;
    double xtolerance;
    int xhas_z;

/* preparing the SQL query */
    sql =
	sqlite3_mprintf
	("SELECT topology_name, srid, tolerance, has_z FROM MAIN.topologies WHERE "
	 "Lower(topology_name) = Lower(%Q)", topo_name);
    ret = sqlite3_prepare_v2 (handle, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("SELECT FROM topologys error: \"%s\"\n",
			sqlite3_errmsg (handle));
	  return 0;
      }

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		int ok_name = 0;
		int ok_srid = 0;
		int ok_tolerance = 0;
		int ok_z = 0;
		if (sqlite3_column_type (stmt, 0) == SQLITE_TEXT)
		  {
		      const char *str =
			  (const char *) sqlite3_column_text (stmt, 0);
		      if (xtopology_name != NULL)
			  free (xtopology_name);
		      xtopology_name = malloc (strlen (str) + 1);
		      strcpy (xtopology_name, str);
		      ok_name = 1;
		  }
		if (sqlite3_column_type (stmt, 1) == SQLITE_INTEGER)
		  {
		      xsrid = sqlite3_column_int (stmt, 1);
		      ok_srid = 1;
		  }
		if (sqlite3_column_type (stmt, 2) == SQLITE_FLOAT)
		  {
		      xtolerance = sqlite3_column_double (stmt, 2);
		      ok_tolerance = 1;
		  }
		if (sqlite3_column_type (stmt, 3) == SQLITE_INTEGER)
		  {
		      xhas_z = sqlite3_column_int (stmt, 3);
		      ok_z = 1;
		  }
		if (ok_name && ok_srid && ok_tolerance && ok_z)
		  {
		      ok = 1;
		      break;
		  }
	    }
	  else
	    {
		spatialite_e
		    ("step: SELECT FROM topologies error: \"%s\"\n",
		     sqlite3_errmsg (handle));
		sqlite3_finalize (stmt);
		return 0;
	    }
      }
    sqlite3_finalize (stmt);

    if (ok)
      {
	  *topology_name = xtopology_name;
	  *srid = xsrid;
	  *tolerance = xtolerance;
	  *has_z = xhas_z;
	  return 1;
      }

    if (xtopology_name != NULL)
	free (xtopology_name);
    return 0;
}

GAIATOPO_DECLARE GaiaTopologyAccessorPtr
gaiaGetTopology (sqlite3 * handle, const void *cache, const char *topo_name)
{
/* attempting to get a reference to some Topology Accessor Object */
    GaiaTopologyAccessorPtr accessor;

/* attempting to retrieve an alredy cached definition */
    accessor = gaiaTopologyFromCache (cache, topo_name);
    if (accessor != NULL)
	return accessor;

/* attempting to create a new Topology Accessor */
    accessor = gaiaTopologyFromDBMS (handle, cache, topo_name);
    return accessor;
}

GAIATOPO_DECLARE GaiaTopologyAccessorPtr
gaiaTopologyFromCache (const void *p_cache, const char *topo_name)
{
/* attempting to retrieve an already defined Topology Accessor Object from the Connection Cache */
    struct gaia_topology *ptr;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache == 0)
	return NULL;

    ptr = (struct gaia_topology *) (cache->firstTopology);
    while (ptr != NULL)
      {
	  /* checking for an already registered Topology */
	  if (strcasecmp (topo_name, ptr->topology_name) == 0)
	      return (GaiaTopologyAccessorPtr) ptr;
	  ptr = ptr->next;
      }
    return NULL;
}

GAIATOPO_DECLARE int
gaiaReadTopologyFromDBMS (sqlite3 *
			  handle,
			  const char
			  *topo_name, char **topology_name, int *srid,
			  double *tolerance, int *has_z)
{
/* testing for existing DBMS objects */
    if (!check_existing_topology (handle, topo_name, 1))
	return 0;

/* retrieving the Topology configuration */
    if (!do_get_topology
	(handle, topo_name, topology_name, srid, tolerance, has_z))
	return 0;
    return 1;
}

GAIATOPO_DECLARE GaiaTopologyAccessorPtr
gaiaTopologyFromDBMS (sqlite3 * handle, const void *p_cache,
		      const char *topo_name)
{
/* attempting to create a Topology Accessor Object into the Connection Cache */
    LWT_BE_CALLBACKS *callbacks;
    struct gaia_topology *ptr;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache == 0)
	return NULL;

/* allocating and initializing the opaque object */
    ptr = malloc (sizeof (struct gaia_topology));
    ptr->db_handle = handle;
    ptr->cache = cache;
    ptr->topology_name = NULL;
    ptr->srid = -1;
    ptr->tolerance = 0;
    ptr->has_z = 0;
    ptr->inside_lwt_callback = 0;
    ptr->last_error_message = NULL;
    ptr->lwt_iface = lwt_CreateBackendIface ((const LWT_BE_DATA *) ptr);
    ptr->prev = cache->lastTopology;
    ptr->next = NULL;

    callbacks = malloc (sizeof (LWT_BE_CALLBACKS));
    callbacks->lastErrorMessage = callback_lastErrorMessage;
    callbacks->topoGetSRID = callback_topoGetSRID;
    callbacks->topoGetPrecision = callback_topoGetPrecision;
    callbacks->topoHasZ = callback_topoHasZ;
    callbacks->createTopology = NULL;
    callbacks->loadTopologyByName = callback_loadTopologyByName;
    callbacks->freeTopology = callback_freeTopology;
    callbacks->getNodeById = callback_getNodeById;
    callbacks->getNodeWithinDistance2D = callback_getNodeWithinDistance2D;
    callbacks->insertNodes = callback_insertNodes;
    callbacks->getEdgeById = callback_getEdgeById;
    callbacks->getEdgeWithinDistance2D = callback_getEdgeWithinDistance2D;
    callbacks->getNextEdgeId = callback_getNextEdgeId;
    callbacks->insertEdges = callback_insertEdges;
    callbacks->updateEdges = callback_updateEdges;
    callbacks->getFaceById = callback_getFaceById;
    callbacks->getFaceContainingPoint = callback_getFaceContainingPoint;
    callbacks->deleteEdges = callback_deleteEdges;
    callbacks->getNodeWithinBox2D = callback_getNodeWithinBox2D;
    callbacks->getEdgeWithinBox2D = callback_getEdgeWithinBox2D;
    callbacks->getEdgeByNode = callback_getEdgeByNode;
    callbacks->updateNodes = callback_updateNodes;
    callbacks->insertFaces = callback_insertFaces;
    callbacks->updateFacesById = callback_updateFacesById;
    callbacks->deleteFacesById = callback_deleteFacesById;
    callbacks->getRingEdges = callback_getRingEdges;
    callbacks->updateEdgesById = callback_updateEdgesById;
    callbacks->getEdgeByFace = callback_getEdgeByFace;
    callbacks->getNodeByFace = callback_getNodeByFace;
    callbacks->updateNodesById = callback_updateNodesById;
    callbacks->deleteNodesById = callback_deleteNodesById;
    callbacks->updateTopoGeomEdgeSplit = callback_updateTopoGeomEdgeSplit;
    callbacks->updateTopoGeomFaceSplit = callback_updateTopoGeomFaceSplit;
    callbacks->checkTopoGeomRemEdge = callback_checkTopoGeomRemEdge;
    callbacks->updateTopoGeomFaceHeal = callback_updateTopoGeomFaceHeal;
    callbacks->checkTopoGeomRemNode = callback_checkTopoGeomRemNode;
    callbacks->updateTopoGeomEdgeHeal = callback_updateTopoGeomEdgeHeal;
    callbacks->getFaceWithinBox2D = callback_getFaceWithinBox2D;
    ptr->callbacks = callbacks;

    lwt_BackendIfaceRegisterCallbacks (ptr->lwt_iface, callbacks);
    ptr->lwt_topology = lwt_LoadTopology (ptr->lwt_iface, topo_name);

    ptr->stmt_getNodeWithinDistance2D = NULL;
    ptr->stmt_insertNodes = NULL;
    ptr->stmt_getEdgeWithinDistance2D = NULL;
    ptr->stmt_getNextEdgeId = NULL;
    ptr->stmt_setNextEdgeId = NULL;
    ptr->stmt_insertEdges = NULL;
    ptr->stmt_getFaceContainingPoint_1 = NULL;
    ptr->stmt_getFaceContainingPoint_2 = NULL;
    ptr->stmt_deleteEdges = NULL;
    ptr->stmt_getNodeWithinBox2D = NULL;
    ptr->stmt_getEdgeWithinBox2D = NULL;
    ptr->stmt_getFaceWithinBox2D = NULL;
    ptr->stmt_updateNodes = NULL;
    ptr->stmt_insertFaces = NULL;
    ptr->stmt_updateFacesById = NULL;
    ptr->stmt_deleteFacesById = NULL;
    ptr->stmt_deleteNodesById = NULL;
    ptr->stmt_getRingEdges = NULL;
    if (ptr->lwt_topology == NULL)
	goto invalid;

/* creating the SQL prepared statements */
    create_topogeo_prepared_stmts ((GaiaTopologyAccessorPtr) ptr);

    return (GaiaTopologyAccessorPtr) ptr;

  invalid:
    gaiaTopologyDestroy ((GaiaTopologyAccessorPtr) ptr);
    return NULL;
}

GAIATOPO_DECLARE void
gaiaTopologyDestroy (GaiaTopologyAccessorPtr topo_ptr)
{
/* destroying a Topology Accessor Object */
    struct gaia_topology *prev;
    struct gaia_topology *next;
    struct splite_internal_cache *cache;
    struct gaia_topology *ptr = (struct gaia_topology *) topo_ptr;
    if (ptr == NULL)
	return;

    prev = ptr->prev;
    next = ptr->next;
    cache = (struct splite_internal_cache *) (ptr->cache);
    if (ptr->lwt_topology != NULL)
	lwt_FreeTopology ((LWT_TOPOLOGY *) (ptr->lwt_topology));
    if (ptr->lwt_iface != NULL)
	lwt_FreeBackendIface ((LWT_BE_IFACE *) (ptr->lwt_iface));
    if (ptr->callbacks != NULL)
	free (ptr->callbacks);
    if (ptr->topology_name != NULL)
	free (ptr->topology_name);
    if (ptr->last_error_message != NULL)
	free (ptr->last_error_message);

    finalize_topogeo_prepared_stmts (topo_ptr);
    free (ptr);

/* unregistering from the Internal Cache double linked list */
    if (prev != NULL)
	prev->next = next;
    if (next != NULL)
	next->prev = prev;
    if (cache->firstTopology == ptr)
	cache->firstTopology = next;
    if (cache->lastTopology == ptr)
	cache->lastTopology = prev;
}

TOPOLOGY_PRIVATE void
finalize_topogeo_prepared_stmts (GaiaTopologyAccessorPtr accessor)
{
/* finalizing the SQL prepared statements */
    struct gaia_topology *ptr = (struct gaia_topology *) accessor;
    if (ptr->stmt_getNodeWithinDistance2D != NULL)
	sqlite3_finalize (ptr->stmt_getNodeWithinDistance2D);
    if (ptr->stmt_insertNodes != NULL)
	sqlite3_finalize (ptr->stmt_insertNodes);
    if (ptr->stmt_getEdgeWithinDistance2D != NULL)
	sqlite3_finalize (ptr->stmt_getEdgeWithinDistance2D);
    if (ptr->stmt_getNextEdgeId != NULL)
	sqlite3_finalize (ptr->stmt_getNextEdgeId);
    if (ptr->stmt_setNextEdgeId != NULL)
	sqlite3_finalize (ptr->stmt_setNextEdgeId);
    if (ptr->stmt_insertEdges != NULL)
	sqlite3_finalize (ptr->stmt_insertEdges);
    if (ptr->stmt_getFaceContainingPoint_1 != NULL)
	sqlite3_finalize (ptr->stmt_getFaceContainingPoint_1);
    if (ptr->stmt_getFaceContainingPoint_2 != NULL)
	sqlite3_finalize (ptr->stmt_getFaceContainingPoint_2);
    if (ptr->stmt_deleteEdges != NULL)
	sqlite3_finalize (ptr->stmt_deleteEdges);
    if (ptr->stmt_getNodeWithinBox2D != NULL)
	sqlite3_finalize (ptr->stmt_getNodeWithinBox2D);
    if (ptr->stmt_getEdgeWithinBox2D != NULL)
	sqlite3_finalize (ptr->stmt_getEdgeWithinBox2D);
    if (ptr->stmt_getFaceWithinBox2D != NULL)
	sqlite3_finalize (ptr->stmt_getFaceWithinBox2D);
    if (ptr->stmt_updateNodes != NULL)
	sqlite3_finalize (ptr->stmt_updateNodes);
    if (ptr->stmt_insertFaces != NULL)
	sqlite3_finalize (ptr->stmt_insertFaces);
    if (ptr->stmt_updateFacesById != NULL)
	sqlite3_finalize (ptr->stmt_updateFacesById);
    if (ptr->stmt_deleteFacesById != NULL)
	sqlite3_finalize (ptr->stmt_deleteFacesById);
    if (ptr->stmt_deleteNodesById != NULL)
	sqlite3_finalize (ptr->stmt_deleteNodesById);
    if (ptr->stmt_getRingEdges != NULL)
	sqlite3_finalize (ptr->stmt_getRingEdges);
    ptr->stmt_getNodeWithinDistance2D = NULL;
    ptr->stmt_insertNodes = NULL;
    ptr->stmt_getEdgeWithinDistance2D = NULL;
    ptr->stmt_getNextEdgeId = NULL;
    ptr->stmt_setNextEdgeId = NULL;
    ptr->stmt_insertEdges = NULL;
    ptr->stmt_getFaceContainingPoint_1 = NULL;
    ptr->stmt_getFaceContainingPoint_2 = NULL;
    ptr->stmt_deleteEdges = NULL;
    ptr->stmt_getNodeWithinBox2D = NULL;
    ptr->stmt_getEdgeWithinBox2D = NULL;
    ptr->stmt_getFaceWithinBox2D = NULL;
    ptr->stmt_updateNodes = NULL;
    ptr->stmt_insertFaces = NULL;
    ptr->stmt_updateFacesById = NULL;
    ptr->stmt_deleteFacesById = NULL;
    ptr->stmt_deleteNodesById = NULL;
    ptr->stmt_getRingEdges = NULL;
}

TOPOLOGY_PRIVATE void
create_topogeo_prepared_stmts (GaiaTopologyAccessorPtr accessor)
{
/* creating the SQL prepared statements */
    struct gaia_topology *ptr = (struct gaia_topology *) accessor;
    finalize_topogeo_prepared_stmts (accessor);
    ptr->stmt_getNodeWithinDistance2D =
	do_create_stmt_getNodeWithinDistance2D (accessor);
    ptr->stmt_insertNodes = do_create_stmt_insertNodes (accessor);
    ptr->stmt_getEdgeWithinDistance2D =
	do_create_stmt_getEdgeWithinDistance2D (accessor);
    ptr->stmt_getNextEdgeId = do_create_stmt_getNextEdgeId (accessor);
    ptr->stmt_setNextEdgeId = do_create_stmt_setNextEdgeId (accessor);
    ptr->stmt_insertEdges = do_create_stmt_insertEdges (accessor);
    ptr->stmt_getFaceContainingPoint_1 =
	do_create_stmt_getFaceContainingPoint_1 (accessor);
    ptr->stmt_getFaceContainingPoint_2 =
	do_create_stmt_getFaceContainingPoint_2 (accessor);
    ptr->stmt_deleteEdges = NULL;
    ptr->stmt_getNodeWithinBox2D = do_create_stmt_getNodeWithinBox2D (accessor);
    ptr->stmt_getEdgeWithinBox2D = do_create_stmt_getEdgeWithinBox2D (accessor);
    ptr->stmt_getFaceWithinBox2D = do_create_stmt_getFaceWithinBox2D (accessor);
    ptr->stmt_updateNodes = NULL;
    ptr->stmt_insertFaces = do_create_stmt_insertFaces (accessor);
    ptr->stmt_updateFacesById = do_create_stmt_updateFacesById (accessor);
    ptr->stmt_deleteFacesById = do_create_stmt_deleteFacesById (accessor);
    ptr->stmt_deleteNodesById = do_create_stmt_deleteNodesById (accessor);
    ptr->stmt_getRingEdges = do_create_stmt_getRingEdges (accessor);
}

TOPOLOGY_PRIVATE void
finalize_all_topo_prepared_stmts (const void *p_cache)
{
/* finalizing all Topology-related prepared Stms */
    struct gaia_topology *p_topo;
    struct gaia_network *p_network;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache == NULL)
	return;
    if (cache->magic1 != SPATIALITE_CACHE_MAGIC1
	|| cache->magic2 != SPATIALITE_CACHE_MAGIC2)
	return;

    p_topo = (struct gaia_topology *) cache->firstTopology;
    while (p_topo != NULL)
      {
	  finalize_topogeo_prepared_stmts ((GaiaTopologyAccessorPtr) p_topo);
	  p_topo = p_topo->next;
      }

    p_network = (struct gaia_network *) cache->firstNetwork;
    while (p_network != NULL)
      {
	  finalize_toponet_prepared_stmts ((GaiaNetworkAccessorPtr) p_network);
	  p_network = p_network->next;
      }
}

TOPOLOGY_PRIVATE void
create_all_topo_prepared_stmts (const void *p_cache)
{
/* (re)creating all Topology-related prepared Stms */
    struct gaia_topology *p_topo;
    struct gaia_network *p_network;
    struct splite_internal_cache *cache =
	(struct splite_internal_cache *) p_cache;
    if (cache == NULL)
	return;
    if (cache->magic1 != SPATIALITE_CACHE_MAGIC1
	|| cache->magic2 != SPATIALITE_CACHE_MAGIC2)
	return;

    p_topo = (struct gaia_topology *) cache->firstTopology;
    while (p_topo != NULL)
      {
	  create_topogeo_prepared_stmts ((GaiaTopologyAccessorPtr) p_topo);
	  p_topo = p_topo->next;
      }

    p_network = (struct gaia_network *) cache->firstNetwork;
    while (p_network != NULL)
      {
	  create_toponet_prepared_stmts ((GaiaNetworkAccessorPtr) p_network);
	  p_network = p_network->next;
      }
}

TOPOLOGY_PRIVATE void
gaiatopo_reset_last_error_msg (GaiaTopologyAccessorPtr accessor)
{
/* resets the last Topology error message */
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return;

    if (topo->last_error_message != NULL)
	free (topo->last_error_message);
    topo->last_error_message = NULL;
}

TOPOLOGY_PRIVATE void
gaiatopo_set_last_error_msg (GaiaTopologyAccessorPtr accessor, const char *msg)
{
/* sets the last Topology error message */
    int len;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (msg == NULL)
	msg = "no message available";

    spatialite_e ("%s\n", msg);
    if (topo == NULL)
	return;

    if (topo->last_error_message != NULL)
	return;

    len = strlen (msg);
    topo->last_error_message = malloc (len + 1);
    strcpy (topo->last_error_message, msg);
}

TOPOLOGY_PRIVATE const char *
gaiatopo_get_last_exception (GaiaTopologyAccessorPtr accessor)
{
/* returns the last Topology error message */
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return NULL;

    return topo->last_error_message;
}

GAIATOPO_DECLARE int
gaiaTopologyDrop (sqlite3 * handle, const char *topo_name)
{
/* attempting to drop an already existing Topology */
    int ret;
    char *sql;

/* creating the Topologies table (just in case) */
    if (!do_create_topologies (handle))
	return 0;

/* testing for existing DBMS objects */
    if (!check_existing_topology (handle, topo_name, 0))
	return 0;

/* dropping the Topology own Tables */
    if (!do_drop_topo_table (handle, topo_name, "edge"))
	goto error;
    if (!do_drop_topo_table (handle, topo_name, "node"))
	goto error;
    if (!do_drop_topo_table (handle, topo_name, "face"))
	goto error;

/* unregistering the Topology */
    sql =
	sqlite3_mprintf
	("DELETE FROM MAIN.topologies WHERE Lower(topology_name) = Lower(%Q)",
	 topo_name);
    ret = sqlite3_exec (handle, sql, NULL, NULL, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto error;

    return 1;

  error:
    return 0;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaAddIsoNode (GaiaTopologyAccessorPtr accessor,
		sqlite3_int64 face, gaiaPointPtr pt, int skip_checks)
{
/* LWT wrapper - AddIsoNode */
    sqlite3_int64 ret;
    int has_z = 0;
    LWPOINT *lw_pt;
    POINTARRAY *pa;
    POINT4D point;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    if (pt->DimensionModel == GAIA_XY_Z || pt->DimensionModel == GAIA_XY_Z_M)
	has_z = 1;
    pa = ptarray_construct (has_z, 0, 1);
    point.x = pt->X;
    point.y = pt->Y;
    if (has_z)
	point.z = pt->Z;
    ptarray_set_point4d (pa, 0, &point);
    lw_pt = lwpoint_construct (topo->srid, NULL, pa);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret =
	lwt_AddIsoNode ((LWT_TOPOLOGY *) (topo->lwt_topology), face, lw_pt,
			skip_checks);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    lwpoint_free (lw_pt);
    return ret;
}

GAIATOPO_DECLARE int
gaiaMoveIsoNode (GaiaTopologyAccessorPtr accessor,
		 sqlite3_int64 node, gaiaPointPtr pt)
{
/* LWT wrapper - MoveIsoNode */
    int ret;
    int has_z = 0;
    LWPOINT *lw_pt;
    POINTARRAY *pa;
    POINT4D point;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    if (pt->DimensionModel == GAIA_XY_Z || pt->DimensionModel == GAIA_XY_Z_M)
	has_z = 1;
    pa = ptarray_construct (has_z, 0, 1);
    point.x = pt->X;
    point.y = pt->Y;
    if (has_z)
	point.z = pt->Z;
    ptarray_set_point4d (pa, 0, &point);
    lw_pt = lwpoint_construct (topo->srid, NULL, pa);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret = lwt_MoveIsoNode ((LWT_TOPOLOGY *) (topo->lwt_topology), node, lw_pt);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    lwpoint_free (lw_pt);
    if (ret == 0)
	return 1;
    return 0;
}

GAIATOPO_DECLARE int
gaiaRemIsoNode (GaiaTopologyAccessorPtr accessor, sqlite3_int64 node)
{
/* LWT wrapper - RemIsoNode */
    int ret;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret = lwt_RemoveIsoNode ((LWT_TOPOLOGY *) (topo->lwt_topology), node);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    if (ret == 0)
	return 1;
    return 0;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaAddIsoEdge (GaiaTopologyAccessorPtr accessor,
		sqlite3_int64 start_node, sqlite3_int64 end_node,
		gaiaLinestringPtr ln)
{
/* LWT wrapper - AddIsoEdge */
    sqlite3_int64 ret;
    LWLINE *lw_line;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    lw_line = gaia_convert_linestring_to_lwline (ln, topo->srid, topo->has_z);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret =
	lwt_AddIsoEdge ((LWT_TOPOLOGY *) (topo->lwt_topology), start_node,
			end_node, lw_line);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    lwline_free (lw_line);
    return ret;
}

GAIATOPO_DECLARE int
gaiaChangeEdgeGeom (GaiaTopologyAccessorPtr accessor,
		    sqlite3_int64 edge_id, gaiaLinestringPtr ln)
{
/* LWT wrapper - ChangeEdgeGeom  */
    int ret;
    LWLINE *lw_line;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    lw_line = gaia_convert_linestring_to_lwline (ln, topo->srid, topo->has_z);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret =
	lwt_ChangeEdgeGeom ((LWT_TOPOLOGY *) (topo->lwt_topology), edge_id,
			    lw_line);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    lwline_free (lw_line);
    if (ret == 0)
	return 1;
    return 0;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaModEdgeSplit (GaiaTopologyAccessorPtr accessor,
		  sqlite3_int64 edge, gaiaPointPtr pt, int skip_checks)
{
/* LWT wrapper - ModEdgeSplit */
    sqlite3_int64 ret;
    int has_z = 0;
    LWPOINT *lw_pt;
    POINTARRAY *pa;
    POINT4D point;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    if (pt->DimensionModel == GAIA_XY_Z || pt->DimensionModel == GAIA_XY_Z_M)
	has_z = 1;
    pa = ptarray_construct (has_z, 0, 1);
    point.x = pt->X;
    point.y = pt->Y;
    if (has_z)
	point.z = pt->Z;
    ptarray_set_point4d (pa, 0, &point);
    lw_pt = lwpoint_construct (topo->srid, NULL, pa);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret =
	lwt_ModEdgeSplit ((LWT_TOPOLOGY *) (topo->lwt_topology), edge, lw_pt,
			  skip_checks);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    lwpoint_free (lw_pt);
    return ret;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaNewEdgesSplit (GaiaTopologyAccessorPtr accessor,
		   sqlite3_int64 edge, gaiaPointPtr pt, int skip_checks)
{
/* LWT wrapper - NewEdgesSplit */
    sqlite3_int64 ret;
    int has_z = 0;
    LWPOINT *lw_pt;
    POINTARRAY *pa;
    POINT4D point;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    if (pt->DimensionModel == GAIA_XY_Z || pt->DimensionModel == GAIA_XY_Z_M)
	has_z = 1;
    pa = ptarray_construct (has_z, 0, 1);
    point.x = pt->X;
    point.y = pt->Y;
    if (has_z)
	point.z = pt->Z;
    ptarray_set_point4d (pa, 0, &point);
    lw_pt = lwpoint_construct (topo->srid, NULL, pa);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret =
	lwt_NewEdgesSplit ((LWT_TOPOLOGY *) (topo->lwt_topology), edge, lw_pt,
			   skip_checks);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    lwpoint_free (lw_pt);
    return ret;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaAddEdgeModFace (GaiaTopologyAccessorPtr accessor,
		    sqlite3_int64 start_node, sqlite3_int64 end_node,
		    gaiaLinestringPtr ln, int skip_checks)
{
/* LWT wrapper - AddEdgeModFace */
    sqlite3_int64 ret;
    LWLINE *lw_line;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    lw_line = gaia_convert_linestring_to_lwline (ln, topo->srid, topo->has_z);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret =
	lwt_AddEdgeModFace ((LWT_TOPOLOGY *) (topo->lwt_topology), start_node,
			    end_node, lw_line, skip_checks);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    lwline_free (lw_line);
    return ret;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaAddEdgeNewFaces (GaiaTopologyAccessorPtr accessor,
		     sqlite3_int64 start_node, sqlite3_int64 end_node,
		     gaiaLinestringPtr ln, int skip_checks)
{
/* LWT wrapper - AddEdgeNewFaces */
    sqlite3_int64 ret;
    LWLINE *lw_line;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    lw_line = gaia_convert_linestring_to_lwline (ln, topo->srid, topo->has_z);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret =
	lwt_AddEdgeNewFaces ((LWT_TOPOLOGY *) (topo->lwt_topology), start_node,
			     end_node, lw_line, skip_checks);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    lwline_free (lw_line);
    return ret;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaRemEdgeNewFace (GaiaTopologyAccessorPtr accessor, sqlite3_int64 edge_id)
{
/* LWT wrapper - RemEdgeNewFace */
    sqlite3_int64 ret;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret = lwt_RemEdgeNewFace ((LWT_TOPOLOGY *) (topo->lwt_topology), edge_id);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    return ret;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaRemEdgeModFace (GaiaTopologyAccessorPtr accessor, sqlite3_int64 edge_id)
{
/* LWT wrapper - RemEdgeModFace */
    sqlite3_int64 ret;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret = lwt_RemEdgeModFace ((LWT_TOPOLOGY *) (topo->lwt_topology), edge_id);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    return ret;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaNewEdgeHeal (GaiaTopologyAccessorPtr accessor, sqlite3_int64 edge_id1,
		 sqlite3_int64 edge_id2)
{
/* LWT wrapper - NewEdgeHeal */
    sqlite3_int64 ret;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret =
	lwt_NewEdgeHeal ((LWT_TOPOLOGY *) (topo->lwt_topology), edge_id1,
			 edge_id2);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    return ret;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaModEdgeHeal (GaiaTopologyAccessorPtr accessor, sqlite3_int64 edge_id1,
		 sqlite3_int64 edge_id2)
{
/* LWT wrapper - ModEdgeHeal */
    sqlite3_int64 ret;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret =
	lwt_ModEdgeHeal ((LWT_TOPOLOGY *) (topo->lwt_topology), edge_id1,
			 edge_id2);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    return ret;
}

GAIATOPO_DECLARE gaiaGeomCollPtr
gaiaGetFaceGeometry (GaiaTopologyAccessorPtr accessor, sqlite3_int64 face)
{
/* LWT wrapper - GetFaceGeometry */
    LWGEOM *result = NULL;
    LWPOLY *lwpoly;
    int has_z = 0;
    POINTARRAY *pa;
    POINT4D pt4d;
    int iv;
    int ib;
    double x;
    double y;
    double z;
    gaiaGeomCollPtr geom;
    gaiaPolygonPtr pg;
    gaiaRingPtr rng;
    int dimension_model;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    if (topo->inside_lwt_callback == 0)
      {
	  /* locking the semaphore if not already inside a callback context */
	  splite_lwgeom_semaphore_lock ();
	  splite_lwgeomtopo_init ();
	  gaiaResetLwGeomMsg ();
      }

    result = lwt_GetFaceGeometry ((LWT_TOPOLOGY *) (topo->lwt_topology), face);

    if (topo->inside_lwt_callback == 0)
      {
	  /* unlocking the semaphore if not already inside a callback context */
	  splite_lwgeom_init ();
	  splite_lwgeom_semaphore_unlock ();
      }

    if (result == NULL)
	return NULL;

/* converting the result as a Gaia Geometry */
    lwpoly = (LWPOLY *) result;
    if (lwpoly->nrings <= 0)
      {
	  /* empty geometry */
	  lwgeom_free (result);
	  return NULL;
      }
    pa = lwpoly->rings[0];
    if (pa->npoints <= 0)
      {
	  /* empty geometry */
	  lwgeom_free (result);
	  return NULL;
      }
    if (FLAGS_GET_Z (pa->flags))
	has_z = 1;
    if (has_z)
      {
	  dimension_model = GAIA_XY_Z;
	  geom = gaiaAllocGeomCollXYZ ();
      }
    else
      {
	  dimension_model = GAIA_XY;
	  geom = gaiaAllocGeomColl ();
      }
    pg = gaiaAddPolygonToGeomColl (geom, pa->npoints, lwpoly->nrings - 1);
    rng = pg->Exterior;
    for (iv = 0; iv < pa->npoints; iv++)
      {
	  /* copying Exterior Ring vertices */
	  getPoint4d_p (pa, iv, &pt4d);
	  x = pt4d.x;
	  y = pt4d.y;
	  if (has_z)
	      z = pt4d.z;
	  else
	      z = 0.0;
	  if (dimension_model == GAIA_XY_Z)
	    {
		gaiaSetPointXYZ (rng->Coords, iv, x, y, z);
	    }
	  else
	    {
		gaiaSetPoint (rng->Coords, iv, x, y);
	    }
      }
    for (ib = 1; ib < lwpoly->nrings; ib++)
      {
	  has_z = 0;
	  pa = lwpoly->rings[ib];
	  if (FLAGS_GET_Z (pa->flags))
	      has_z = 1;
	  rng = gaiaAddInteriorRing (pg, ib - 1, pa->npoints);
	  for (iv = 0; iv < pa->npoints; iv++)
	    {
		/* copying Exterior Ring vertices */
		getPoint4d_p (pa, iv, &pt4d);
		x = pt4d.x;
		y = pt4d.y;
		if (has_z)
		    z = pt4d.z;
		else
		    z = 0.0;
		if (dimension_model == GAIA_XY_Z)
		  {
		      gaiaSetPointXYZ (rng->Coords, iv, x, y, z);
		  }
		else
		  {
		      gaiaSetPoint (rng->Coords, iv, x, y);
		  }
	    }
      }
    lwgeom_free (result);
    geom->DeclaredType = GAIA_POLYGON;
    geom->Srid = topo->srid;
    return geom;
}

static int
do_check_create_faceedges_table (GaiaTopologyAccessorPtr accessor)
{
/* attemtping to create or validate the target table */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    char *errMsg = NULL;
    int exists = 0;
    int ok_face_id = 0;
    int ok_sequence = 0;
    int ok_edge_id = 0;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

/* testing for an already existing table */
    table = sqlite3_mprintf ("%s_face_edges_temp", topo->topology_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("PRAGMA TEMP.table_info(\"%s\")", xtable);
    free (xtable);
    ret =
	sqlite3_get_table (topo->db_handle, sql, &results, &rows, &columns,
			   &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg = sqlite3_mprintf ("ST_GetFaceEdges exception: %s", errMsg);
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  sqlite3_free (errMsg);
	  return 0;
      }
    for (i = 1; i <= rows; i++)
      {
	  const char *name = results[(i * columns) + 1];
	  const char *type = results[(i * columns) + 2];
	  const char *notnull = results[(i * columns) + 3];
	  const char *dflt_value = results[(i * columns) + 4];
	  const char *pk = results[(i * columns) + 5];
	  if (strcmp (name, "face_id") == 0 && strcmp (type, "INTEGER") == 0
	      && strcmp (notnull, "1") == 0 && dflt_value == NULL
	      && strcmp (pk, "1") == 0)
	      ok_face_id = 1;
	  if (strcmp (name, "sequence") == 0 && strcmp (type, "INTEGER") == 0
	      && strcmp (notnull, "1") == 0 && dflt_value == NULL
	      && strcmp (pk, "2") == 0)
	      ok_sequence = 1;
	  if (strcmp (name, "edge_id") == 0 && strcmp (type, "INTEGER") == 0
	      && strcmp (notnull, "1") == 0 && dflt_value == NULL
	      && strcmp (pk, "0") == 0)
	      ok_edge_id = 1;
	  exists = 1;
      }
    sqlite3_free_table (results);
    if (ok_face_id && ok_sequence && ok_edge_id)
	return 1;		/* already existing and valid */

    if (exists)
	return 0;		/* already existing but invalid */

/* attempting to create the table */
    table = sqlite3_mprintf ("%s_face_edges_temp", topo->topology_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("CREATE TEMP TABLE \"%s\" (\n\tface_id INTEGER NOT NULL,\n"
	 "\tsequence INTEGER NOT NULL,\n\tedge_id INTEGER NOT NULL,\n"
	 "\tCONSTRAINT pk_topo_facee_edges PRIMARY KEY (face_id, sequence))",
	 xtable);
    free (xtable);
    ret = sqlite3_exec (topo->db_handle, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg = sqlite3_mprintf ("ST_GetFaceEdges exception: %s", errMsg);
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  sqlite3_free (errMsg);
	  return 0;
      }

    return 1;
}

static int
do_populate_faceedges_table (GaiaTopologyAccessorPtr accessor,
			     sqlite3_int64 face, LWT_ELEMID * edges,
			     int num_edges)
{
/* populating the target table */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    int i;
    sqlite3_stmt *stmt = NULL;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

/* deleting all rows belonging to Face (if any) */
    table = sqlite3_mprintf ("%s_face_edges_temp", topo->topology_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("DELETE FROM TEMP.\"%s\" WHERE face_id = ?", xtable);
    free (xtable);
    ret = sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg = sqlite3_mprintf ("ST_GetFaceEdges exception: %s",
				       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_int64 (stmt, 1, face);
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
      {
	  char *msg = sqlite3_mprintf ("ST_GetFaceEdges exception: %s",
				       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }
    sqlite3_finalize (stmt);
    stmt = NULL;

/* preparing the INSERT statement */
    table = sqlite3_mprintf ("%s_face_edges_temp", topo->topology_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("INSERT INTO TEMP.\"%s\" (face_id, sequence, edge_id) VALUES (?, ?, ?)",
	 xtable);
    free (xtable);
    ret = sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg = sqlite3_mprintf ("ST_GetFaceEdges exception: %s",
				       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }
    for (i = 0; i < num_edges; i++)
      {
	  /* inserting all Face/Edges */
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_int64 (stmt, 1, face);
	  sqlite3_bind_int (stmt, 2, i + 1);
	  sqlite3_bind_int64 (stmt, 3, *(edges + i));
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		char *msg = sqlite3_mprintf ("ST_GetFaceEdges exception: %s",
					     sqlite3_errmsg (topo->db_handle));
		gaiatopo_set_last_error_msg (accessor, msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt);
    return 1;

  error:
    if (stmt != NULL)
	sqlite3_finalize (stmt);
    return 0;
}

GAIATOPO_DECLARE int
gaiaGetFaceEdges (GaiaTopologyAccessorPtr accessor, sqlite3_int64 face)
{
/* LWT wrapper - GetFaceEdges */
    LWT_ELEMID *edges = NULL;
    int num_edges;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    /* locking the semaphore */
    splite_lwgeom_semaphore_lock ();
    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();

    num_edges =
	lwt_GetFaceEdges ((LWT_TOPOLOGY *) (topo->lwt_topology), face, &edges);

    /* unlocking the semaphore */
    splite_lwgeom_init ();
    splite_lwgeom_semaphore_unlock ();

    if (num_edges < 0)
	return 0;

    if (num_edges > 0)
      {
	  /* attemtping to create or validate the target table */
	  if (!do_check_create_faceedges_table (accessor))
	    {
		lwfree (edges);
		return 0;
	    }

	  /* populating the target table */
	  if (!do_populate_faceedges_table (accessor, face, edges, num_edges))
	    {
		lwfree (edges);
		return 0;
	    }
      }
    lwfree (edges);
    return 1;
}

static int
do_check_create_validate_topogeo_table (GaiaTopologyAccessorPtr accessor)
{
/* attemtping to create or validate the target table */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    char *errMsg = NULL;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

/* attempting to drop the table (just in case if it already exists) */
    table = sqlite3_mprintf ("%s_validate_topogeo", topo->topology_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("DROP TABLE IF EXISTS temp.\"%s\"", xtable);
    free (xtable);
    ret = sqlite3_exec (topo->db_handle, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("ST_ValidSpatialNet exception: %s", errMsg);
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  sqlite3_free (errMsg);
	  return 0;
      }
/* attempting to create the table */
    table = sqlite3_mprintf ("%s_validate_topogeo", topo->topology_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("CREATE TEMP TABLE \"%s\" (\n\terror TEXT,\n"
	 "\tprimitive1 INTEGER,\n\tprimitive2 INTEGER)", xtable);
    free (xtable);
    ret = sqlite3_exec (topo->db_handle, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("ST_ValidateTopoGeo exception: %s", errMsg);
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  sqlite3_free (errMsg);
	  return 0;
      }

    return 1;
}

static int
do_topo_check_coincident_nodes (GaiaTopologyAccessorPtr accessor,
				sqlite3_stmt * stmt)
{
/* checking for coincident nodes */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

    table = sqlite3_mprintf ("%s_node", topo->topology_name);
    xtable = gaiaDoubleQuotedSql (table);
    sql =
	sqlite3_mprintf ("SELECT n1.node_id, n2.node_id FROM MAIN.\"%s\" AS n1 "
			 "JOIN MAIN.\"%s\" AS n2 ON (n1.node_id <> n2.node_id AND "
			 "ST_Equals(n1.geom, n2.geom) = 1 AND n2.node_id IN "
			 "(SELECT rowid FROM SpatialIndex WHERE f_table_name = %Q AND "
			 "f_geometry_column = 'geom' AND search_frame = n1.geom))",
			 xtable, xtable, table);
    sqlite3_free (table);
    free (xtable);
    ret =
	sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf
	      ("ST_ValidateTopoGeo() - CoicidentNodes error: \"%s\"",
	       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 node_id1 = sqlite3_column_int64 (stmt_in, 0);
		sqlite3_int64 node_id2 = sqlite3_column_int64 (stmt_in, 1);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "coincident nodes", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, node_id1);
		sqlite3_bind_int64 (stmt, 3, node_id2);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidateTopoGeo() insert #1 error: \"%s\"",
			   sqlite3_errmsg (topo->db_handle));
		      gaiatopo_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidateTopoGeo() - CoicidentNodes step error: %s",
		     sqlite3_errmsg (topo->db_handle));
		gaiatopo_set_last_error_msg ((GaiaTopologyAccessorPtr) topo,
					     msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

static int
do_topo_check_edge_node (GaiaTopologyAccessorPtr accessor, sqlite3_stmt * stmt)
{
/* checking for edge-node crossing */
    char *sql;
    char *table;
    char *xtable1;
    char *xtable2;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

    table = sqlite3_mprintf ("%s_edge", topo->topology_name);
    xtable1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_node", topo->topology_name);
    xtable2 = gaiaDoubleQuotedSql (table);
    sql = sqlite3_mprintf ("SELECT e.edge_id, n.node_id FROM MAIN.\"%s\" AS e "
			   "JOIN MAIN.\"%s\" AS n ON (ST_Distance(e.geom, n.geom) <= 0 "
			   "AND ST_Disjoint(ST_StartPoint(e.geom), n.geom) = 1 AND "
			   "ST_Disjoint(ST_EndPoint(e.geom), n.geom) = 1 AND n.node_id IN "
			   "(SELECT rowid FROM SpatialIndex WHERE f_table_name = %Q AND "
			   "f_geometry_column = 'geom' AND search_frame = e.geom))",
			   xtable1, xtable2, table);
    sqlite3_free (table);
    free (xtable1);
    free (xtable2);
    ret =
	sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf
	      ("ST_ValidateTopoGeo() - EdgeCrossedNode error: \"%s\"",
	       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 edge_id = sqlite3_column_int64 (stmt_in, 0);
		sqlite3_int64 node_id = sqlite3_column_int64 (stmt_in, 1);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "edge crossed node", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, node_id);
		sqlite3_bind_int64 (stmt, 3, edge_id);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidateTopoGeo() insert #2 error: \"%s\"",
			   sqlite3_errmsg (topo->db_handle));
		      gaiatopo_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidateTopoGeo() - EdgeCrossedNode step error: %s",
		     sqlite3_errmsg (topo->db_handle));
		gaiatopo_set_last_error_msg ((GaiaTopologyAccessorPtr) topo,
					     msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

static int
do_topo_check_non_simple (GaiaTopologyAccessorPtr accessor, sqlite3_stmt * stmt)
{
/* checking for non-simple edges */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

    table = sqlite3_mprintf ("%s_edge", topo->topology_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("SELECT edge_id FROM MAIN.\"%s\" WHERE ST_IsSimple(geom) = 0", xtable);
    free (xtable);
    ret =
	sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf
	      ("ST_ValidateTopoGeo() - NonSimpleEdge error: \"%s\"",
	       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 edge_id = sqlite3_column_int64 (stmt_in, 0);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "edge not simple", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, edge_id);
		sqlite3_bind_null (stmt, 3);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidateTopoGeo() insert #3 error: \"%s\"",
			   sqlite3_errmsg (topo->db_handle));
		      gaiatopo_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidateTopoGeo() - NonSimpleEdge step error: %s",
		     sqlite3_errmsg (topo->db_handle));
		gaiatopo_set_last_error_msg ((GaiaTopologyAccessorPtr) topo,
					     msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

static int
do_topo_check_edge_edge (GaiaTopologyAccessorPtr accessor, sqlite3_stmt * stmt)
{
/* checking for edge-edge crossing */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

    table = sqlite3_mprintf ("%s_edge", topo->topology_name);
    xtable = gaiaDoubleQuotedSql (table);
    sql =
	sqlite3_mprintf ("SELECT e1.edge_id, e2.edge_id FROM MAIN.\"%s\" AS e1 "
			 "JOIN MAIN.\"%s\" AS e2 ON (e1.edge_id <> e2.edge_id AND "
			 "ST_Crosses(e1.geom, e2.geom) = 1 AND e2.edge_id IN "
			 "(SELECT rowid FROM SpatialIndex WHERE f_table_name = %Q AND "
			 "f_geometry_column = 'geom' AND search_frame = e1.geom))",
			 xtable, xtable, table);
    sqlite3_free (table);
    free (xtable);
    ret =
	sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf
	      ("ST_ValidateTopoGeo() - EdgeCrossesEdge error: \"%s\"",
	       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 edge_id1 = sqlite3_column_int64 (stmt_in, 0);
		sqlite3_int64 edge_id2 = sqlite3_column_int64 (stmt_in, 1);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "edge crosses edge", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, edge_id1);
		sqlite3_bind_int64 (stmt, 3, edge_id2);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidateTopoGeo() insert #4 error: \"%s\"",
			   sqlite3_errmsg (topo->db_handle));
		      gaiatopo_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidateTopoGeo() - EdgeCrossesEdge step error: %s",
		     sqlite3_errmsg (topo->db_handle));
		gaiatopo_set_last_error_msg ((GaiaTopologyAccessorPtr) topo,
					     msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

static int
do_topo_check_start_nodes (GaiaTopologyAccessorPtr accessor,
			   sqlite3_stmt * stmt)
{
/* checking for edges mismatching start nodes */
    char *sql;
    char *table;
    char *xtable1;
    char *xtable2;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

    table = sqlite3_mprintf ("%s_edge", topo->topology_name);
    xtable1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_node", topo->topology_name);
    xtable2 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf ("SELECT e.edge_id, e.start_node FROM MAIN.\"%s\" AS e "
			 "JOIN MAIN.\"%s\" AS n ON (e.start_node = n.node_id) "
			 "WHERE ST_Disjoint(ST_StartPoint(e.geom), n.geom) = 1",
			 xtable1, xtable2);
    free (xtable1);
    free (xtable2);
    ret =
	sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf
	      ("ST_ValidateTopoGeo() - StartNodes error: \"%s\"",
	       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 edge_id = sqlite3_column_int64 (stmt_in, 0);
		sqlite3_int64 node_id = sqlite3_column_int64 (stmt_in, 1);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "geometry start mismatch", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, edge_id);
		sqlite3_bind_int64 (stmt, 3, node_id);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidateTopoGeo() insert #5 error: \"%s\"",
			   sqlite3_errmsg (topo->db_handle));
		      gaiatopo_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidateTopoGeo() - StartNodes step error: %s",
		     sqlite3_errmsg (topo->db_handle));
		gaiatopo_set_last_error_msg ((GaiaTopologyAccessorPtr) topo,
					     msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

static int
do_topo_check_end_nodes (GaiaTopologyAccessorPtr accessor, sqlite3_stmt * stmt)
{
/* checking for edges mismatching end nodes */
    char *sql;
    char *table;
    char *xtable1;
    char *xtable2;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

    table = sqlite3_mprintf ("%s_edge", topo->topology_name);
    xtable1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_node", topo->topology_name);
    xtable2 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("SELECT e.edge_id, e.end_node FROM MAIN.\"%s\" AS e "
			   "JOIN MAIN.\"%s\" AS n ON (e.end_node = n.node_id) "
			   "WHERE ST_Disjoint(ST_EndPoint(e.geom), n.geom) = 1",
			   xtable1, xtable2);
    free (xtable1);
    free (xtable2);
    ret =
	sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("ST_ValidateTopoGeo() - EndNodes error: \"%s\"",
			       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 edge_id = sqlite3_column_int64 (stmt_in, 0);
		sqlite3_int64 node_id = sqlite3_column_int64 (stmt_in, 1);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "geometry end mismatch", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, edge_id);
		sqlite3_bind_int64 (stmt, 3, node_id);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidateTopoGeo() insert #6 error: \"%s\"",
			   sqlite3_errmsg (topo->db_handle));
		      gaiatopo_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidateTopoGeo() - EndNodes step error: %s",
		     sqlite3_errmsg (topo->db_handle));
		gaiatopo_set_last_error_msg ((GaiaTopologyAccessorPtr) topo,
					     msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

static int
do_topo_check_face_no_edges (GaiaTopologyAccessorPtr accessor,
			     sqlite3_stmt * stmt)
{
/* checking for faces with no edges */
    char *sql;
    char *table;
    char *xtable1;
    char *xtable2;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

    table = sqlite3_mprintf ("%s_face", topo->topology_name);
    xtable1 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_edge", topo->topology_name);
    xtable2 = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("SELECT f.face_id, Count(e1.edge_id) AS cnt1, "
			   "Count(e2.edge_id) AS cnt2 FROM MAIN.\"%s\" AS f "
			   "LEFT JOIN MAIN.\"%s\" AS e1 ON (f.face_id = e1.left_face) "
			   "LEFT JOIN MAIN.\"%s\" AS e2 ON (f.face_id = e2.right_face) "
			   "GROUP BY f.face_id HAVING cnt1 = 0 AND cnt2 = 0",
			   xtable1, xtable2, xtable2);
    free (xtable1);
    free (xtable2);
    ret =
	sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf
	      ("ST_ValidateTopoGeo() - FaceNoEdges error: \"%s\"",
	       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 face_id = sqlite3_column_int64 (stmt_in, 0);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "face without edges", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, face_id);
		sqlite3_bind_null (stmt, 3);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidateTopoGeo() insert #7 error: \"%s\"",
			   sqlite3_errmsg (topo->db_handle));
		      gaiatopo_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidateTopoGeo() - FaceNoEdges step error: %s",
		     sqlite3_errmsg (topo->db_handle));
		gaiatopo_set_last_error_msg ((GaiaTopologyAccessorPtr) topo,
					     msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

static int
do_topo_check_no_universal_face (GaiaTopologyAccessorPtr accessor,
				 sqlite3_stmt * stmt)
{
/* checking for missing universal face */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    char *errMsg = NULL;
    int count = 0;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

    table = sqlite3_mprintf ("%s_face", topo->topology_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf ("SELECT Count(*) FROM MAIN.\"%s\" WHERE face_id = 0",
			 xtable);
    free (xtable);
    ret =
	sqlite3_get_table (topo->db_handle, sql, &results, &rows, &columns,
			   &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  sqlite3_free (errMsg);
	  return 0;
      }
    for (i = 1; i <= rows; i++)
      {
	  count = atoi (results[(i * columns) + 0]);
      }
    sqlite3_free_table (results);

    if (count <= 0)
      {
	  /* reporting the error */
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_text (stmt, 1, "no universal face", -1, SQLITE_STATIC);
	  sqlite3_bind_null (stmt, 2);
	  sqlite3_bind_null (stmt, 3);
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidateTopoGeo() insert #8 error: \"%s\"",
		     sqlite3_errmsg (topo->db_handle));
		gaiatopo_set_last_error_msg (accessor, msg);
		sqlite3_free (msg);
		return 0;
	    }
      }

    return 1;
}

static int
do_topo_check_create_aux_faces (GaiaTopologyAccessorPtr accessor)
{
/* creating the aux-Face temp table */
    char *table;
    char *xtable;
    char *sql;
    char *errMsg;
    int ret;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

/* creating the aux-face Temp Table */
    table = sqlite3_mprintf ("%s_aux_face_%d", topo->topology_name, getpid ());
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("CREATE TEMPORARY TABLE \"%s\" (\n"
			   "\tface_id INTEGER PRIMARY KEY,\n\tgeom BLOB)",
			   xtable);
    free (xtable);
    ret = sqlite3_exec (topo->db_handle, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("CREATE TEMPORARY TABLE aux_face - error: %s\n",
			       errMsg);
	  sqlite3_free (errMsg);
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  return 0;
      }

/* creating the exotic spatial index */
    table =
	sqlite3_mprintf ("%s_aux_face_%d_rtree", topo->topology_name,
			 getpid ());
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("CREATE VIRTUAL TABLE temp.\"%s\" USING RTree "
			   "(id_face, x_min, x_max, y_min, y_max)", xtable);
    free (xtable);
    ret = sqlite3_exec (topo->db_handle, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("CREATE TEMPORARY TABLE aux_face - error: %s\n",
			       errMsg);
	  sqlite3_free (errMsg);
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  return 0;
      }

    return 1;
}

static int
do_topo_check_build_aux_faces (GaiaTopologyAccessorPtr accessor,
			       sqlite3_stmt * stmt)
{
/* populating the aux-face Temp Table */
    char *sql;
    char *table;
    char *xtable;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    sqlite3_stmt *stmt_out = NULL;
    sqlite3_stmt *stmt_rtree = NULL;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

/* preparing the input SQL statement */
    table = sqlite3_mprintf ("%s_face", topo->topology_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("SELECT face_id, ST_GetFaceGeometry(%Q, face_id) "
			   "FROM MAIN.\"%s\" WHERE face_id <> 0",
			   topo->topology_name, xtable);
    free (xtable);
    ret =
	sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf
	      ("ST_ValidateTopoGeo() - GetFaceGeometry error: \"%s\"",
	       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

/* preparing the output SQL statement */
    table = sqlite3_mprintf ("%s_aux_face_%d", topo->topology_name, getpid ());
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("INSERT INTO TEMP.\"%s\" (face_id, geom) VALUES (?, ?)", xtable);
    free (xtable);
    ret =
	sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt_out,
			    NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("ST_ValidateTopoGeo() - AuxFace error: \"%s\"",
			       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

/* preparing the RTree SQL statement */
    table =
	sqlite3_mprintf ("%s_aux_face_%d_rtree", topo->topology_name,
			 getpid ());
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("INSERT INTO TEMP.\"%s\" "
			   "(id_face, x_min, x_max, y_min, y_max) VALUES (?, ?, ?, ?, ?)",
			   xtable);
    free (xtable);
    ret =
	sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt_rtree,
			    NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf
	      ("ST_ValidateTopoGeo() - AuxFaceRTree error: \"%s\"",
	       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		gaiaGeomCollPtr geom = NULL;
		const unsigned char *blob;
		int blob_sz;
		sqlite3_int64 face_id = sqlite3_column_int64 (stmt_in, 0);
		if (sqlite3_column_type (stmt_in, 1) == SQLITE_BLOB)
		  {
		      blob = sqlite3_column_blob (stmt_in, 1);
		      blob_sz = sqlite3_column_bytes (stmt_in, 1);
		      geom = gaiaFromSpatiaLiteBlobWkb (blob, blob_sz);
		  }
		if (geom == NULL)
		  {
		      /* reporting the error */
		      sqlite3_reset (stmt);
		      sqlite3_clear_bindings (stmt);
		      sqlite3_bind_text (stmt, 1, "invalid face geometry", -1,
					 SQLITE_STATIC);
		      sqlite3_bind_int64 (stmt, 2, face_id);
		      sqlite3_bind_null (stmt, 3);
		      ret = sqlite3_step (stmt);
		      if (ret == SQLITE_DONE || ret == SQLITE_ROW)
			  ;
		      else
			{
			    char *msg =
				sqlite3_mprintf
				("ST_ValidateTopoGeo() insert #9 error: \"%s\"",
				 sqlite3_errmsg (topo->db_handle));
			    gaiatopo_set_last_error_msg (accessor, msg);
			    sqlite3_free (msg);
			    goto error;
			}
		  }
		else
		  {
		      double xmin = geom->MinX;
		      double xmax = geom->MaxX;
		      double ymin = geom->MinY;
		      double ymax = geom->MaxY;
		      gaiaFreeGeomColl (geom);
		      /* inserting into AuxFace */
		      sqlite3_reset (stmt_out);
		      sqlite3_clear_bindings (stmt_out);
		      sqlite3_bind_int64 (stmt_out, 1, face_id);
		      sqlite3_bind_blob (stmt_out, 2, blob, blob_sz,
					 SQLITE_STATIC);
		      ret = sqlite3_step (stmt_out);
		      if (ret == SQLITE_DONE || ret == SQLITE_ROW)
			  ;
		      else
			{
			    char *msg =
				sqlite3_mprintf
				("ST_ValidateTopoGeo() insert #10 error: \"%s\"",
				 sqlite3_errmsg (topo->db_handle));
			    gaiatopo_set_last_error_msg (accessor, msg);
			    sqlite3_free (msg);
			    goto error;
			}
		      /* updating the AuxFaceRTree */
		      sqlite3_reset (stmt_rtree);
		      sqlite3_clear_bindings (stmt_rtree);
		      sqlite3_bind_int64 (stmt_rtree, 1, face_id);
		      sqlite3_bind_double (stmt_rtree, 2, xmin);
		      sqlite3_bind_double (stmt_rtree, 3, xmax);
		      sqlite3_bind_double (stmt_rtree, 4, ymin);
		      sqlite3_bind_double (stmt_rtree, 5, ymax);
		      ret = sqlite3_step (stmt_rtree);
		      if (ret == SQLITE_DONE || ret == SQLITE_ROW)
			  ;
		      else
			{
			    char *msg =
				sqlite3_mprintf
				("ST_ValidateTopoGeo() insert #11 error: \"%s\"",
				 sqlite3_errmsg (topo->db_handle));
			    gaiatopo_set_last_error_msg (accessor, msg);
			    sqlite3_free (msg);
			    goto error;
			}
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidateTopoGeo() - GetFaceGeometry step error: %s",
		     sqlite3_errmsg (topo->db_handle));
		gaiatopo_set_last_error_msg ((GaiaTopologyAccessorPtr) topo,
					     msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);
    sqlite3_finalize (stmt_out);
    sqlite3_finalize (stmt_rtree);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    if (stmt_out == NULL)
	sqlite3_finalize (stmt_out);
    if (stmt_rtree == NULL)
	sqlite3_finalize (stmt_rtree);
    return 0;
}

static int
do_topo_check_overlapping_faces (GaiaTopologyAccessorPtr accessor,
				 sqlite3_stmt * stmt)
{
/* checking for overlapping faces */
    char *sql;
    char *table;
    char *xtable;
    char *rtree;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;


    table = sqlite3_mprintf ("%s_aux_face_%d", topo->topology_name, getpid ());
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_aux_face_%d_rtree", topo->topology_name,
			     getpid ());
    rtree = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("SELECT a.face_id, b.face_id FROM TEMP.\"%s\" AS a, TEMP.\"%s\" AS b "
	 "WHERE a.geom IS NOT NULL AND a.face_id <> b.face_id AND ST_Overlaps(a.geom, b.geom) = 1 "
	 "AND b.face_id IN (SELECT id_face FROM TEMP.\"%s\" WHERE x_min <= MbrMaxX(a.geom) "
	 "AND x_max >= MbrMinX(a.geom) AND y_min <= MbrMaxY(a.geom) AND y_max >= MbrMinY(a.geom))",
	 xtable, xtable, rtree);
    free (xtable);
    free (rtree);
    ret =
	sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf
	      ("ST_ValidateTopoGeo() - OverlappingFaces error: \"%s\"",
	       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 face_id1 = sqlite3_column_int64 (stmt_in, 0);
		sqlite3_int64 face_id2 = sqlite3_column_int64 (stmt_in, 1);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "face overlaps face", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, face_id1);
		sqlite3_bind_int64 (stmt, 3, face_id2);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidateTopoGeo() insert #12 error: \"%s\"",
			   sqlite3_errmsg (topo->db_handle));
		      gaiatopo_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidateTopoGeo() - OverlappingFaces step error: %s",
		     sqlite3_errmsg (topo->db_handle));
		gaiatopo_set_last_error_msg ((GaiaTopologyAccessorPtr) topo,
					     msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

static int
do_topo_check_face_within_face (GaiaTopologyAccessorPtr accessor,
				sqlite3_stmt * stmt)
{
/* checking for face-within-face */
    char *sql;
    char *table;
    char *xtable;
    char *rtree;
    int ret;
    sqlite3_stmt *stmt_in = NULL;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;


    table = sqlite3_mprintf ("%s_aux_face_%d", topo->topology_name, getpid ());
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    table = sqlite3_mprintf ("%s_aux_face_%d_rtree", topo->topology_name,
			     getpid ());
    rtree = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("SELECT a.face_id, b.face_id FROM TEMP.\"%s\" AS a, TEMP.\"%s\" AS b "
	 "WHERE a.geom IS NOT NULL AND a.face_id <> b.face_id AND ST_Within(a.geom, b.geom) = 1 "
	 "AND b.face_id IN (SELECT id_face FROM TEMP.\"%s\" WHERE x_min <= MbrMaxX(a.geom) "
	 "AND x_max >= MbrMinX(a.geom) AND y_min <= MbrMaxY(a.geom) AND y_max >= MbrMinY(a.geom))",
	 xtable, xtable, rtree);
    free (xtable);
    free (rtree);
    ret =
	sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt_in, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf
	      ("ST_ValidateTopoGeo() - FaceWithinFace error: \"%s\"",
	       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    sqlite3_reset (stmt_in);
    sqlite3_clear_bindings (stmt_in);
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt_in);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		sqlite3_int64 face_id1 = sqlite3_column_int64 (stmt_in, 0);
		sqlite3_int64 face_id2 = sqlite3_column_int64 (stmt_in, 1);
		/* reporting the error */
		sqlite3_reset (stmt);
		sqlite3_clear_bindings (stmt);
		sqlite3_bind_text (stmt, 1, "face within face", -1,
				   SQLITE_STATIC);
		sqlite3_bind_int64 (stmt, 2, face_id1);
		sqlite3_bind_int64 (stmt, 3, face_id2);
		ret = sqlite3_step (stmt);
		if (ret == SQLITE_DONE || ret == SQLITE_ROW)
		    ;
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("ST_ValidateTopoGeo() insert #13 error: \"%s\"",
			   sqlite3_errmsg (topo->db_handle));
		      gaiatopo_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf
		    ("ST_ValidateTopoGeo() - FaceWithinFace step error: %s",
		     sqlite3_errmsg (topo->db_handle));
		gaiatopo_set_last_error_msg ((GaiaTopologyAccessorPtr) topo,
					     msg);
		sqlite3_free (msg);
		goto error;
	    }
      }
    sqlite3_finalize (stmt_in);

    return 1;

  error:
    if (stmt_in == NULL)
	sqlite3_finalize (stmt_in);
    return 0;
}

static int
do_topo_check_drop_aux_faces (GaiaTopologyAccessorPtr accessor)
{
/* dropping the aux-Face temp table */
    char *table;
    char *xtable;
    char *sql;
    char *errMsg;
    int ret;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;

/* dropping the aux-face Temp Table */
    table = sqlite3_mprintf ("%s_aux_face_%d", topo->topology_name, getpid ());
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("DROP TABLE TEMP.\"%s\"", xtable);
    free (xtable);
    ret = sqlite3_exec (topo->db_handle, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg = sqlite3_mprintf ("DROP TABLE temp.aux_face - error: %s\n",
				       errMsg);
	  sqlite3_free (errMsg);
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  return 0;
      }

/* dropping the aux-face Temp RTree */
    table =
	sqlite3_mprintf ("%s_aux_face_%d_rtree", topo->topology_name,
			 getpid ());
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql = sqlite3_mprintf ("DROP TABLE TEMP.\"%s\"", xtable);
    free (xtable);
    ret = sqlite3_exec (topo->db_handle, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg =
	      sqlite3_mprintf ("DROP TABLE temp.aux_face_rtree - error: %s\n",
			       errMsg);
	  sqlite3_free (errMsg);
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  return 0;
      }

    return 1;
}

GAIATOPO_DECLARE int
gaiaValidateTopoGeo (GaiaTopologyAccessorPtr accessor)
{
/* generating a validity report for a given Topology */
    char *table;
    char *xtable;
    char *sql;
    int ret;
    sqlite3_stmt *stmt = NULL;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    if (!do_check_create_validate_topogeo_table (accessor))
	return 0;

    table = sqlite3_mprintf ("%s_validate_topogeo", topo->topology_name);
    xtable = gaiaDoubleQuotedSql (table);
    sqlite3_free (table);
    sql =
	sqlite3_mprintf
	("INSERT INTO TEMP.\"%s\" (error, primitive1, primitive2) VALUES (?, ?, ?)",
	 xtable);
    free (xtable);
    ret = sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg = sqlite3_mprintf ("ST_ValidateTopoGeo error: \"%s\"",
				       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

    if (!do_topo_check_coincident_nodes (accessor, stmt))
	goto error;

    if (!do_topo_check_edge_node (accessor, stmt))
	goto error;

    if (!do_topo_check_non_simple (accessor, stmt))
	goto error;

    if (!do_topo_check_edge_edge (accessor, stmt))
	goto error;

    if (!do_topo_check_start_nodes (accessor, stmt))
	goto error;

    if (!do_topo_check_end_nodes (accessor, stmt))
	goto error;

    if (!do_topo_check_face_no_edges (accessor, stmt))
	goto error;

    if (!do_topo_check_no_universal_face (accessor, stmt))
	goto error;

    if (!do_topo_check_create_aux_faces (accessor))
	goto error;

    if (!do_topo_check_build_aux_faces (accessor, stmt))
	goto error;

    if (!do_topo_check_overlapping_faces (accessor, stmt))
	goto error;

    if (!do_topo_check_face_within_face (accessor, stmt))
	goto error;

    if (!do_topo_check_drop_aux_faces (accessor))
	goto error;

    sqlite3_finalize (stmt);
    return 1;

  error:
    if (stmt != NULL)
	sqlite3_finalize (stmt);
    return 0;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaGetNodeByPoint (GaiaTopologyAccessorPtr accessor, gaiaPointPtr pt,
		    double tolerance)
{
/* LWT wrapper - GetNodeByPoint */
    sqlite3_int64 ret;
    int has_z = 0;
    LWPOINT *lw_pt;
    POINTARRAY *pa;
    POINT4D point;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    if (pt->DimensionModel == GAIA_XY_Z || pt->DimensionModel == GAIA_XY_Z_M)
	has_z = 1;
    pa = ptarray_construct (has_z, 0, 1);
    point.x = pt->X;
    point.y = pt->Y;
    if (has_z)
	point.z = pt->Z;
    ptarray_set_point4d (pa, 0, &point);
    lw_pt = lwpoint_construct (topo->srid, NULL, pa);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret =
	lwt_GetNodeByPoint ((LWT_TOPOLOGY *) (topo->lwt_topology), lw_pt,
			    tolerance);
    lwpoint_free (lw_pt);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    return ret;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaGetEdgeByPoint (GaiaTopologyAccessorPtr accessor, gaiaPointPtr pt,
		    double tolerance)
{
/* LWT wrapper - GetEdgeByPoint */
    sqlite3_int64 ret;
    int has_z = 0;
    LWPOINT *lw_pt;
    POINTARRAY *pa;
    POINT4D point;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    if (pt->DimensionModel == GAIA_XY_Z || pt->DimensionModel == GAIA_XY_Z_M)
	has_z = 1;
    pa = ptarray_construct (has_z, 0, 1);
    point.x = pt->X;
    point.y = pt->Y;
    if (has_z)
	point.z = pt->Z;
    ptarray_set_point4d (pa, 0, &point);
    lw_pt = lwpoint_construct (topo->srid, NULL, pa);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret =
	lwt_GetEdgeByPoint ((LWT_TOPOLOGY *) (topo->lwt_topology), lw_pt,
			    tolerance);
    lwpoint_free (lw_pt);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    return ret;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaGetFaceByPoint (GaiaTopologyAccessorPtr accessor, gaiaPointPtr pt,
		    double tolerance)
{
/* LWT wrapper - GetFaceByPoint */
    sqlite3_int64 ret;
    int has_z = 0;
    LWPOINT *lw_pt;
    POINTARRAY *pa;
    POINT4D point;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    if (pt->DimensionModel == GAIA_XY_Z || pt->DimensionModel == GAIA_XY_Z_M)
	has_z = 1;
    pa = ptarray_construct (has_z, 0, 1);
    point.x = pt->X;
    point.y = pt->Y;
    if (has_z)
	point.z = pt->Z;
    ptarray_set_point4d (pa, 0, &point);
    lw_pt = lwpoint_construct (topo->srid, NULL, pa);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret =
	lwt_GetFaceByPoint ((LWT_TOPOLOGY *) (topo->lwt_topology), lw_pt,
			    tolerance);
    lwpoint_free (lw_pt);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    return ret;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaTopoGeo_AddPoint (GaiaTopologyAccessorPtr accessor, gaiaPointPtr pt,
		      double tolerance)
{
/* LWT wrapper - AddPoint */
    sqlite3_int64 ret;
    int has_z = 0;
    LWPOINT *lw_pt;
    POINTARRAY *pa;
    POINT4D point;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    if (pt->DimensionModel == GAIA_XY_Z || pt->DimensionModel == GAIA_XY_Z_M)
	has_z = 1;
    pa = ptarray_construct (has_z, 0, 1);
    point.x = pt->X;
    point.y = pt->Y;
    if (has_z)
	point.z = pt->Z;
    ptarray_set_point4d (pa, 0, &point);
    lw_pt = lwpoint_construct (topo->srid, NULL, pa);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    ret =
	lwt_AddPoint ((LWT_TOPOLOGY *) (topo->lwt_topology), lw_pt, tolerance);
    lwpoint_free (lw_pt);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    return ret;
}

static sqlite3_int64
do_add_split_linestring (GaiaTopologyAccessorPtr accessor,
			 gaiaLinestringPtr line, double tolerance,
			 int line_max_points)
{
/* attempting to split a Linestrings into many simpler Edges */
    sqlite3_int64 value = -1;
    sqlite3_int64 ret = -1;
    gaiaLinestringPtr ln;
    int num_lines;
    int mean_points;
    int points;
    int iv;
    int pout = 0;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return -1;

/* allocating the first split Edge */
    num_lines = line->Points / line_max_points;
    if ((num_lines * line_max_points) < line->Points)
	num_lines++;
    points = line->Points / num_lines;
    if ((points * num_lines) < line->Points)
	points++;
    mean_points = points;
    if (line->DimensionModel == GAIA_XY_Z
	|| line->DimensionModel == GAIA_XY_Z_M)
	ln = gaiaAllocLinestringXYZ (points);
    else
	ln = gaiaAllocLinestring (points);

    for (iv = 0; iv <= line->Points; iv++)
      {
	  /* consuming all Points from the input Linestring */
	  double x;
	  double y;
	  double z = 0.0;
	  double m = 0.0;
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
	  if (line->DimensionModel == GAIA_XY_Z
	      || line->DimensionModel == GAIA_XY_Z_M)
	    {
		gaiaSetPointXYZ (ln->Coords, pout, x, y, z);
	    }
	  else
	    {
		gaiaSetPoint (ln->Coords, pout, x, y);
	    }
	  pout++;
	  if (pout < points)
	    {
		/* continuing to add Points into the same Edge */
		continue;
	    }

	  /* inserting the current Edge */
	  ret = gaiaTopoGeo_AddLineString (accessor, ln, tolerance, -1);
	  gaiaFreeLinestring (ln);
	  if (ret < 0)
	      return -1;
	  if (value < 0)
	      value = ret;

	  /* allocating another split Edge */
	  points = line->Points - iv;
	  if (points <= 1)
	      break;
	  if (points <= line_max_points)
	    {
		if (line->DimensionModel == GAIA_XY_Z
		    || line->DimensionModel == GAIA_XY_Z_M)
		    ln = gaiaAllocLinestringXYZ (points);
		else
		    ln = gaiaAllocLinestring (points);
	    }
	  else
	    {
		if ((mean_points * line_max_points) < line->Points)
		    points = mean_points + 1;
		else
		    points = mean_points;
		if (line->DimensionModel == GAIA_XY_Z
		    || line->DimensionModel == GAIA_XY_Z_M)
		    ln = gaiaAllocLinestringXYZ (points);
		else
		    ln = gaiaAllocLinestring (points);
	    }
	  pout = 0;
	  if (line->DimensionModel == GAIA_XY_Z
	      || line->DimensionModel == GAIA_XY_Z_M)
	    {
		gaiaSetPointXYZ (ln->Coords, pout, x, y, z);
	    }
	  else
	    {
		gaiaSetPoint (ln->Coords, pout, x, y);
	    }
	  pout++;
      }

    return value;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaTopoGeo_AddLineString (GaiaTopologyAccessorPtr accessor,
			   gaiaLinestringPtr ln, double tolerance,
			   int line_max_points)
{
/* LWT wrapper - AddLinestring */
    sqlite3_int64 ret = -1;
    LWT_ELEMID *edgeids;
    int nedges;
    LWLINE *lw_line;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    if (line_max_points >= 64)
      {
	  if (ln->Points > line_max_points)
	    {
		/* attempting to split the Linestrings into many simpler Edges */
		return do_add_split_linestring (accessor, ln, tolerance,
						line_max_points);
	    }
      }

    lw_line = gaia_convert_linestring_to_lwline (ln, topo->srid, topo->has_z);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    edgeids =
	lwt_AddLine ((LWT_TOPOLOGY *) (topo->lwt_topology), lw_line, tolerance,
		     &nedges);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    lwline_free (lw_line);
    if (edgeids != NULL)
      {
	  ret = *edgeids;
	  lwfree (edgeids);
      }
    return ret;
}

static int
check_split_polygon (gaiaPolygonPtr pg, int ring_max_points)
{
/* checks if a Polygon is a candidate for splitting */
    gaiaRingPtr rng;
    int ib;

/* testing the Exterior Ring */
    rng = pg->Exterior;
    if (rng->Points > ring_max_points)
	return 1;
    for (ib = 0; ib < pg->NumInteriors; ib++)
      {
	  /* testing Interior Rings */
	  rng = pg->Interiors + ib;
	  if (rng->Points > ring_max_points)
	      return 1;
      }
    return 0;
}

static gaiaLinestringPtr
ring2line (gaiaRingPtr rng)
{
/* creating a Linestring from a Ring */
    gaiaLinestringPtr ln;
    int iv;

/* allocating the Linestring */
    if (rng->DimensionModel == GAIA_XY_Z || rng->DimensionModel == GAIA_XY_Z_M)
	ln = gaiaAllocLinestringXYZ (rng->Points);
    else
	ln = gaiaAllocLinestring (rng->Points);

    for (iv = 0; iv < rng->Points; iv++)
      {
	  /* copying all Ring points */
	  double x;
	  double y;
	  double z = 0.0;
	  double m = 0.0;
	  if (rng->DimensionModel == GAIA_XY_Z)
	    {
		gaiaGetPointXYZ (rng->Coords, iv, &x, &y, &z);
	    }
	  else if (rng->DimensionModel == GAIA_XY_M)
	    {
		gaiaGetPointXYM (rng->Coords, iv, &x, &y, &m);
	    }
	  else if (rng->DimensionModel == GAIA_XY_Z_M)
	    {
		gaiaGetPointXYZM (rng->Coords, iv, &x, &y, &z, &m);
	    }
	  else
	    {
		gaiaGetPoint (rng->Coords, iv, &x, &y);
	    }
	  if (rng->DimensionModel == GAIA_XY_Z
	      || rng->DimensionModel == GAIA_XY_Z_M)
	    {
		gaiaSetPointXYZ (ln->Coords, iv, x, y, z);
	    }
	  else
	    {
		gaiaSetPoint (ln->Coords, iv, x, y);
	    }
      }

    return ln;
}

static sqlite3_int64
do_split_ring (GaiaTopologyAccessorPtr accessor, gaiaRingPtr rng,
	       double tolerance, int ring_max_points)
{
/* attempting to split a Ring into many simpler Edges/Faces */
    sqlite3_int64 ret;
    sqlite3_int64 value = -1;
    struct aux_split_polygon *aux;
    struct aux_split_outer_edge *outer;
    struct aux_split_inner_edge *inner;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    struct splite_internal_cache *cache;
    if (topo == NULL)
	return 0;
    cache = (struct splite_internal_cache *) (topo->cache);

    if (rng->Points <= ring_max_points)
      {
	  /* ring already below threshold - inserting as is */
	  gaiaLinestringPtr ln = ring2line (rng);
	  ret = gaiaTopoGeo_AddLineString (accessor, ln, tolerance, -1);
	  gaiaFreeLinestring (ln);
	  if (ret < 0)
	      return -1;
	  return ret;
      }

/* splitting Edges and Faces */
    aux = create_aux_split_polygon (topo->has_z);
    aux_split_polygon_split_ring (aux, rng, ring_max_points);
    aux_split_polygon_inner_edges (topo, aux, rng, cache);
    inner = aux->first_inner;
    while (inner != NULL)
      {
	  /* inserting first all inner Edges */
	  if (inner->confirmed)
	    {
		ret =
		    gaiaTopoGeo_AddLineString (accessor, inner->edge, tolerance,
					       -1);
		if (ret < 0)
		    goto error;
		if (value < 0)
		    value = ret;
	    }
	  inner = inner->next;
      }
    outer = aux->first_outer;
    while (outer != NULL)
      {
	  /* then inserting all outer Edges */
	  ret =
	      gaiaTopoGeo_AddLineString (accessor, outer->edge, tolerance, -1);
	  if (ret < 0)
	      goto error;
	  if (value < 0)
	      value = ret;
	  outer = outer->next;
      }
    destroy_aux_split_polygon (aux);
    return value;

  error:
    destroy_aux_split_polygon (aux);
    return -1;
}

static sqlite3_int64
do_add_split_polygon (GaiaTopologyAccessorPtr accessor, gaiaPolygonPtr pg,
		      double tolerance, int ring_max_points)
{
/* attempting to split Rings into many simpler Edges/Faces */
    gaiaRingPtr rng;
    int ib;
    sqlite3_int64 ret;
    sqlite3_int64 value = -1;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

/* processing the Exterior Ring */
    rng = pg->Exterior;
    ret = do_split_ring (accessor, rng, tolerance, ring_max_points);
    if (ret < 0)
	return -1;
    if (value < 0)
	value = ret;

    for (ib = 0; ib < pg->NumInteriors; ib++)
      {
	  /* processing an Interior Ring */
	  rng = pg->Interiors + ib;
	  ret = do_split_ring (accessor, rng, tolerance, ring_max_points);
	  if (ret < 0)
	      return -1;
	  if (value < 0)
	      value = ret;
      }
    return value;
}

GAIATOPO_DECLARE sqlite3_int64
gaiaTopoGeo_AddPolygon (GaiaTopologyAccessorPtr accessor, gaiaPolygonPtr pg,
			double tolerance, int ring_max_points)
{
/* LWT wrapper - AddPolygon */
    sqlite3_int64 ret = -1;
    LWT_ELEMID *faceids;
    int nfaces;
    LWPOLY *lw_polyg;
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    if (topo == NULL)
	return 0;

    if (ring_max_points > 64)
      {
	  if (check_split_polygon (pg, ring_max_points))
	    {
		/* attempting to split Rings into many simpler Edges/Faces */
		return do_add_split_polygon (accessor, pg, tolerance,
					     ring_max_points);
	    }
      }

    lw_polyg = gaia_convert_polygon_to_lwpoly (pg, topo->srid, topo->has_z);

/* locking the semaphore */
    splite_lwgeom_semaphore_lock ();

    splite_lwgeomtopo_init ();
    gaiaResetLwGeomMsg ();
    faceids =
	lwt_AddPolygon ((LWT_TOPOLOGY *) (topo->lwt_topology), lw_polyg,
			tolerance, &nfaces);
    splite_lwgeom_init ();

/* unlocking the semaphore */
    splite_lwgeom_semaphore_unlock ();

    lwpoly_free (lw_polyg);
    if (faceids != NULL)
      {
	  if (nfaces <= 0)
	      ret = 0;
	  else
	      ret = *faceids;
	  lwfree (faceids);
      }
    return ret;
}

static int
do_insert_into_topology (GaiaTopologyAccessorPtr accessor, gaiaGeomCollPtr geom,
			 double tolerance, int line_max_points,
			 int ring_max_points)
{
/* processing all individual geometry items */
    gaiaPointPtr pt;
    gaiaLinestringPtr ln;
    gaiaPolygonPtr pg;

    pt = geom->FirstPoint;
    while (pt != NULL)
      {
	  /* looping on Point items */
	  if (gaiaTopoGeo_AddPoint (accessor, pt, tolerance) < 0)
	      return 0;
	  pt = pt->Next;
      }

    ln = geom->FirstLinestring;
    while (ln != NULL)
      {
	  /* looping on Linestrings items */
	  if (gaiaTopoGeo_AddLineString
	      (accessor, ln, tolerance, line_max_points) < 0)
	      return 0;
	  ln = ln->Next;
      }

    pg = geom->FirstPolygon;
    while (pg != NULL)
      {
	  /* looping on Polygon items */
	  if (gaiaTopoGeo_AddPolygon (accessor, pg, tolerance, ring_max_points)
	      < 0)
	      return 0;
	  pg = pg->Next;
      }
    return 1;
}

GAIATOPO_DECLARE int
gaiaTopoGeo_FromGeoTable (GaiaTopologyAccessorPtr accessor,
			  const char *db_prefix, const char *table,
			  const char *column, double tolerance,
			  int line_max_points, int ring_max_points)
{
/* attempting to import a whole GeoTable into a Topology-Geometry */
    struct gaia_topology *topo = (struct gaia_topology *) accessor;
    sqlite3_stmt *stmt = NULL;
    int ret;
    char *sql;
    char *xprefix;
    char *xtable;
    char *xcolumn;

/* building the SQL statement */
    xprefix = gaiaDoubleQuotedSql (db_prefix);
    xtable = gaiaDoubleQuotedSql (table);
    xcolumn = gaiaDoubleQuotedSql (column);
    sql =
	sqlite3_mprintf ("SELECT \"%s\" FROM \"%s\".\"%s\"", xcolumn,
			 xprefix, xtable);
    free (xprefix);
    free (xtable);
    free (xcolumn);
    ret = sqlite3_prepare_v2 (topo->db_handle, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  char *msg = sqlite3_mprintf ("TopoGeo_FromGeoTable error: \"%s\"",
				       sqlite3_errmsg (topo->db_handle));
	  gaiatopo_set_last_error_msg (accessor, msg);
	  sqlite3_free (msg);
	  goto error;
      }

/* setting up the prepared statement */
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);

    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		if (sqlite3_column_type (stmt, 0) == SQLITE_NULL)
		    continue;
		if (sqlite3_column_type (stmt, 0) == SQLITE_BLOB)
		  {
		      const unsigned char *blob = sqlite3_column_blob (stmt, 0);
		      int blob_sz = sqlite3_column_bytes (stmt, 0);
		      gaiaGeomCollPtr geom =
			  gaiaFromSpatiaLiteBlobWkb (blob, blob_sz);
		      if (geom != NULL)
			{
			    if (!do_insert_into_topology
				(accessor, geom, tolerance, line_max_points,
				 ring_max_points))
			      {
				  gaiaFreeGeomColl (geom);
				  goto error;
			      }
			    gaiaFreeGeomColl (geom);
			}
		      else
			{
			    char *msg =
				sqlite3_mprintf
				("TopoGeo_FromGeoTable error: Invalid Geometry");
			    gaiatopo_set_last_error_msg (accessor, msg);
			    sqlite3_free (msg);
			    goto error;
			}
		  }
		else
		  {
		      char *msg =
			  sqlite3_mprintf
			  ("TopoGeo_FromGeoTable error: not a BLOB value");
		      gaiatopo_set_last_error_msg (accessor, msg);
		      sqlite3_free (msg);
		      goto error;
		  }
	    }
	  else
	    {
		char *msg =
		    sqlite3_mprintf ("TopoGeo_FromGeoTable error: \"%s\"",
				     sqlite3_errmsg (topo->db_handle));
		gaiatopo_set_last_error_msg (accessor, msg);
		sqlite3_free (msg);
		goto error;
	    }
      }

    sqlite3_finalize (stmt);
    return 1;

  error:
    if (stmt != NULL)
	sqlite3_finalize (stmt);
    return 0;
}

#endif /* end TOPOLOGY conditionals */
