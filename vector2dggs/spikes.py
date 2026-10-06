"""
Removal of spikes: places where a polygon's ring runs out to a vertex and
back along almost the same line, enclosing a sliver of practically no
width. Opt-in (--drop-spikes), as it changes the geometry supplied.

A spike is harmless to a planar reading of the ring, but where edges are
read as great-circle arcs (S2) the arc bows away from the straight line
between its ends, and a vertex returning within that bow lands on the
other side of the outbound edge: the ring then crosses itself, and S2
cannot index it (#224).
"""

import numpy as np
import shapely
from shapely.geometry import MultiPolygon, Polygon


def _sliver_widths(pts: np.ndarray, geographic: bool, metres_per_unit: float):
    """
    For each vertex of an open ring: whether it is a tip (its two edges
    meet at an acute angle), and the width in metres of the sliver it
    tips - how far the end of its shorter edge lies from the line of its
    longer one, i.e. twice the triangle's area over the longer side.
    """
    u = np.roll(pts, 1, axis=0) - pts
    v = np.roll(pts, -1, axis=0) - pts
    if geographic:
        # shortest way round, then a local east-west scale at the tip
        u[:, 0] = (u[:, 0] + 180) % 360 - 180
        v[:, 0] = (v[:, 0] + 180) % 360 - 180
        scale = np.cos(np.radians(pts[:, 1]))
        u[:, 0] *= scale
        v[:, 0] *= scale
    u *= metres_per_unit
    v *= metres_per_unit
    cross = np.abs(u[:, 0] * v[:, 1] - u[:, 1] * v[:, 0])
    dot = (u * v).sum(axis=1)
    longer = np.maximum(np.hypot(*u.T), np.hypot(*v.T))
    with np.errstate(divide="ignore", invalid="ignore"):
        width = np.where(longer > 0, cross / longer, np.inf)
    return dot > 0, width


def _drop_ring_spikes(
    ring, tolerance_m: float, geographic: bool, metres_per_unit: float
) -> tuple[np.ndarray | None, int]:
    """
    The ring's coordinates with spike tips removed, or None if fewer than
    three vertices survive; and the number of vertices removed.

    A tip is a vertex where the ring doubles back: its edges meet at an
    acute angle, enclosing a sliver narrower than the tolerance. Repeats
    until no tip remains, since removing one tip of a spike that tapers
    along several vertices exposes the next, so such a spike is removed
    entirely, however long. A finger of constant width is not a spike: past
    its tip its sides meet the ring at right angles, and it is kept. Within
    a pass, a tip whose predecessor is also a tip waits for the next pass,
    as removing both at once would judge the second by a neighbour that is
    gone.
    """
    coords = np.asarray(ring.coords)
    pts = coords[:-1, :2].copy()
    removed = 0
    while len(pts) >= 3:
        acute, width = _sliver_widths(pts, geographic, metres_per_unit)
        tip = acute & (width < tolerance_m)
        if not tip.any():
            break
        drop = tip & ~np.roll(tip, 1)
        if not drop.any():  # every vertex a tip: take one at a time
            drop = np.zeros_like(tip)
            drop[np.flatnonzero(tip)[0]] = True
        pts = pts[~drop]
        removed += int(drop.sum())
    if removed == 0:
        return coords, 0
    if len(pts) < 3:
        return None, removed
    return np.vstack([pts, pts[:1]]), removed


def _drop_polygon_spikes(
    polygon: Polygon, tolerance_m: float, geographic: bool, metres_per_unit: float
) -> tuple[Polygon, int]:
    exterior, removed = _drop_ring_spikes(
        polygon.exterior, tolerance_m, geographic, metres_per_unit
    )
    if exterior is None:
        return Polygon(), removed
    holes = []
    for interior in polygon.interiors:
        hole, n = _drop_ring_spikes(interior, tolerance_m, geographic, metres_per_unit)
        removed += n
        if hole is not None:  # a hole that was all spike encloses nothing
            holes.append(hole)
    if removed == 0:
        return polygon, 0
    return Polygon(exterior, holes), removed


def drop_spikes(
    geom, tolerance_m: float, geographic: bool, metres_per_unit: float
) -> tuple[object, int]:
    """
    geom with the tips of spikes narrower than tolerance_m removed from
    every ring, and the number of vertices removed. Non-polygonal
    geometries pass through unchanged. A ring reduced to under three
    vertices was all spike: a hole is dropped, and an exterior leaves the
    polygon empty (dropped downstream as an empty geometry).

    Coordinates are in units of metres_per_unit (common._metres_per_unit);
    geographic says they are degrees, scaled east-west by latitude. Z is
    dropped from rings that change.
    """
    if isinstance(geom, Polygon):
        return _drop_polygon_spikes(geom, tolerance_m, geographic, metres_per_unit)
    if isinstance(geom, MultiPolygon):
        parts, removed = [], 0
        for part in geom.geoms:
            cleaned, n = _drop_polygon_spikes(
                part, tolerance_m, geographic, metres_per_unit
            )
            removed += n
            if not cleaned.is_empty:
                parts.append(cleaned)
        if removed == 0:
            return geom, 0
        return MultiPolygon(parts) if parts else shapely.MultiPolygon(), removed
    return geom, 0
