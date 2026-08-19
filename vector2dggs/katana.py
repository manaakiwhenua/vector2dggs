"""
Copyright (c) 2016, Joshua Arnott

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS “AS IS” AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

Modifications copyright (c) 2023-2026 Manaaki Whenua - Landcare Research; substantially extended from the original (geometry validity handling, recursion depth cap, cut-line recording, and cut-edge densification).
"""

from bisect import bisect_left
from math import ceil, floor

from shapely import force_2d, has_m, has_z
from shapely.geometry import (
    GeometryCollection,
    LinearRing,
    LineString,
    MultiLineString,
    MultiPolygon,
    Polygon,
    box,
)
from shapely.geometry.base import BaseGeometry
from shapely.validation import make_valid


def katana(
    geometry: BaseGeometry | None,
    threshold: float,
    count: int = 0,
    max_recursion_depth: int = 250,
    check_2D: bool = True,
    cuts: list[tuple[bool, float]] | None = None,
) -> list[BaseGeometry]:
    """
    Recursively split a geometry into two parts across its shortest dimension.
    Invalid input `geometry` will silently be made valid (if possible).
    Any LinearRings will be converted to Polygons.
    `threshold`: maximum acceptable area of the bounding box for any output geometry.
    `count`: used to track recursion depth
    `cuts`: when given, every cut line is appended as (is_vertical, coordinate); see densify_cut_edges.
    """

    if (geometry is None) or (geometry.is_empty):
        return []
    if isinstance(geometry, LinearRing):
        geometry = Polygon(geometry)
    if check_2D and (has_z(geometry) or has_m(geometry)):
        geometry = force_2d(geometry)
        check_2D = False  # No further 2D check needed
    if not geometry.is_valid:
        geometry = make_valid(geometry)
        if geometry.geom_type == "GeometryCollection":
            geometry.normalize()
        geometry = geometry.buffer(0)
    bounds = geometry.bounds
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]
    if ((width * height) <= threshold) or (count >= max_recursion_depth):
        return [geometry]
    if height >= width:
        # split left to right
        cut = bounds[1] + height / 2
        a = box(bounds[0], bounds[1], bounds[2], cut)
        b = box(bounds[0], cut, bounds[2], bounds[3])
        if cuts is not None:
            cuts.append((False, cut))
    else:
        # split top to bottom
        cut = bounds[0] + width / 2
        a = box(bounds[0], bounds[1], bounds[0] + width / 2, bounds[3])
        b = box(bounds[0] + width / 2, bounds[1], bounds[2], bounds[3])
        if cuts is not None:
            cuts.append((True, cut))
    result = []
    for d in (
        a,
        b,
    ):
        c = geometry.intersection(d)
        if not isinstance(c, GeometryCollection):
            c = GeometryCollection([c])
        for e in c.geoms:
            if isinstance(
                e, (Polygon, MultiPolygon, LineString, MultiLineString, LinearRing)
            ):
                result.extend(
                    katana(
                        e,
                        threshold,
                        count + 1,
                        max_recursion_depth=max_recursion_depth,
                        check_2D=check_2D,
                        cuts=cuts,
                    )
                )

    return result


def densify_cut_edges(
    geometry: BaseGeometry,
    cuts: list[tuple[bool, float]],
    max_segment: float,
) -> BaseGeometry:
    """
    Insert vertices at absolute multiples of `max_segment` along boundary segments that lie on recorded cut lines, leaving all other segments untouched.
    Adjacent pieces of any cut then share those grid vertices, so backends that reinterpret edges geodesically can only diverge across a cut by the sagitta of one `max_segment` chord (sub-mm for sensible eps).
    Without this, pieces cut at different recursion depths carry differently-spaced vertices along the same line, and the diverging curves lose cells whose centres fall between them.
    """
    if not cuts:
        return geometry
    v_cuts = sorted({c for vertical, c in cuts if vertical})
    h_cuts = sorted({c for vertical, c in cuts if not vertical})
    tol = max_segment * 1e-3

    def on_cut(lines: list[float], value: float) -> bool:
        i = bisect_left(lines, value)
        return any(
            0 <= j < len(lines) and abs(lines[j] - value) <= tol for j in (i - 1, i)
        )

    def densify(coords) -> list[tuple[float, float]]:
        out = [coords[0]]
        for (x0, y0), (x1, y1) in zip(coords, coords[1:], strict=False):
            along_x = y0 == y1 and on_cut(h_cuts, y0)
            along_y = x0 == x1 and on_cut(v_cuts, x0)
            if along_x or along_y:
                c0, c1 = (x0, x1) if along_x else (y0, y1)
                lo, hi = min(c0, c1), max(c0, c1)
                grid = [
                    k * max_segment
                    for k in range(floor(lo / max_segment) + 1, ceil(hi / max_segment))
                    if lo < k * max_segment < hi
                ]
                if c0 > c1:
                    grid.reverse()
                out.extend((g, y0) if along_x else (x0, g) for g in grid)
            out.append((x1, y1))
        return out

    def apply(geom: BaseGeometry) -> BaseGeometry:
        if isinstance(geom, Polygon):
            return Polygon(
                densify(list(geom.exterior.coords)),
                [densify(list(ring.coords)) for ring in geom.interiors],
            )
        if isinstance(geom, LineString):
            return LineString(densify(list(geom.coords)))
        if isinstance(geom, (MultiPolygon, MultiLineString, GeometryCollection)):
            return type(geom)([apply(g) for g in geom.geoms])
        return geom

    return apply(geometry)
