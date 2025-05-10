"""
Copyright (c) 2016, Joshua Arnott

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS “AS IS” AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

from shapely import force_2d, has_z, has_m
from shapely.geometry import (
    box,
    Polygon,
    MultiPolygon,
    LineString,
    MultiLineString,
    GeometryCollection,
    LinearRing,
)
from shapely.geometry.base import BaseGeometry
from shapely.validation import make_valid
from typing import Union, List


def katana(
    geometry: Union[BaseGeometry, None],
    threshold: float,
    count: int = 0,
    check_2D: bool = True,
) -> List[BaseGeometry]:
    """
    Recursively split a geometry into two parts across its shortest dimension.
    Invalid input `geometry` will silently be made valid (if possible).
    Any LinearRings will be converted to Polygons.
    """
    if geometry is None:
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
    if max(width, height) <= threshold or count == 250:
        # either the polygon is smaller than the threshold, or the maximum
        # number of recursions has been reached
        return [geometry]
    if height >= width:
        # split left to right
        a = box(bounds[0], bounds[1], bounds[2], bounds[1] + height / 2)
        b = box(bounds[0], bounds[1] + height / 2, bounds[2], bounds[3])
    else:
        # split top to bottom
        a = box(bounds[0], bounds[1], bounds[0] + width / 2, bounds[3])
        b = box(bounds[0] + width / 2, bounds[1], bounds[2], bounds[3])
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
                result.extend(katana(e, threshold, count + 1, check_2D))

    return result
