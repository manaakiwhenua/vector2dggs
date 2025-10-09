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
    if (width * height) <= threshold or count == 250:
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
    # Add additional vertices to prevent indexing errors from reprojection to EPSG:4386 later along long edges 
    a, b = map(lambda g: g.segmentize(min(width, height)/2), [a, b])
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


if __name__ == "__main__":
    from functools import wraps
    from time import time

    from shapely import wkt

    from vector2dggs.common import timing

    def timing(f):
        @wraps(f)
        def wrap(*args, **kw):
            ts = time()
            result = f(*args, **kw)
            te = time()
            print(
                "func:%r args:[%r, %r] took: %2.4f sec"
                % (f.__name__, args, kw, te - ts)
            )
            return result

        return wrap

    polygon_a = wkt.loads(
        "POLYGON ((-62.490234 37.09024, -87.363281 30.524413, -86.484375 21.616579, "
        "-81.5625 15.029686, -64.248047 16.972741, -64.599609 25.562265, "
        "-70.576172 25.641526, -59.941406 31.052934, -52.470703 35.317366, "
        "-62.490234 37.09024))"
    )
    polygon_b = wkt.loads(
        "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))"
    )
    polygon_c = wkt.loads(
        "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))"
    )
    polygon_d = wkt.loads(
        "POLYGON ((-42.978516 31.952162, -65.478516 29.382175, -47.460937 26.509905, -72.509766 19.311143, -48.779297 20.632784, -47.109375 11.436955, -44.033203 21.207459, -34.980469 16.467695, -40.869141 23.483401, -27.333984 26.037042, -35.068359 28.304381, -43.242188 27.605671, -42.978516 31.952162))"
    )

    area_threshold = 0.5

    @timing
    def timed_test_execution(*args):
        for geom in [polygon_a, polygon_b, polygon_c, polygon_d]:
            collection = katana(geom, *args)
            print(GeometryCollection(collection))

    timed_test_execution(area_threshold)
