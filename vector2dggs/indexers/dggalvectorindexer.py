from collections.abc import Iterable, Sequence
from functools import cache
from typing import Any, cast

import dggal
import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import shapely
from shapely.geometry import LineString, MultiPolygon, Point, Polygon

import vector2dggs.constants as const
from vector2dggs.indexers.vectorindexer import VectorIndexer

# One-off library setup per process; pool workers import this module afresh.
dggal.pydggal_setup(dggal.Application(appGlobals=globals()))

_NULL_ZONE = 2**64 - 1


@cache
def _dggrs(class_name: str) -> Any:
    """One grid object per process: cffi handles neither pickle nor fork."""
    return getattr(dggal, class_name)()


class DGGALVectorIndexer(VectorIndexer[str | int]):
    """
    Integration for the nested (single-parent) grids of DGGAL: the rhombic
    ISEA/IVEA/RTEA 4R and 9R grids and HEALPix. Zones are worked as uint64
    throughout; the structured text ID is produced at the output boundary.

    DGGAL has no polygon fill or line trace. Polygons are filled from
    listZones over the bounding box filtered on zone centroids, and lines by
    the base class's flood-fill cover.
    """

    DGGRS_CLASS: str
    CODESPACE: str | None = None
    GEODESIC_POLYFILL = False
    CELL_ARROW_TYPE: pa.DataType = pa.uint64()
    LIBRARIES = ("dggal",)

    @property
    def dggrs(self) -> Any:
        return _dggrs(self.DGGRS_CLASS)

    def _zone(self, cell: str | int) -> int:
        """Native int, or a text ID read back from the pipeline's own output."""
        if isinstance(cell, str):
            return int(self.dggrs.getZoneFromTextID(cell))
        return int(cell)

    # -- identity and sizing -------------------------------------------------

    def cells_to_string(self, cells: Iterable[str | int]) -> list[str]:
        d = self.dggrs
        return [d.getZoneTextID(self._zone(c)) for c in cells]

    def grid_metadata(self) -> dict[str, Any]:
        return {"codespace": self.CODESPACE, "grid": self.DGGRS_CLASS}

    def cell_area_m2(self, resolution: int) -> float:
        # equal-area grids: the authalic sphere over the zone count
        return float(
            4
            * np.pi
            * const.EARTH_AUTHALIC_RADIUS_M**2
            / self.dggrs.countZones(resolution)
        )

    @property
    def max_res(self) -> int:
        return int(self.dggrs.getMaxDGGRSZoneLevel())

    # -- points, neighbours, outlines --------------------------------------

    def _cell_at(self, lon: float, lat: float, resolution: int) -> int:
        return int(
            self.dggrs.getZoneFromWGS84Centroid(resolution, dggal.GeoPoint(lat, lon))
        )

    def _neighbours(self, cell: str | int) -> list[int]:
        """
        getZoneNeighbors gives edge neighbours only, four on these square
        grids; the second ring supplies the vertex-only ones _cover_segment
        needs.
        """
        d = self.dggrs
        zone = self._zone(cell)
        ring = {int(n) for n in d.getZoneNeighbors(zone)}
        second = {int(n) for z in ring for n in d.getZoneNeighbors(z)}
        return list((ring | second) - {zone})

    def _centroids(self, cells: Iterable[str | int]) -> np.ndarray:
        d = self.dggrs
        points = [d.getZoneWGS84Centroid(self._zone(c)) for c in cells]
        return np.array([(float(p.lon), float(p.lat)) for p in points], dtype=float)

    def _rings(self, cells: Iterable[str | int], refined: bool) -> list[np.ndarray]:
        d = self.dggrs
        return [
            np.array(
                [
                    (float(p.lon), float(p.lat))
                    for p in (
                        d.getZoneRefinedWGS84Vertices(z, 0)
                        if refined
                        else d.getZoneWGS84Vertices(z)
                    )
                ]
            )
            for z in (self._zone(c) for c in cells)
        ]

    def cell_to_point(self, cell: str | int) -> Point:
        lon, lat = self._centroids([cell])[0]
        return Point(lon, lat)

    def cells_to_points(self, cells: Iterable[str | int]) -> Iterable[Point]:
        return cast(Iterable[Point], shapely.points(self._centroids(cells)))

    def cell_to_polygon(self, cell: str | int) -> Polygon:
        return Polygon(self._rings([cell], refined=False)[0])

    def cells_to_polygons(self, cells: Iterable[str | int]) -> Iterable[Polygon]:
        # corners only, joined by straight edges (see the README)
        rings = self._rings(cells, refined=False)
        return [Polygon(r) for r in rings]

    def _cells_to_test_polygons(self, cells: Iterable[str | int]) -> Iterable[Any]:
        """
        Refined outlines for the containment tests. A zone straddling the
        antimeridian comes back unwrapped (longitudes past ±180), so it is
        tested as itself and its 360° shift, whichever side the geometry
        under test is on.
        """
        polygons = []
        for ring in self._rings(cells, refined=True):
            lons = ring[:, 0]
            parts = [ring]
            if lons.max() > 180:
                parts.append(ring - (360, 0))
            if lons.min() < -180:
                parts.append(ring + (360, 0))
            polygons.append(
                Polygon(parts[0])
                if len(parts) == 1
                else MultiPolygon([Polygon(p) for p in parts])
            )
        return polygons

    # -- filling -----------------------------------------------------------

    def _bbox_zones(self, geom: Polygon | LineString, resolution: int) -> list[int]:
        minx, miny, maxx, maxy = geom.bounds
        extent = dggal.GeoExtent(
            ll=dggal.GeoPoint(max(miny, -90.0), max(minx, -180.0)),
            ur=dggal.GeoPoint(min(maxy, 90.0), min(maxx, 180.0)),
        )
        return [int(z) for z in self.dggrs.listZones(resolution, extent)]

    def _centre_fill(self, geom: Polygon, resolution: int) -> list[int]:
        zones = self._bbox_zones(geom, resolution)
        if not zones:
            return []
        centroids = self._centroids(zones)
        shapely.prepare(geom)
        inside = shapely.contains_xy(geom, centroids[:, 0], centroids[:, 1])
        return [z for z, hit in zip(zones, inside, strict=True) if hit]

    def _polyfill_polygon(self, geom: Polygon, resolution: int) -> list[int]:
        # A cell meeting the polygon either holds its centre inside it or is
        # crossed by its boundary, so the boundary's cover turns the centre
        # fill into intersects (union) or within (difference). The pipeline
        # composes within the same way from intersects; this serves direct
        # callers.
        cells = set(self._centre_fill(geom, resolution))
        if self.mode != const.ContainmentMode.CENTRE:
            boundary: set[int] = set()
            for ring in (geom.exterior, *geom.interiors):
                boundary |= {
                    int(c)
                    for c in self._cover_line(LineString(ring.coords), resolution)
                }
            if self.mode == const.ContainmentMode.INTERSECTS:
                cells |= boundary
            else:
                cells -= boundary
        return list(cells)

    def _linetrace(self, geom: LineString, resolution: int) -> list[int]:
        return [int(c) for c in self._cover_line(geom, resolution)]

    def _polyfill_polygons(self, df: gpd.GeoDataFrame, resolution: int) -> pd.DataFrame:
        return self._geo_to_cells(
            df, resolution, self._polyfill_polygon, df.geometry.name
        )

    def _polyfill_linestrings(
        self, df: gpd.GeoDataFrame, resolution: int
    ) -> pd.DataFrame:
        return self._geo_to_cells(df, resolution, self._linetrace, df.geometry.name)

    def _polyfill_points(self, df: gpd.GeoDataFrame, resolution: int) -> pd.DataFrame:
        return self._geo_to_cells(
            df,
            resolution,
            lambda geom, res: [self._cell_at(geom.x, geom.y, res)],
            df.geometry.name,
        )

    # -- hierarchy ---------------------------------------------------------

    def _ancestor(self, zone: int, parent_res: int) -> int:
        # a nested zone's ancestor holds its centroid: one point lookup,
        # rather than a walk up the parent chain
        d = self.dggrs
        return int(d.getZoneFromWGS84Centroid(parent_res, d.getZoneWGS84Centroid(zone)))

    def secondary_index(self, df: pd.DataFrame, parent_res: int) -> pd.DataFrame:
        parents = {z: self._ancestor(int(z), parent_res) for z in df.index.unique()}
        df[f"{self.dggs}_{parent_res:02}"] = df.index.map(parents).astype("uint64")
        return df

    def get_resolution(self, cell: str | int) -> int:
        return int(self.dggrs.getZoneLevel(self._zone(cell)))

    def children_at_res(self, cell: str | int, target_res: int) -> list[int]:
        zone = self._zone(cell)
        depth = target_res - self.get_resolution(zone)
        if depth <= 0:
            return [zone]
        return [int(z) for z in self.dggrs.getSubZones(zone, depth)]

    def compaction(
        self,
        df: pd.DataFrame,
        res: int,
        col_order: list[str],
        dggs_col: str,
        id_field: str | Sequence[str],
        parent_res: int,
    ) -> pd.DataFrame:
        d = self.dggrs

        def _compact(cells: Iterable[str | int]) -> list[int]:
            # compactZones edits its Array in place: complete sibling sets
            # become their parent, repeated upward
            zones = dggal.Array("<DGGRSZone>")
            for c in cells:
                zones.add(self._zone(c))
            d.compactZones(zones)
            return [int(z) for z in zones]

        def _child(cell: str | int, target: int) -> int:
            return self.children_at_res(cell, target)[0]

        return self.compaction_common(
            df,
            res,
            id_field,
            col_order,
            dggs_col,
            _compact,
            _child,
            parent_res,
            self.get_resolution,
            self.children_at_res,
        )


class ISEA4RVectorIndexer(DGGALVectorIndexer):
    """ISEA projection, rhombic zones, refinement ratio 4."""

    DGGRS_CLASS = "ISEA4R"


class IVEA4RVectorIndexer(DGGALVectorIndexer):
    """IVEA projection, rhombic zones, refinement ratio 4."""

    DGGRS_CLASS = "IVEA4R"


class RTEA4RVectorIndexer(DGGALVectorIndexer):
    """RTEA projection, rhombic zones, refinement ratio 4."""

    DGGRS_CLASS = "RTEA4R"


class ISEA9RVectorIndexer(DGGALVectorIndexer):
    """ISEA projection, rhombic zones, refinement ratio 9."""

    DGGRS_CLASS = "ISEA9R"


class IVEA9RVectorIndexer(DGGALVectorIndexer):
    """IVEA projection, rhombic zones, refinement ratio 9."""

    DGGRS_CLASS = "IVEA9R"


class RTEA9RVectorIndexer(DGGALVectorIndexer):
    """RTEA projection, rhombic zones, refinement ratio 9."""

    DGGRS_CLASS = "RTEA9R"


class HEALPixVectorIndexer(DGGALVectorIndexer):
    """HEALPix, with DGGAL's hierarchical text identifiers."""

    DGGRS_CLASS = "HEALPix"


class HEALPixNUNIQVectorIndexer(DGGALVectorIndexer):
    """HEALPix with NUNIQ identifiers: the same zones as HEALPix, named differently."""

    DGGRS_CLASS = "HEALPix_NUNIQ"
