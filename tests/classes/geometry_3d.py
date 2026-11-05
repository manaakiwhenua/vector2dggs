import importlib
from unittest import TestCase

import geopandas as gpd
import pyarrow.parquet as pq
import shapely
from shapely.geometry import LineString, Point, Polygon

import vector2dggs.constants as const
from vector2dggs.common import _force_2d

from .base import TestRunthrough, skip_unless_backend

BACKENDS = ("h3", "rhp", "s2", "a5", "geohash")
RES = {"h3": 8, "s2": 13, "a5": 15, "rhp": 8, "geohash": 6}
MODULES = {"rhp": "rHP"}  # CLI module names that differ from the DGGS key


def _features_3d() -> gpd.GeoDataFrame:
    """One of each supported geometry type, every vertex carrying Z."""
    return gpd.GeoDataFrame(
        {
            "fid": [1, 2, 3],
            "geometry": [
                Polygon(
                    [
                        (174.70, -41.30, 10),
                        (174.75, -41.30, 10),
                        (174.75, -41.25, 10),
                        (174.70, -41.25, 10),
                    ]
                ),
                LineString([(174.70, -41.30, 5), (174.75, -41.25, 5)]),
                Point(174.72, -41.27, 3),
            ],
        },
        crs=4326,
    )


class TestForce2D(TestCase):
    """_force_2d, applied to every batch before bisection, drops Z and M (#231)."""

    def test_z_is_dropped(self):
        df = _features_3d().set_index("fid")
        self.assertTrue(df.geometry.has_z.all())
        out = _force_2d(df)
        self.assertEqual(len(out), 3)
        self.assertFalse(out.geometry.has_z.any())
        self.assertEqual(
            list(out.geometry.geom_type), ["Polygon", "LineString", "Point"]
        )

    def test_m_and_zm_are_dropped(self):
        df = gpd.GeoDataFrame(
            {
                "fid": [1, 2, 3, 4],
                "geometry": [
                    shapely.from_wkt("POINT M (174.72 -41.27 7)"),
                    shapely.from_wkt("POINT ZM (174.72 -41.27 3 7)"),
                    shapely.from_wkt(
                        "LINESTRING ZM (174.70 -41.30 5 1, 174.75 -41.25 5 2)"
                    ),
                    shapely.from_wkt(
                        "POLYGON ZM ((174.70 -41.30 10 1, 174.75 -41.30 10 2, "
                        "174.75 -41.25 10 3, 174.70 -41.25 10 4, 174.70 -41.30 10 1))"
                    ),
                ],
            },
            crs=4326,
        ).set_index("fid")
        self.assertTrue(shapely.has_m(df.geometry.values).all())
        out = _force_2d(df)
        self.assertEqual(len(out), 4)
        self.assertFalse(shapely.has_m(out.geometry.values).any())
        self.assertFalse(shapely.has_z(out.geometry.values).any())
        self.assertEqual(
            list(out.geometry.geom_type), ["Point", "Point", "LineString", "Polygon"]
        )


def _bisected_polygon_3d(dggs: str) -> Polygon:
    """A 3D square just over the default bisection threshold for dggs at RES."""
    side = 1.3 * const.DEFAULT_AREA_THRESHOLD_M2(dggs, RES[dggs]) ** 0.5 / 111_000
    lon, lat = 174.7, -41.3
    return Polygon(
        [
            (lon, lat, 10),
            (lon + side, lat, 10),
            (lon + side, lat + side, 10),
            (lon, lat + side, 10),
        ]
    )


class Test3DInput(TestRunthrough):
    """
    3D features index on every backend (#231), whether or not they are
    bisected: katana no longer flattens, _clean_geometries does, for all.
    """

    def _run(self, dggs: str, src, out) -> int:
        module = importlib.import_module(f"vector2dggs.{MODULES.get(dggs, dggs)}")
        getattr(module, dggs)(
            [str(src), str(out), "-r", str(RES[dggs]), "-t", "1"],
            standalone_mode=False,
        )
        files = sorted(out.rglob("*.parquet"))
        self.assertTrue(files, f"{dggs}: no parquet output written")
        return sum(pq.read_metadata(f).num_rows for f in files)

    @staticmethod
    def _installed():
        for dggs in BACKENDS:
            try:
                skip_unless_backend(dggs)
            except Exception:
                continue
            yield dggs

    def test_small_features(self):
        # below the bisection threshold, so katana never sees them
        src = self.output_path.parent / "features_3d.gpkg"
        _features_3d().to_file(src, driver="GPKG")
        for dggs in self._installed():
            with self.subTest(dggs=dggs):
                rows = self._run(dggs, src, self.output_path / dggs)
                self.assertGreaterEqual(rows, 3, f"{dggs}: {rows} rows")

    def test_bisected_polygon(self):
        for dggs in self._installed():
            with self.subTest(dggs=dggs):
                src = self.output_path.parent / f"big_3d_{dggs}.gpkg"
                gpd.GeoDataFrame(
                    {"fid": [1], "geometry": [_bisected_polygon_3d(dggs)]}, crs=4326
                ).to_file(src, driver="GPKG")
                rows = self._run(dggs, src, self.output_path / dggs)
                self.assertGreater(rows, const.DEFAULT_CUT_CELLS_PER_PIECE)
