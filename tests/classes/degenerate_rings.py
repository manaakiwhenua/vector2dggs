import tempfile
import warnings
from pathlib import Path
from unittest import TestCase

import geopandas as gpd
import pandas as pd
import shapely
from shapely.geometry import Polygon

from vector2dggs import common
from vector2dggs import constants as const
from vector2dggs.indexerfactory import indexer_instance
from vector2dggs.indexers.vectorindexer import InvalidGeometryError
from vector2dggs.spikes import drop_spikes

from .base import skip_unless_backend

# The two features from #224 (MfE Irrigated Land Area 2020, layer 105407),
# in lon/lat as reprojected from EPSG:2193.

# Near-duplicate vertices (1e-14 degrees apart): self-intersecting for
# shapely, an invalid loop for S2. 1.89 ha.
FEATURE_A = (
    "POLYGON ((171.10341308314696 -44.68334623209624, 171.10349950242062 "
    "-44.68328738413344, 171.10358659245873 -44.68322807921747, 171.10369093904197 "
    "-44.68312686123868, 171.10369093904197 -44.68312686123867, 171.1032049608305 "
    "-44.68390701840317, 171.1019346718429 -44.683820720459366, 171.1008234810407 "
    "-44.68427158115471, 171.09976471100833 -44.68423658062024, 171.09977736535004 "
    "-44.68235943118847, 171.099934931097 -44.68236415888309, 171.099934931097 "
    "-44.682364158883104, 171.09994411168555 -44.68246645054625, 171.09995975083652 "
    "-44.682512347070244, 171.10003033634968 -44.68271949487789, 171.1001768423985 "
    "-44.68295801265988, 171.10037917890665 -44.68317475644066, 171.10063119832543 "
    "-44.68336314030071, 171.10092524323193 -44.68351743999458, 171.1013285068364 "
    "-44.68362933637548, 171.10132850683647 -44.683629336375496, 171.1014754902656 "
    "-44.68367055074125, 171.1016026652446 -44.68370621059051, 171.10168052175106 "
    "-44.68371237726754, 171.1019654580308 -44.6837349453747, 171.1023054697985 "
    "-44.683719406966524, 171.1023297332529 -44.68371829809681, 171.1024193122104 "
    "-44.68370276004238, 171.1026844217531 -44.683656774599704, 171.10301874574213 "
    "-44.68355224436773, 171.10322808709452 -44.68345276924278, 171.103322546368 "
    "-44.68340788370867, 171.10341308314688 -44.68334623209629, 171.1034130831469 "
    "-44.683346232096284, 171.10341308314696 -44.68334623209624))"
)

# A 0-degree spike: out 158 m to vertex 2 and back to vertex 3, 0.4 mm from
# the outbound edge - inside the great-circle bow of that edge, so valid for
# shapely but self-crossing for S2. 0.48 ha.
FEATURE_B = (
    "POLYGON ((176.8056215879939 -39.474460130064315, 176.80623994145597 "
    "-39.47583294383016, 176.80756578354925 -39.474849956062364, 176.80700018756875 "
    "-39.47526929757364, 176.80627638092693 -39.47607249240748, 176.80615390931263 "
    "-39.47602581248104, 176.80540064947314 -39.474808486146856, 176.8056215879939 "
    "-39.474460130064315))"
)

# Coarse enough that covering the whole sphere is cheap (6 * 4**5 cells),
# should a regression ever let an invalid polygon through to the coverer:
# at a fine level that would exhaust memory rather than fail.
SAFE_S2_LEVEL = 5


def _inlet_polygon(tip_lat: float) -> Polygon:
    """
    A 2-degree-wide box whose east-west top edge bows ~0.004 degrees south
    as a great circle, with an 86 m inlet rising from the bottom edge to
    tip_lat. No spike: every angle is 90 degrees.
    """
    return Polygon(
        [
            (170, -40),
            (171, -40),
            (171, tip_lat),
            (171.001, tip_lat),
            (171.001, -40),
            (172, -40),
            (172, -39),
            (170, -39),
        ]
    )


class TestS2DegenerateRings(TestCase):
    def setUp(self):
        skip_unless_backend("s2")
        self.indexer = indexer_instance("s2")

    def test_near_duplicate_vertices_are_merged(self):
        cells = self.indexer.cells_from_polygon(shapely.from_wkt(FEATURE_A), 18)
        # 1.89 ha over ~0.12 ha level-18 cells
        self.assertTrue(10 <= len(cells) <= 30, len(cells))

    def test_spike_is_refused_not_covered(self):
        with self.assertRaises(InvalidGeometryError):
            self.indexer.cells_from_polygon(shapely.from_wkt(FEATURE_B), SAFE_S2_LEVEL)

    def test_narrow_inlet_within_edge_bow_is_refused(self):
        # valid for shapely, and not a spike, but the inlet's tip lies
        # north of the top edge's great-circle arc
        inlet = _inlet_polygon(-39.002)
        self.assertTrue(inlet.is_valid)
        with self.assertRaises(InvalidGeometryError):
            self.indexer.cells_from_polygon(inlet, SAFE_S2_LEVEL)
        # the same inlet stopping short of the arc is fine
        self.indexer.cells_from_polygon(_inlet_polygon(-39.006), SAFE_S2_LEVEL)

    def test_refused_polygon_is_skipped_with_a_warning_naming_it(self):
        df = gpd.GeoDataFrame(
            {
                "feature": ["a", "b"],
                "geometry": [shapely.from_wkt(FEATURE_A), _inlet_polygon(-39.002)],
            },
            crs=4326,
        )
        with self.assertLogs(common.LOGGER, level="WARNING") as logs:
            result = self.indexer.polyfill(df, SAFE_S2_LEVEL + 13, id_col="feature")
        self.assertEqual(set(result["feature"]), {"a"})
        self.assertTrue(
            any("Feature b:" in m and "not indexed" in m for m in logs.output),
            logs.output,
        )

    def test_spike_dropped_polygon_is_indexed(self):
        cleaned, removed = drop_spikes(
            shapely.from_wkt(FEATURE_B), 0.01, True, const.METRES_PER_DEGREE
        )
        self.assertEqual(removed, 1)
        cells = self.indexer.cells_from_polygon(cleaned, 18)
        self.assertTrue(2 <= len(cells) <= 10, len(cells))


class TestRHPInvalidGeometry(TestCase):
    def setUp(self):
        skip_unless_backend("rhp")
        self.indexer = indexer_instance("rhp")

    def test_refused_polygon_is_skipped_with_a_warning_naming_it(self):
        df = gpd.GeoDataFrame(
            {"feature": ["a"], "geometry": [shapely.from_wkt(FEATURE_A)]}, crs=4326
        )
        with self.assertLogs(common.LOGGER, level="WARNING") as logs:
            result = self.indexer.polyfill(df, 13, id_col="feature")
        self.assertTrue(result.empty)
        self.assertTrue(
            any("Feature a:" in m and "Self-intersection" in m for m in logs.output),
            logs.output,
        )

    def test_near_duplicate_vertices_are_merged_by_cleaning(self):
        df = gpd.GeoDataFrame(
            {"feature": ["a"], "geometry": [shapely.from_wkt(FEATURE_A)]}, crs=4326
        )
        cleaned = common._clean_geometries(df, self.indexer)
        result = self.indexer.polyfill(cleaned, 13, id_col="feature")
        # 1.89 ha over ~33 m^2 cells
        self.assertTrue(500 <= len(result) <= 650, len(result))


class TestSubToleranceGeometry(TestCase):
    """
    A polygon smaller than the repeated-point tolerance collapses when its
    near-duplicates are merged, which GEOS refuses: it must be kept as it
    is (and index to nothing) rather than fail the batch it is in.
    """

    TINY = Polygon(
        [(170, -40), (170 + 1e-10, -40), (170 + 1e-10, -40 + 1e-10), (170, -40 + 1e-10)]
    )

    def test_cleaning_keeps_it(self):
        df = gpd.GeoDataFrame(
            {
                "feature": ["a", "tiny"],
                "geometry": [shapely.from_wkt(FEATURE_A), self.TINY],
            },
            crs=4326,
        )
        cleaned = common._clean_geometries(df, indexer_instance("h3"))
        self.assertEqual(len(cleaned), 2)
        self.assertTrue(cleaned.geometry.iloc[1].equals(self.TINY))
        self.assertTrue(cleaned.geometry.iloc[0].is_valid)

    def test_s2_refuses_it(self):
        skip_unless_backend("s2")
        with self.assertRaises(InvalidGeometryError):
            indexer_instance("s2").cells_from_polygon(self.TINY, 18)


class TestDropSpikes(TestCase):
    def _b_projected(self):
        return gpd.GeoSeries([shapely.from_wkt(FEATURE_B)], crs=4326).to_crs(2193)[0]

    def test_spike_tip_removed_in_either_crs(self):
        b = shapely.from_wkt(FEATURE_B)
        for geom, geographic, mpu in (
            (b, True, const.METRES_PER_DEGREE),
            (self._b_projected(), False, 1.0),
        ):
            cleaned, removed = drop_spikes(geom, 0.01, geographic, mpu)
            self.assertEqual(removed, 1)
            self.assertEqual(
                len(cleaned.exterior.coords), len(geom.exterior.coords) - 1
            )
        # area lost is the 0.03 m^2 sliver
        cleaned, _ = drop_spikes(self._b_projected(), 0.01, False, 1.0)
        self.assertAlmostEqual(cleaned.area, self._b_projected().area, delta=0.1)

    def test_wider_than_tolerance_is_kept(self):
        # in lon/lat the sliver is 0.4 mm wide (in EPSG:2193, its source,
        # exactly 0: the spike returns along the outbound edge itself)
        b = shapely.from_wkt(FEATURE_B)
        _, removed = drop_spikes(b, 0.0001, True, const.METRES_PER_DEGREE)
        self.assertEqual(removed, 0)
        _, removed = drop_spikes(b, 0.001, True, const.METRES_PER_DEGREE)
        self.assertEqual(removed, 1)

    def test_tapering_multi_vertex_spike_removed_entirely(self):
        # a 100 m spike off the top of a 100 m square, 1 m wide at its base
        # and narrowing to its tip, with a vertex part-way along each side:
        # each removal exposes the next acute tip
        square = Polygon([(0, 0), (100, 0), (100, 100), (0, 100)])
        spiked = Polygon(
            [
                (0, 0),
                (100, 0),
                (100, 100),
                (51, 100),
                (50.8, 140),
                (50.5, 200),
                (50.3, 160),
                (50, 100),
                (0, 100),
            ]
        )
        cleaned, removed = drop_spikes(spiked, 2.0, False, 1.0)
        self.assertEqual(removed, 3)
        self.assertTrue(cleaned.equals(square))

    def test_parallel_sided_finger_keeps_its_body(self):
        # not a spike: past its tip, a 1 m finger of constant width meets
        # its sides at right angles, so only the tip goes
        fingered = Polygon(
            [
                (0, 0),
                (100, 0),
                (100, 100),
                (51, 100),
                (51, 150),
                (50.5, 200),
                (50, 150),
                (50, 100),
                (0, 100),
            ]
        )
        cleaned, removed = drop_spikes(fingered, 2.0, False, 1.0)
        self.assertEqual(removed, 1)
        self.assertAlmostEqual(cleaned.area, 100 * 100 + 50, delta=1e-6)

    def test_ordinary_corners_and_inlets_untouched(self):
        inlet = _inlet_polygon(-39.002)
        cleaned, removed = drop_spikes(inlet, 10.0, True, const.METRES_PER_DEGREE)
        self.assertEqual(removed, 0)
        self.assertIs(cleaned, inlet)


class TestDegenerateRingsPipeline(TestCase):
    """
    #224 end to end: from EPSG:2193, as reported. Without --drop-spikes the
    spiked feature is skipped with a warning (rather than exhausting
    memory); with it, both are indexed.
    """

    def setUp(self):
        skip_unless_backend("s2")
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        df = gpd.GeoDataFrame(
            {
                "id": [1, 2],
                "geometry": [shapely.from_wkt(FEATURE_A), shapely.from_wkt(FEATURE_B)],
            },
            crs=4326,
        ).to_crs(2193)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df.to_file(self.dir / "in.gpkg", layer="x")

    def _index(self, **kwargs) -> pd.DataFrame:
        out = self.dir / "out"
        common.index(
            "s2",
            str(self.dir / "in.gpkg"),
            str(out),
            18,
            None,
            False,
            1,
            id_field="id",
            layer="x",
            overwrite=True,
            **kwargs,
        )
        return pd.read_parquet(out)

    def test_spiked_feature_skipped_by_default(self):
        with self.assertLogs(common.LOGGER, level="WARNING") as logs:
            result = self._index()
        self.assertEqual(set(result["id"]), {1})
        # the per-feature warning comes from a pool worker, out of reach of
        # assertLogs; the run's summary names the omission
        self.assertTrue(
            any("1 of 2 features" in m and "invalid" in m for m in logs.output),
            logs.output,
        )

    def test_drop_spikes_indexes_both(self):
        result = self._index(drop_spikes=0.01)
        self.assertEqual(set(result["id"]), {1, 2})
