# Aggregates the test classes under tests/classes/ for pytest discovery.
# Imports are optional: a module whose backend extra is not installed is
# omitted rather than failing collection (see also skip_unless_backend).
# ruff: noqa: E402
import contextlib
import importlib
import pathlib
import unittest

with contextlib.suppress(ImportError):
    from .classes.antimeridian import TestAntimeridian as TestAntimeridian
with contextlib.suppress(ImportError):
    from .classes.antimeridian import (
        TestUnwrappedLongitudes as TestUnwrappedLongitudes,
    )
with contextlib.suppress(ImportError):
    from .classes.bisection import TestBisection as TestBisection
with contextlib.suppress(ImportError):
    from .classes.bisection import (
        TestBisectionPreparation as TestBisectionPreparation,
    )
with contextlib.suppress(ImportError):
    from .classes.bisection import TestDroppedFeatureReport as TestDroppedFeatureReport
with contextlib.suppress(ImportError):
    from .classes.bisection import TestGeodesicCutEdges as TestGeodesicCutEdges
with contextlib.suppress(ImportError):
    from .classes.compaction import TestA5CompactionBounds as TestA5CompactionBounds
with contextlib.suppress(ImportError):
    from .classes.compaction import (
        TestGeohashCompactionBounds as TestGeohashCompactionBounds,
    )
with contextlib.suppress(ImportError):
    from .classes.compaction import TestH3CompactionBounds as TestH3CompactionBounds
with contextlib.suppress(ImportError):
    from .classes.compaction import TestRHPCompactionBounds as TestRHPCompactionBounds
with contextlib.suppress(ImportError):
    from .classes.compaction import TestS2CompactionBounds as TestS2CompactionBounds
with contextlib.suppress(ImportError):
    from .classes.common_units import TestAvailableMemoryMb as TestAvailableMemoryMb
with contextlib.suppress(ImportError):
    from .classes.common_units import TestCommitOutput as TestCommitOutput
with contextlib.suppress(ImportError):
    from .classes.common_units import (
        TestDictionaryEncodeAttributes as TestDictionaryEncodeAttributes,
    )
with contextlib.suppress(ImportError):
    from .classes.common_units import TestDefaultThreads as TestDefaultThreads
with contextlib.suppress(ImportError):
    from .classes.common_units import TestDropCondition as TestDropCondition
with contextlib.suppress(ImportError):
    from .classes.common_units import TestGetParentRes as TestGetParentRes
with contextlib.suppress(ImportError):
    from .classes.common_units import TestStagedFileChunks as TestStagedFileChunks
with contextlib.suppress(ImportError):
    from .classes.common_units import (
        TestWritePartitionGuards as TestWritePartitionGuards,
    )
with contextlib.suppress(ImportError):
    from .classes.compaction_pipeline import (
        TestCompactionAcrossPartitions as TestCompactionAcrossPartitions,
    )
with contextlib.suppress(ImportError):
    from .classes.compaction_pipeline import (
        TestCompactionEmptyPartition as TestCompactionEmptyPartition,
    )
with contextlib.suppress(ImportError):
    from .classes.errors import TestErrors as TestErrors
with contextlib.suppress(ImportError):
    from .classes.errors import (
        TestIndexCompactionDefaults as TestIndexCompactionDefaults,
    )
with contextlib.suppress(ImportError):
    from .classes.errors import TestOverwriteRequired as TestOverwriteRequired
with contextlib.suppress(ImportError):
    from .classes.errors import TestStagedOutput as TestStagedOutput
with contextlib.suppress(ImportError):
    from .classes.ingest import TestBatchedIngest as TestBatchedIngest
with contextlib.suppress(ImportError):
    from .classes.input_dispatch import TestInputDispatch as TestInputDispatch
with contextlib.suppress(ImportError):
    from .classes.katana import TestKatana as TestKatana
with contextlib.suppress(ImportError):
    from .classes.linetrace import TestEmptyTraces as TestEmptyTraces
with contextlib.suppress(ImportError):
    from .classes.linetrace import (
        TestLinetraceSetSemantics as TestLinetraceSetSemantics,
    )
with contextlib.suppress(ImportError):
    from .classes.output_validation import TestOutputValidation as TestOutputValidation
with contextlib.suppress(ImportError):
    from .classes.polyfill_contract import (
        TestPolyfillTokenContract as TestPolyfillTokenContract,
    )
with contextlib.suppress(ImportError):
    from .classes.postgis import TestPostGIS as TestPostGIS
with contextlib.suppress(ImportError):
    from .classes.readme_help import TestReadmeHelp as TestReadmeHelp
with contextlib.suppress(ImportError):
    from .classes.write_limits import TestProcessPoolContext as TestProcessPoolContext
with contextlib.suppress(ImportError):
    from .classes.write_limits import TestRaiseRlimitNofile as TestRaiseRlimitNofile
with contextlib.suppress(ImportError):
    from .classes.write_limits import TestSortedHiveWrite as TestSortedHiveWrite
with contextlib.suppress(ImportError):
    from .classes.runthrough import TestA5 as TestA5
    from .classes.runthrough import TestGeohash as TestGeohash
    from .classes.runthrough import TestH3 as TestH3
    from .classes.runthrough import TestRHP as TestRHP
    from .classes.runthrough import TestS2 as TestS2


class TestAggregatorCompleteness(unittest.TestCase):
    """A test class that isn't imported above is silently never collected."""

    def test_every_test_class_is_aggregated(self):
        classes_dir = pathlib.Path(__file__).parent / "classes"
        missing = []
        for mod_file in sorted(classes_dir.glob("*.py")):
            if mod_file.name == "__init__.py":
                continue
            name = f"{__package__}.classes.{mod_file.stem}"
            try:
                mod = importlib.import_module(name)
            except ImportError:  # optional backend not installed
                continue
            for attr, val in vars(mod).items():
                if (
                    isinstance(val, type)
                    and issubclass(val, unittest.TestCase)
                    and val.__module__ == name
                    and any(m.startswith("test") for m in vars(val))
                    and attr not in globals()
                ):
                    missing.append(f"{mod_file.name}: {attr}")
        self.assertFalse(missing, f"not aggregated: {missing}")
