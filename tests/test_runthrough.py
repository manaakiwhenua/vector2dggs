# Aggregates the test classes under tests/classes/ for pytest discovery.
# Imports are optional: a module whose backend extra is not installed is
# omitted rather than failing collection (see also skip_unless_backend).
# ruff: noqa: E402
import contextlib

with contextlib.suppress(ImportError):
    from .classes.antimeridian import TestAntimeridian as TestAntimeridian
with contextlib.suppress(ImportError):
    from .classes.bisection import TestBisection as TestBisection
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
    from .classes.compaction_pipeline import (
        TestCompactionAcrossPartitions as TestCompactionAcrossPartitions,
    )
with contextlib.suppress(ImportError):
    from .classes.errors import TestErrors as TestErrors
with contextlib.suppress(ImportError):
    from .classes.errors import TestOverwriteRequired as TestOverwriteRequired
with contextlib.suppress(ImportError):
    from .classes.katana import TestKatana as TestKatana
with contextlib.suppress(ImportError):
    from .classes.linetrace import (
        TestLinetraceSetSemantics as TestLinetraceSetSemantics,
    )
with contextlib.suppress(ImportError):
    from .classes.output_validation import TestOutputValidation as TestOutputValidation
with contextlib.suppress(ImportError):
    from .classes.postgis import TestPostGIS as TestPostGIS
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
