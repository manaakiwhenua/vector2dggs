from .classes.a5 import TestA5 as TestA5
from .classes.antimeridian import TestAntimeridian as TestAntimeridian
from .classes.bisection import TestBisection as TestBisection
from .classes.compaction import TestA5CompactionBounds as TestA5CompactionBounds
from .classes.compaction import (
    TestGeohashCompactionBounds as TestGeohashCompactionBounds,
)
from .classes.compaction import TestH3CompactionBounds as TestH3CompactionBounds
from .classes.compaction import TestRHPCompactionBounds as TestRHPCompactionBounds
from .classes.compaction import TestS2CompactionBounds as TestS2CompactionBounds
from .classes.compaction_pipeline import (
    TestCompactionAcrossPartitions as TestCompactionAcrossPartitions,
)
from .classes.errors import TestErrors as TestErrors
from .classes.errors import TestOverwriteRequired as TestOverwriteRequired
from .classes.geohash import TestGeohash as TestGeohash
from .classes.geometry_types import TestGeometryTypes as TestGeometryTypes
from .classes.h3 import TestH3 as TestH3
from .classes.katana import TestKatana as TestKatana
from .classes.linetrace import TestLinetraceSetSemantics as TestLinetraceSetSemantics
from .classes.output_validation import TestOutputValidation as TestOutputValidation
from .classes.postgis import TestPostGIS as TestPostGIS
from .classes.rHP import TestRHP as TestRHP
from .classes.s2 import TestS2 as TestS2
from .classes.write_limits import TestMaxOpenFilesPerTask as TestMaxOpenFilesPerTask
from .classes.write_limits import TestRaiseRlimitNofile as TestRaiseRlimitNofile
from .classes.write_limits import TestSortedHiveWrite as TestSortedHiveWrite
