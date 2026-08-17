import tempfile
from pathlib import Path
from unittest import SkipTest, TestCase

from vector2dggs.indexerfactory import indexer_instance


def skip_unless_backend(dggs: str) -> None:
    """Skip the calling test/class when the backend's extra isn't installed."""
    try:
        indexer_instance(dggs)
    except ImportError as e:
        raise SkipTest(str(e)) from e


class TestRunthrough(TestCase):
    """
    Parent class for tests that write DGGS output. Provides a per-test
    output path inside a temporary directory, so tests are isolated from
    each other and can run in parallel.
    """

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.output_path = Path(tmp.name) / "output"
