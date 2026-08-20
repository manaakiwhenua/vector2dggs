import re
from pathlib import Path
from unittest import TestCase

from click.testing import CliRunner

from vector2dggs.h3 import h3

README = Path(__file__).parents[2] / "README.md"


class TestReadmeHelp(TestCase):
    """The README's canonical --help block must match the real output, so it
    cannot silently drift as options change. To refresh it, paste the output
    of: python -c "from tests.classes.readme_help import render; print(render())"
    """

    def test_readme_help_block_matches_cli(self):
        match = re.search(
            r"```\n(Usage: vector2dggs h3 .*?)```", README.read_text(), re.DOTALL
        )
        self.assertIsNotNone(match, "README h3 --help block not found")
        self.assertEqual(match.group(1).rstrip("\n"), render())


def render() -> str:
    result = CliRunner().invoke(
        h3, ["--help"], prog_name="vector2dggs h3", terminal_width=80
    )
    assert result.exit_code == 0
    return result.output.rstrip("\n")
