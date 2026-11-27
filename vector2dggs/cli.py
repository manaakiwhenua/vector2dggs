import click

from vector2dggs import __version__
from vector2dggs.commands import COMMANDS


@click.group()
@click.version_option(version=__version__)
def cli() -> None:
    pass


for command in COMMANDS:
    cli.add_command(command)


def main() -> None:
    cli()
