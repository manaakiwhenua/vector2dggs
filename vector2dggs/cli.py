import click

from vector2dggs import __version__
from vector2dggs.a5 import a5
from vector2dggs.h3 import h3
from vector2dggs.rHP import rhp
from vector2dggs.s2 import s2
from vector2dggs.geohash import geohash


@click.group()
@click.version_option(version=__version__)
def cli():
    pass


cli.add_command(a5)
cli.add_command(h3)
cli.add_command(rhp)
cli.add_command(s2)
cli.add_command(geohash)


def main():
    cli()
