import click

from vector2dggs import __version__
from vector2dggs.h3 import h3
from vector2dggs.rHP import rhp
from vector2dggs.s2 import s2
from vector2dggs.geohash import geohash

#   If the program does terminal interaction, make it output a short
# notice like this when it starts in an interactive mode:

#     <program>  Copyright (C) <year>  <name of author>
#     This program comes with ABSOLUTELY NO WARRANTY; for details type `show w'.
#     This is free software, and you are welcome to redistribute it
#     under certain conditions; type `show c' for details.


@click.group()
@click.version_option(version=__version__)
def cli():
    pass


cli.add_command(h3)
cli.add_command(rhp)
cli.add_command(s2)
cli.add_command(geohash)


def main():
    cli()
