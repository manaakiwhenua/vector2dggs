import vector2dggs.constants as const
from vector2dggs.cli_factory import make_dggs_command

geohash = make_dggs_command(
    "geohash", "geohash", "Geohash", const.MIN_GEOHASH, const.MAX_GEOHASH
)
