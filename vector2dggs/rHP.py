import vector2dggs.constants as const
from vector2dggs.cli_factory import make_dggs_command

rhp = make_dggs_command(
    "rhp",
    "rhp",
    "rHEALPix",
    const.MIN_RHP,
    const.MAX_RHP,
    grid_default=const.RHP_DEFAULT_GRID,
)
