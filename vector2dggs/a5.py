import vector2dggs.constants as const
from vector2dggs.cli_factory import make_dggs_command

a5 = make_dggs_command(
    "a5",
    "a5",
    "A5",
    const.MIN_A5,
    const.MAX_A5,
    int_cells=True,
    # pya5 has no wholly-within test; see A5VectorIndexer.SUPPORTED_MODES
    modes=(
        const.ContainmentMode.CENTRE.value,
        const.ContainmentMode.INTERSECTS.value,
    ),
)
