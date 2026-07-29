"""Geohash linestring traversal.

Maps a shapely LineString geometry to the set of geohash cells it touches at
a given precision, via a geometry-aware bidirectional A* search that
penalises deviation from the actual segment geometry.

``linetrace_linewise(geom, level: int) -> list[str]`` where ``geom`` is a
shapely LineString and ``level`` is the geohash precision (number of
characters, 1-12).
"""

import math

from geohash import decode, encode, neighbors
from shapely.geometry import LineString


def path_cells_linewise(
    start: str, end: str, seg_start: tuple, seg_end: tuple, w: float = 1.0
) -> set[str]:
    """Return geohash cells on a geometry-aware bidirectional A* path.

    Each step costs ``1 + w * perp_distance(cell, segment)`` where
    ``perp_distance`` is the perpendicular distance from the candidate cell's
    centre to the actual line segment ``[seg_start, seg_end]``. The deviation
    penalty is added to *g* (edge cost), not *h* (heuristic), so the heuristic
    remains admissible and the Kaindl-Kainz termination condition is correct.

    The weight *w* controls the trade-off between shortest grid path and line
    fidelity: ``w=0`` degenerates to an unweighted bidirectional A*; larger
    *w* increasingly penalises cells that stray from the actual segment.
    Because the perpendicular distance is in degrees and the base step cost
    is 1, *w* must be calibrated to the resolution: roughly
    ``w ~ 1 / cell_size_degrees`` to make one cell of deviation cost one
    extra step.
    """
    import heapq

    if start == end:
        return {start, end}

    line = LineString([seg_start, seg_end])
    from shapely.geometry import Point

    start_lat, start_lng = decode(start)
    end_lat, end_lng = decode(end)

    def h_fwd(cell: str) -> float:
        lat, lng = decode(cell)
        return (lat - end_lat) ** 2 + (lng - end_lng) ** 2

    def h_bwd(cell: str) -> float:
        lat, lng = decode(cell)
        return (lat - start_lat) ** 2 + (lng - start_lng) ** 2

    def step_cost(cell: str) -> float:
        lat, lng = decode(cell)
        return 1.0 + w * line.distance(Point(lng, lat))

    g_fwd = {start: 0.0}
    came_from_fwd: dict[str, str | None] = {start: None}
    heap_fwd = [(h_fwd(start), start)]

    g_bwd = {end: 0.0}
    came_from_bwd: dict[str, str | None] = {end: None}
    heap_bwd = [(h_bwd(end), end)]

    best_cost = math.inf
    meeting_cell: str | None = None

    def reconstruct(cell: str) -> set[str]:
        path = set()
        node: str | None = cell
        while node is not None:
            path.add(node)
            node = came_from_fwd[node]
        node = came_from_bwd[cell]
        while node is not None:
            path.add(node)
            node = came_from_bwd[node]
        return path

    while heap_fwd and heap_bwd:
        min_f_fwd = heap_fwd[0][0]
        min_f_bwd = heap_bwd[0][0]
        if meeting_cell is not None and best_cost <= min_f_fwd + min_f_bwd:
            return reconstruct(meeting_cell)
        if min_f_fwd <= min_f_bwd:
            _, current = heapq.heappop(heap_fwd)
            for nb in neighbors(current):
                g_new = g_fwd[current] + step_cost(nb)
                if g_new < g_fwd.get(nb, math.inf):
                    g_fwd[nb] = g_new
                    came_from_fwd[nb] = current
                    heapq.heappush(heap_fwd, (g_new + h_fwd(nb), nb))
                if nb in g_bwd:
                    total = g_fwd[nb] + g_bwd[nb]
                    if total < best_cost:
                        best_cost = total
                        meeting_cell = nb
        else:
            _, current = heapq.heappop(heap_bwd)
            for nb in neighbors(current):
                g_new = g_bwd[current] + step_cost(nb)
                if g_new < g_bwd.get(nb, math.inf):
                    g_bwd[nb] = g_new
                    came_from_bwd[nb] = current
                    heapq.heappush(heap_bwd, (g_new + h_bwd(nb), nb))
                if nb in g_fwd:
                    total = g_fwd[nb] + g_bwd[nb]
                    if total < best_cost:
                        best_cost = total
                        meeting_cell = nb

    return reconstruct(meeting_cell) if meeting_cell else {start, end}


def linetrace_linewise(geom, level: int, w: float = 1.0) -> list[str]:
    """Return all geohash cells touched by a LineString using geometry-aware bidir A*.

    Each segment is traced with :func:`path_cells_linewise` which penalises
    cells whose centres deviate from the actual segment geometry. See
    :func:`path_cells_linewise` for guidance on tuning *w*.
    """
    coords = list(geom.coords)
    cells = set()
    for i in range(len(coords) - 1):
        seg_start = coords[i][:2]
        seg_end = coords[i + 1][:2]
        start = encode(coords[i][1], coords[i][0], precision=level)
        end = encode(coords[i + 1][1], coords[i + 1][0], precision=level)
        cells.update(path_cells_linewise(start, end, seg_start, seg_end, w))
    return list(cells)
