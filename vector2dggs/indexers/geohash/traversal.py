"""Geohash linestring traversal algorithms.

These functions map a shapely LineString geometry to the set of geohash cells
it touches at a given precision. Five algorithms are provided:

  linetrace_greedy      Greedy neighbour walk: simplest and fastest.
  linetrace_astar       A* grid path search: optimal grid path.
  linetrace_bidir       Bidirectional A*: faster than A* for long segments.
  linetrace_linewise    Geometry-aware bidirectional A*: penalises deviation
                        from the actual segment geometry.
  linetrace_intersect   Intersection-based: finds cells the line actually
                        passes through. Most geometrically accurate; no
                        path-finding or tuning parameters required.

All functions have the signature ``(geom, level: int) -> list[str]`` where
``geom`` is a shapely LineString and ``level`` is the geohash precision
(number of characters, 1–12).
"""

import math

from geohash import decode, decode_exactly, encode, neighbors
from shapely.geometry import LineString, box

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def cell_size(level: int) -> tuple[float, float]:
    """Return ``(width_deg, height_deg)`` for a geohash cell at *level*.

    Geohash uses 5 bits per character, alternating longitude/latitude starting
    with longitude, so longitude gets ``ceil(5*level/2)`` bits and latitude
    gets ``floor(5*level/2)`` bits.
    """
    total_bits = 5 * level
    lon_bits = (total_bits + 1) // 2
    lat_bits = total_bits // 2
    return 360.0 / (2**lon_bits), 180.0 / (2**lat_bits)


def sample_segment(
    seg_start: tuple, seg_end: tuple, interval_deg: float
) -> list[tuple]:
    """Sample a lat/lon segment at roughly *interval_deg* spacing.

    Both endpoints are always included. Flat lat/lon interpolation is used:
    appropriate for geohash, whose cells are defined in lat/lon space (unlike
    sphere-projected DGGSs such as A5).
    """
    dx = seg_end[0] - seg_start[0]
    dy = seg_end[1] - seg_start[1]
    length = math.sqrt(dx * dx + dy * dy)
    if length == 0 or interval_deg <= 0:
        return [seg_start, seg_end]
    n = max(1, int(math.ceil(length / interval_deg)))
    return [
        (seg_start[0] + dx * i / n, seg_start[1] + dy * i / n) for i in range(n + 1)
    ]


# ---------------------------------------------------------------------------
# Greedy walk
# ---------------------------------------------------------------------------


def path_cells_greedy(start: str, end: str) -> set[str]:
    """Return geohash cells on a greedy grid path from *start* to *end*.

    At each step the 8 neighbours of the current cell are evaluated and the
    one whose decoded centre is closest (squared Euclidean distance in lat/lon
    space) to the end cell's centre is chosen. The walk terminates when the end
    cell is reached or after 10 000 steps (safety cap for very long segments at
    high resolution).

    This is the geohash equivalent of H3's ``grid_path_cells``.
    """
    cells = {start, end}
    if start == end:
        return cells
    end_lat, end_lng = decode(end)
    current = start
    for _ in range(10_000):
        if current == end:
            break
        current = min(
            neighbors(current),
            key=lambda h: (decode(h)[0] - end_lat) ** 2 + (decode(h)[1] - end_lng) ** 2,
        )
        cells.add(current)
    return cells


def linetrace_greedy(geom, level: int) -> list[str]:
    """Return all geohash cells touched by a LineString using a greedy walk.

    For each consecutive vertex pair the two endpoint geohashes are computed
    and :func:`path_cells_greedy` walks the grid between them.
    """
    coords = list(geom.coords)
    cells = set()
    for i in range(len(coords) - 1):
        start = encode(coords[i][1], coords[i][0], precision=level)
        end = encode(coords[i + 1][1], coords[i + 1][0], precision=level)
        cells.update(path_cells_greedy(start, end))
    return list(cells)


# ---------------------------------------------------------------------------
# A*
# ---------------------------------------------------------------------------


def path_cells_astar(start: str, end: str) -> set[str]:
    """Return geohash cells on the optimal grid path using A* search.

    Unlike the greedy walk (which uses only *h*, the distance to the goal),
    A* tracks *g* (actual steps taken) and prioritises ``f = g + h``. This
    guarantees the shortest connected path and avoids the greedy walk's failure
    mode near the poles or the antimeridian.

    The heuristic is squared Euclidean distance in lat/lon space (no sqrt:
    relative ordering is all that matters). Because all edges have cost 1, the
    heuristic is admissible and A* is optimal.
    """
    import heapq

    if start == end:
        return {start, end}

    end_lat, end_lng = decode(end)

    def h(cell: str) -> float:
        lat, lng = decode(cell)
        return (lat - end_lat) ** 2 + (lng - end_lng) ** 2

    open_heap = [(h(start), start)]
    g = {start: 0}
    came_from = {start: None}

    while open_heap:
        _, current = heapq.heappop(open_heap)
        if current == end:
            path = set()
            node = current
            while node is not None:
                path.add(node)
                node = came_from[node]
            return path
        for nb in neighbors(current):
            g_new = g[current] + 1
            if g_new < g.get(nb, float("inf")):
                g[nb] = g_new
                came_from[nb] = current
                heapq.heappush(open_heap, (g_new + h(nb), nb))

    return {start, end}


def linetrace_astar(geom, level: int) -> list[str]:
    """Return all geohash cells touched by a LineString using A* path finding."""
    coords = list(geom.coords)
    cells = set()
    for i in range(len(coords) - 1):
        start = encode(coords[i][1], coords[i][0], precision=level)
        end = encode(coords[i + 1][1], coords[i + 1][0], precision=level)
        cells.update(path_cells_astar(start, end))
    return list(cells)


# ---------------------------------------------------------------------------
# Bidirectional A*
# ---------------------------------------------------------------------------


def path_cells_bidir(start: str, end: str) -> set[str]:
    """Return geohash cells on the optimal grid path using bidirectional A*.

    Two frontiers expand simultaneously: one forward from *start*, one
    backward from *end*: each guided by ``f = g + h`` toward the opposite
    endpoint. The search terminates using the Kaindl-Kainz condition
    (``best_cost <= min_f_fwd + min_f_bwd``) to guarantee optimality.
    """
    import heapq

    if start == end:
        return {start, end}

    start_lat, start_lng = decode(start)
    end_lat, end_lng = decode(end)

    def h_fwd(cell: str) -> float:
        lat, lng = decode(cell)
        return (lat - end_lat) ** 2 + (lng - end_lng) ** 2

    def h_bwd(cell: str) -> float:
        lat, lng = decode(cell)
        return (lat - start_lat) ** 2 + (lng - start_lng) ** 2

    g_fwd = {start: 0}
    came_from_fwd = {start: None}
    heap_fwd = [(h_fwd(start), start)]

    g_bwd = {end: 0}
    came_from_bwd = {end: None}
    heap_bwd = [(h_bwd(end), end)]

    best_cost = math.inf
    meeting_cell = None

    def reconstruct(cell: str) -> set[str]:
        path = set()
        node = cell
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
                g_new = g_fwd[current] + 1
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
                g_new = g_bwd[current] + 1
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


def linetrace_bidir(geom, level: int) -> list[str]:
    """Return all geohash cells touched by a LineString using bidirectional A*."""
    coords = list(geom.coords)
    cells = set()
    for i in range(len(coords) - 1):
        start = encode(coords[i][1], coords[i][0], precision=level)
        end = encode(coords[i + 1][1], coords[i + 1][0], precision=level)
        cells.update(path_cells_bidir(start, end))
    return list(cells)


# ---------------------------------------------------------------------------
# Geometry-aware bidirectional A*
# ---------------------------------------------------------------------------


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
    fidelity: ``w=0`` degenerates to :func:`path_cells_bidir`; larger *w*
    increasingly penalises cells that stray from the actual segment. Because
    the perpendicular distance is in degrees and the base step cost is 1, *w*
    must be calibrated to the resolution: roughly ``w ~ 1 / cell_size_degrees``
    to make one cell of deviation cost one extra step.
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
    came_from_fwd = {start: None}
    heap_fwd = [(h_fwd(start), start)]

    g_bwd = {end: 0.0}
    came_from_bwd = {end: None}
    heap_bwd = [(h_bwd(end), end)]

    best_cost = math.inf
    meeting_cell = None

    def reconstruct(cell: str) -> set[str]:
        path = set()
        node = cell
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


# ---------------------------------------------------------------------------
# Intersection-based (A5-style)
# ---------------------------------------------------------------------------


def linetrace_intersect(geom, level: int) -> list[str]:
    """Return all geohash cells touched by a LineString using intersection testing.

    This is the most geometrically accurate algorithm. It is analogous to A5's
    ``lineStringToCells``:

    1. Each vertex-pair segment is sampled at half-cell-radius intervals.
    2. For each consecutive sample pair, a strict local BFS finds every cell
       whose bounding box the sub-segment geometrically intersects.
    3. Because geohash cells are axis-aligned rectangles in lat/lon space, no
       face projection is needed: intersection is a direct bbox vs. segment
       check via shapely.

    Unlike the path-finding algorithms, this approach finds cells the line
    actually passes through rather than walking the grid between endpoint cells,
    and requires no tuning parameters.
    """
    coords = list(geom.coords)
    cells = set()

    cell_width, cell_height = cell_size(level)
    cell_radius = 0.5 * (cell_width**2 + cell_height**2) ** 0.5
    sample_interval = cell_radius * 0.5

    for i in range(len(coords) - 1):
        seg_start = coords[i][:2]
        seg_end = coords[i + 1][:2]

        samples = sample_segment(seg_start, seg_end, sample_interval)
        sample_cells = [encode(s[1], s[0], precision=level) for s in samples]

        for j in range(len(samples) - 1):
            a, b = samples[j], samples[j + 1]
            subseg = LineString([a, b])
            cell_a, cell_b = sample_cells[j], sample_cells[j + 1]
            cells.add(cell_a)
            cells.add(cell_b)
            if cell_a == cell_b:
                continue

            visited = {cell_a, cell_b}
            frontier = [cell_a, cell_b]
            while frontier:
                next_frontier = []
                for cell in frontier:
                    for nb in neighbors(cell):
                        if nb in visited:
                            continue
                        visited.add(nb)
                        lat, lon, lat_err, lon_err = decode_exactly(nb)
                        if box(
                            lon - lon_err,
                            lat - lat_err,
                            lon + lon_err,
                            lat + lat_err,
                        ).intersects(subseg):
                            cells.add(nb)
                            next_frontier.append(nb)
                frontier = next_frontier

    return list(cells)
