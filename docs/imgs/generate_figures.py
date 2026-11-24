"""Regenerate the README figures.

Indexes the repo's own test fixture (LCDB v5.0 land cover for Rangatira,
South East Island, Chatham Islands; CC BY 4.0, LRIS) with the vector2dggs CLI
and renders into this directory:

- indexing-example.png     the polygons at two H3 resolutions, the table and
                           partitioned Parquet they become, and output size
- compaction-example.png   the same polygons at rHEALPix 13, before and after
                           --compact
- containment-modes.gif    the three -m modes on five grids, one frame each,
                           and the same animation as containment-modes.mp4
                           when ffmpeg is on PATH

Needs vector2dggs[all] on PATH (or the CLI named by VECTOR2DGGS), geopandas,
pyarrow, h3, matplotlib and pillow. Indexing takes a few minutes of CPU; the
runs are kept in --workdir and reused.

    python docs/imgs/generate_figures.py --workdir /tmp/v2d-figs
"""

import argparse
import os
import shutil
import subprocess
import tempfile

import matplotlib

matplotlib.use("Agg")
import geopandas as gpd
import h3
import matplotlib.pyplot as plt
import numpy as np
import pyarrow.parquet as pq
import shapely
from matplotlib.collections import LineCollection, PolyCollection
from matplotlib.patches import Patch
from PIL import Image

HERE = os.path.dirname(os.path.abspath(__file__))
VECTOR2DGGS = os.environ.get("VECTOR2DGGS", "vector2dggs")
FIXTURE = os.path.join(
    os.path.dirname(os.path.dirname(HERE)), "tests", "data", "se-island.gpkg"
)
LAYER, CRS = "se-island", "EPSG:3793"  # the 14-feature layer; Chatham Islands TM
ID, ATTR = "LCDB_UID", "Name_2018"

HERO_RES = (10, 12)  # H3 resolutions for the indexing figure's map panels
DISK_RES, DISK_PARENT = 12, 8  # the on-disk comparison: 10 partitions at r8
COMPACT_DGGS, COMPACT_RES = "rhp", 13
# Grid and resolution pairs for the containment animation, chosen so cells
# are a legible size over the zoomed window.
MODE_GRIDS = [("h3", 12), ("s2", 18), ("a5", 17), ("rhp", 12), ("geohash", 8)]
MODES = {
    "centre": (
        "A cell is indexed when its CENTRE POINT falls inside the feature",
        "Cells partition the plane — a coverage in gives a coverage out, and no "
        "cell is claimed twice",
    ),
    "intersects": (
        "A cell is indexed when its AREA MEETS the feature anywhere",
        "Complete coverage — cells straddling a shared edge belong to both "
        "features (darker below)",
    ),
    "within": (
        "A cell is indexed when its AREA LIES WHOLLY INSIDE the feature",
        "Every cell is unambiguously the feature's — at the cost of dropping the "
        "boundary",
    ),
}
GRID_NAMES = {
    "h3": "H3",
    "s2": "S2",
    "a5": "A5",
    "rhp": "rHEALPix",
    "geohash": "Geohash",
}
# Zoom windows as fractions of the island's square bounds: (cx, cy, half-width)
COMPACT_ZOOM = (0.59, 0.49, 0.11)
MODE_ZOOM = (0.74, 0.12, 0.045)

LC_COLOURS = {
    "Indigenous Forest": "#1b6e3a",
    "Broadleaved Indigenous Hardwoods": "#5fc48a",
    "Low Producing Grassland": "#e8b43a",
    "Gravel or Rock": "#a79bd6",
}
INK, MUT, FAINT, NOTE = "#1a1a1a", "#555555", "#888888", "#b0332a"
MONO = "DejaVu Sans Mono"
CELL, CELL_DUP, FEATURE, BLUE, RED = (
    "#8ab0cc",
    "#2e5f8a",
    "#efe9d9",
    "#2f6fb7",
    "#c0392b",
)

plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "text.color": INK,
        "axes.edgecolor": "#bbbbbb",
        "figure.facecolor": "white",
    }
)


def v2d(w, dggs, out, res, *extra, check=True):
    """Index the island; returns False when the CLI rejects the arguments."""
    p = subprocess.run(
        [
            VECTOR2DGGS,
            dggs,
            f"{w}/island.gpkg",
            out,
            "-r",
            str(res),
            "-id",
            ID,
            "-ka",
            ATTR,
            "-o",
            "-v",
            "WARNING",
            *extra,
        ],
        check=check,
        stdout=subprocess.DEVNULL,
        stderr=None if check else subprocess.DEVNULL,
    )
    return p.returncode == 0


def island():
    return gpd.read_file(FIXTURE, layer=LAYER).to_crs(CRS)[[ID, ATTR, "geometry"]]


def index_all(w):
    if not os.path.exists(f"{w}/island.gpkg"):
        isl = island()
        isl.to_file(f"{w}/island.gpkg")
        isl.to_parquet(f"{w}/island.parquet")
    r_lo, r_hi = HERO_RES
    pr = ("-pr", str(DISK_PARENT))
    jobs = {
        f"h3_{r_lo}": lambda o: v2d(w, "h3", o, r_lo, "--geo", "polygon"),
        f"h3_{r_hi}": lambda o: v2d(w, "h3", o, r_hi, "--geo", "polygon"),
        **{
            f"disk_{cp}{co}": (
                lambda o, cp=cp, co=co: v2d(
                    w, "h3", o, DISK_RES, "-cp", cp, *pr, *(("-co",) if co else ())
                )
            )
            for cp in ("none", "snappy", "zstd")
            for co in ("", "_co")
        },
        "compact": lambda o: v2d(w, COMPACT_DGGS, o, COMPACT_RES, "--geo", "polygon"),
        "compact_co": lambda o: v2d(
            w, COMPACT_DGGS, o, COMPACT_RES, "--geo", "polygon", "-co"
        ),
    }
    for name, job in jobs.items():
        out = f"{w}/runs/{name}"
        if not os.path.exists(out):
            print(f"  indexing {name}")
            job(out)
    # A backend that lacks a mode exits before writing anything; that run is
    # retried on every invocation, which costs nothing.
    for m in MODES:
        for d, r in MODE_GRIDS:
            out = f"{w}/runs/mode_{m}_{d}"
            if not os.path.exists(out):
                print(f"  indexing mode_{m}_{d}")
                v2d(w, d, out, r, "-m", m, "--geo", "polygon", check=False)


def cells(path):
    df = pq.read_table(path).to_pandas()
    if df.index.name:  # the cell ID comes back as the index
        df = df.reset_index()
    g = gpd.GeoDataFrame(
        df.drop(columns=["geometry"]),
        geometry=gpd.GeoSeries(
            shapely.from_wkb(df["geometry"]), index=df.index, crs=4326
        ),
    )
    return g.to_crs(CRS)


def cell_col(g, res):
    return next(c for c in g.columns if c.endswith(f"_{res:02d}"))


def plot_cells(ax, g, colors, lw=0.0, ec="none"):
    verts = [np.asarray(p.exterior.coords) for p in g.geometry]
    ax.add_collection(
        PolyCollection(
            verts, facecolors=colors, edgecolors=ec, linewidths=lw, rasterized=True
        )
    )


def plot_boundaries(ax, isl, lw=1.3):
    lines = []
    for geom in isl.geometry:
        polys = geom.geoms if hasattr(geom, "geoms") else [geom]
        for p in polys:
            lines.append(np.asarray(p.exterior.coords))
            lines.extend(np.asarray(r.coords) for r in p.interiors)
    ax.add_collection(LineCollection(lines, colors=INK, linewidths=lw))


def lc_colors(g):
    return [LC_COLOURS[n] for n in g[ATTR]]


def square_bounds(gdf, pad=0.04):
    x0, y0, x1, y1 = gdf.total_bounds
    cx, cy, half = (x0 + x1) / 2, (y0 + y1) / 2, max(x1 - x0, y1 - y0) / 2 * (1 + pad)
    return (cx - half, cx + half, cy - half, cy + half)


def zoom_window(sb, frac):
    fx, fy, fh = frac
    w = sb[1] - sb[0]
    cx, cy, half = sb[0] + fx * w, sb[2] + fy * w, fh * w
    return (cx - half, cx + half, cy - half, cy + half)


def frame(ax, win, title=None, size=13):
    ax.set_xlim(win[0], win[1]), ax.set_ylim(win[2], win[3])
    ax.set_aspect("equal"), ax.set_xticks([]), ax.set_yticks([])
    if title:
        ax.set_title(title, size=size, pad=6)


def badge(ax, text, loc="ne", size=10):
    x, ha = (0.97, "right") if loc == "ne" else (0.03, "left")
    ax.text(
        x,
        0.97 if loc == "ne" else 0.03,
        text,
        transform=ax.transAxes,
        ha=ha,
        va="top" if loc == "ne" else "bottom",
        size=size,
        bbox={"boxstyle": "round,pad=0.35", "fc": "white", "ec": "#cccccc"},
    )


def scalebar(ax, metres, label, win):
    x0 = win[0] + (win[1] - win[0]) * 0.06
    y0 = win[2] + (win[3] - win[2]) * 0.06
    ax.plot([x0, x0 + metres], [y0, y0], color=INK, lw=2, solid_capstyle="butt")
    ax.text(x0 + metres / 2, y0 + (win[3] - win[2]) * 0.025, label, ha="center", size=9)


def in_view(g, win):
    return g.cx[win[0] : win[1], win[2] : win[3]]


def text_run(fig, x, y, parts, gap=0.008):
    """Place text fragments left to right on one baseline."""
    r = fig.canvas.get_renderer()
    for text, kw in parts:
        t = fig.text(x, y, text, va="center", **kw)
        x = fig.transFigure.inverted().transform(t.get_window_extent(r))[1][0] + gap


def dir_kb(d):
    return round(
        sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(d) for f in fs)
        / 1024
    )


def fig_indexing(w):
    isl = island()
    sb = square_bounds(isl)
    r_lo, r_hi = HERO_RES
    fig = plt.figure(figsize=(13.4, 13.4), dpi=100)
    fig.text(
        0.018,
        0.980,
        "vector2dggs — vector in, DGGS cells out",
        size=21,
        weight="bold",
        va="top",
    )
    fig.text(
        0.018,
        0.951,
        "Chatham Islands land cover (LCDB v5.0, mapped at 1:50,000) indexed to H3  ·  "
        "attributes ride along, one row per cell",
        size=12.5,
        color=MUT,
        va="top",
    )
    fig.text(
        0.018,
        0.917,
        "Every polygon becomes the set of cells that fill it, and each cell keeps the "
        "feature's attributes\nA finer resolution follows the boundary more closely — "
        "and costs proportionally more rows",
        size=11,
        va="top",
        linespacing=1.5,
    )

    runs = {r: cells(f"{w}/runs/h3_{r}") for r in HERO_RES}
    dropped = isl[~isl[ID].isin(runs[r_lo][ID])]
    for x, (title, r) in zip(
        (0.018, 0.352, 0.686),
        [("Vector input", None), *[(f"H3 resolution {r}", r) for r in HERO_RES]],
        strict=True,
    ):
        ax = fig.add_axes([x, 0.545, 0.30, 0.315])
        if r is None:
            isl.plot(ax=ax, color=lc_colors(isl), edgecolor="white", linewidth=0.5)
            badge(ax, f"{len(isl)} polygons")
        else:
            g = runs[r]
            plot_cells(ax, g, lc_colors(g), lw=0.3 if r == r_lo else 0.0, ec="white")
            badge(ax, f"{len(g):,} cells")
            if r == r_lo:
                dropped.plot(ax=ax, facecolor="none", edgecolor=NOTE, linewidth=1.3)
            else:
                scalebar(ax, 500, "500 m", sb)
        frame(ax, sb, title)
    fig.text(
        0.502,
        0.528,
        f"{len(dropped)} of {len(isl)} features (outlined red) are smaller than a cell "
        f"and produce nothing at this resolution — {len(isl) - len(dropped)} survive."
        "\n-m intersects would keep them.",
        size=10.5,
        color=NOTE,
        ha="center",
        va="top",
        linespacing=1.4,
    )
    fig.legend(
        handles=[Patch(fc=c, label=n) for n, c in LC_COLOURS.items()],
        loc="upper left",
        bbox_to_anchor=(0.012, 0.496),
        ncol=4,
        frameon=False,
        fontsize=10.5,
        handlelength=1.4,
        columnspacing=1.8,
    )

    # ---- one feature in, its rows out --------------------------------------
    t = pq.read_table(f"{w}/runs/disk_snappy").to_pandas().reset_index()
    cid = cell_col(t, DISK_RES)
    feature = t[ID].value_counts().idxmax()
    rows = t[t[ID] == feature].sort_values(cid)
    fig.text(0.018, 0.455, "One feature in", size=12, weight="bold", va="top")
    attrs = isl[isl[ID] == feature].iloc[0]
    fig.text(
        0.018,
        0.425,
        f"{ID:11s}{attrs[ID]}\n{ATTR:11s}{attrs[ATTR]}",
        size=10,
        family=MONO,
        va="top",
        linespacing=1.7,
    )
    fig.text(
        0.018,
        0.362,
        f"{len(rows):,} rows out  ·  H3 resolution {DISK_RES}",
        size=12,
        weight="bold",
        va="top",
    )
    lines = [f"{cid:20s}{ID:20s}{ATTR}"]
    for _, r in rows.head(4).iterrows():
        lines.append(f"{r[cid]:20s}{r[ID]:20s}{r[ATTR]}")
    lines.append(f"… {len(rows) - 4:,} more")
    fig.text(
        0.018,
        0.332,
        "\n".join(lines),
        size=9.5,
        family=MONO,
        va="top",
        linespacing=1.7,
    )
    fig.text(
        0.018,
        0.212,
        "-k / -ka NAME       keep every attribute, or only the named ones\n"
        "--cell-id uint64    native integer cell IDs (H3, S2, A5)\n"
        "--geo polygon|point GeoParquet cell geometry\n"
        "-m intersects       also index features smaller than a cell",
        size=9.5,
        family=MONO,
        color=MUT,
        va="top",
        linespacing=1.7,
    )

    # ---- on disk ------------------------------------------------------------
    fig.text(
        0.52,
        0.455,
        "On disk  ·  hive-partitioned Parquet",
        size=12,
        weight="bold",
        va="top",
    )
    disk = f"{w}/runs/disk_snappy"
    parts = sorted(p for p in os.listdir(disk) if "=" in p)
    tree = ["out/"]
    for p in parts[:2]:
        f = os.listdir(f"{disk}/{p}")[0]
        tree.append(f"  {p}/{f[:14]}….parquet")
    tree.append(f"  … {len(parts)} partitions in all")
    fig.text(
        0.52,
        0.425,
        "\n".join(tree),
        size=9.5,
        family=MONO,
        va="top",
        linespacing=1.7,
    )
    fig.text(
        0.52,
        0.355,
        "The partition key lives in the directory name — it costs no bytes in the "
        "files,\nand a reader can skip whole partitions without opening them",
        size=10,
        color=MUT,
        va="top",
        linespacing=1.5,
    )

    kb = {
        "gpkg": os.path.getsize(f"{w}/island.gpkg") / 1024,
        "geoparquet": os.path.getsize(f"{w}/island.parquet") / 1024,
    }
    n_rows = {}
    for co in ("", "_co"):
        csv = f"{w}/runs/table{co}.csv"
        src = pq.read_table(f"{w}/runs/disk_snappy{co}")
        n_rows[co] = src.num_rows
        if not os.path.exists(csv):
            src.sort_by(cid).to_pandas().to_csv(csv, index=False)
        kb["csv" + co] = os.path.getsize(csv) / 1024
        for cp in ("none", "snappy", "zstd"):
            kb[cp + co] = dir_kb(f"{w}/runs/disk_{cp}{co}")
    n_vertices = int(shapely.get_num_coordinates(isl.geometry).sum())
    cell_m = h3.average_hexagon_area(DISK_RES, "m^2") ** 0.5
    fig.text(
        0.52, 0.300, "The same feature set on disk", size=12, weight="bold", va="top"
    )
    fig.text(
        0.52,
        0.272,
        f"The columnar layout alone is {kb['csv'] / kb['none']:.1f}× smaller than CSV; "
        f"-cp and -co compound\nThe source is {n_vertices:,} vertices at 1:50,000 — "
        f"H3 {DISK_RES} cells are ~{cell_m:.0f} m across,\nfiner than that scale",
        size=10,
        color=MUT,
        va="top",
        linespacing=1.5,
    )
    bars = [
        ("input  ·  GeoPackage", "gpkg", FAINT),
        ("input  ·  GeoParquet", "geoparquet", FAINT),
        None,
        ("CSV", "csv", "#8a8a8a"),
        ("Parquet, -cp none", "none", BLUE),
        ("snappy  (default)", "snappy", BLUE),
        ("-cp zstd", "zstd", BLUE),
    ]
    bx = fig.add_axes([0.60, 0.030, 0.30, 0.172])
    xmax = kb["csv"] * 1.35
    y = len(bars) - 0.5
    for b in bars:
        if b is None:  # a half-row rule between inputs and outputs
            y -= 0.5
            bx.axhline(y, color="#dddddd", lw=0.8)
            bx.text(xmax, y + 0.06, "saved by -co", size=9, color=FAINT, ha="right")
            continue
        y -= 1
        label, key, c = b
        bx.text(-xmax * 0.015, y, label, ha="right", va="center", size=9.5)
        if key + "_co" not in kb:
            bx.barh(y, kb[key], height=0.6, color=c)
            bx.text(
                kb[key] + xmax * 0.012, y, f"{kb[key]:,.0f} KB", va="center", size=9.5
            )
            continue
        for dy, co, colour in ((0.22, "", c), (-0.22, "_co", RED)):
            v = kb[key + co]
            bx.barh(y + dy, v, height=0.4, color=colour)
            bx.text(
                v + xmax * 0.012,
                y + dy,
                f"{v:,.0f} KB",
                va="center",
                size=9.5,
                color=INK if not co else RED,
            )
        bx.text(
            xmax,
            y,
            f"{kb[key] / kb[key + '_co']:.1f}×",
            ha="right",
            va="center",
            size=10,
            weight="bold",
            color=RED,
        )
    bx.set_xlim(0, xmax), bx.set_ylim(-0.6, len(bars) - 0.9), bx.axis("off")
    fig.legend(
        handles=[
            Patch(fc=FAINT, label="input"),
            Patch(fc=BLUE, label=f"as indexed  ·  {n_rows['']:,} rows"),
            Patch(fc=RED, label=f"-co/--compact  ·  {n_rows['_co']:,} rows"),
        ],
        loc="upper left",
        bbox_to_anchor=(0.512, 0.222),
        ncol=3,
        frameon=False,
        fontsize=10.5,
        handlelength=1.4,
        columnspacing=1.6,
    )

    fig.text(
        0.982,
        0.006,
        "vector2dggs · tests/data/se-island.gpkg · LCDB v5.0 (CC BY 4.0, LRIS)",
        size=9,
        color=FAINT,
        ha="right",
    )
    fig.savefig(f"{HERE}/indexing-example.png", dpi=100)
    plt.close(fig)


def fig_compaction(w):
    isl = island()
    sb = square_bounds(isl)
    zoom = zoom_window(sb, COMPACT_ZOOM)
    full, compact = cells(f"{w}/runs/compact"), cells(f"{w}/runs/compact_co")
    name = f"{GRID_NAMES[COMPACT_DGGS]} {COMPACT_RES}"
    fig = plt.figure(figsize=(13.4, 8.3), dpi=100)
    fig.text(0.018, 0.975, "vector2dggs — compaction", size=21, weight="bold", va="top")
    fig.text(
        0.018,
        0.925,
        f"The same island land cover, indexed to {name} · a coverage in, so every "
        "cell belongs to exactly one feature",
        size=12.5,
        color=MUT,
        va="top",
    )
    fig.text(
        0.018,
        0.878,
        "-co/--compact replaces a complete set of sibling cells with their parent, "
        "down to the partition's parent resolution\nThe coverage is identical — only "
        "the row count changes, and cells near a boundary stay fine while interiors "
        "go coarse",
        size=11,
        va="top",
        linespacing=1.5,
    )
    panels = [
        (full, zoom, f"Every cell  ·  resolution {COMPACT_RES}", "in view"),
        (compact, zoom, "-co/--compact  ·  same coverage", "in view"),
        (compact, sb, "Whole feature set  ·  compacted", "cells"),
    ]
    for i, (g, win, title, unit) in enumerate(panels):
        ax = fig.add_axes([0.018 + i * 0.3215, 0.283, 0.306, 0.494])
        shown = in_view(g, win) if unit == "in view" else g
        plot_cells(ax, shown, CELL, lw=0.4 if win is zoom else 0.15, ec="white")
        plot_boundaries(ax, isl, lw=1.5 if win is zoom else 1.0)
        if win is sb:
            ax.add_patch(
                plt.Rectangle(
                    (zoom[0], zoom[2]),
                    zoom[1] - zoom[0],
                    zoom[3] - zoom[2],
                    fc="none",
                    ec=NOTE,
                    lw=2,
                )
            )
            scalebar(ax, 500, "500 m", sb)
        else:
            scalebar(ax, 100, "100 m", zoom)
        badge(ax, f"{len(shown):,} {unit}")
        frame(ax, win, title)
    fig.text(
        0.661 + 0.306,
        0.255,
        "red box = the area shown in the two panels at left",
        size=9.5,
        color=FAINT,
        ha="right",
        va="top",
    )
    text_run(
        fig,
        0.018,
        0.14,
        [
            (f"{len(full):,}", {"size": 30, "weight": "bold"}),
            ("→", {"size": 22, "color": FAINT}),
            (f"{len(compact):,}", {"size": 30, "weight": "bold", "color": BLUE}),
            ("cells", {"size": 16, "color": MUT}),
        ],
        gap=0.012,
    )
    fig.text(
        0.018,
        0.065,
        f"{1 - len(compact) / len(full):.0%} fewer rows for the same {len(isl)} "
        "features, and the same coverage",
        size=12,
        color=MUT,
        va="center",
    )
    fig.legend(
        handles=[
            Patch(fc=CELL, label="indexed cell"),
            plt.Line2D([], [], color=INK, lw=1.5, label="feature boundary"),
        ],
        loc="center left",
        bbox_to_anchor=(0.60, 0.11),
        frameon=False,
        fontsize=11,
        handlelength=1.6,
        labelspacing=0.8,
    )
    fig.text(
        0.982,
        0.012,
        "vector2dggs · tests/data/se-island.gpkg · LCDB v5.0 (CC BY 4.0, LRIS)",
        size=9,
        color=FAINT,
        ha="right",
    )
    fig.savefig(f"{HERE}/compaction-example.png", dpi=100)
    plt.close(fig)


def mode_frame(w, mode, isl, sb, zoom, runs):
    fig = plt.figure(figsize=(13.2, 9.4), dpi=100)
    fig.text(
        0.018,
        0.978,
        "vector2dggs — polygon containment modes",
        size=21,
        weight="bold",
        va="top",
    )
    fig.text(
        0.018,
        0.945,
        f"The same {len(isl)} land-cover polygons, indexed five ways · "
        "within ⊆ centre ⊆ intersects",
        size=12.5,
        color=MUT,
        va="top",
    )
    for x, m in zip((0.658, 0.748, 0.862), MODES, strict=True):
        on = m == mode
        fig.text(
            x,
            0.956,
            f"-m {m}",
            size=12.5,
            weight="bold" if on else "normal",
            color="white" if on else FAINT,
            va="center",
            bbox={
                "boxstyle": "round,pad=0.45,rounding_size=0.6",
                "fc": BLUE if on else "#eeeeee",
                "ec": "none",
            },
        )
    fig.text(
        0.018,
        0.895,
        "\n".join(MODES[mode]),
        size=11.5,
        va="top",
        linespacing=1.5,
    )
    slots = [(0.054, 0.48), (0.380, 0.48), (0.706, 0.48), (0.054, 0.10), (0.380, 0.10)]
    for (x, y), (d, r) in zip(slots, MODE_GRIDS, strict=True):
        ax = fig.add_axes([x, y, 0.240, 0.336])
        isl.plot(ax=ax, color=FEATURE, linewidth=0)
        g = runs.get(d)
        if g is None:
            ax.text(
                0.5,
                0.5,
                "not supported",
                transform=ax.transAxes,
                ha="center",
                va="center",
                size=15,
                style="italic",
                color=FAINT,
                bbox={
                    "boxstyle": "round,pad=0.5",
                    "fc": "white",
                    "ec": "#dddddd",
                    "alpha": 0.85,
                },
            )
        else:
            shown = in_view(g, zoom)
            dup = shown[cell_col(shown, r)].duplicated(keep=False)
            plot_cells(ax, shown, np.where(dup, CELL_DUP, CELL), lw=0.5, ec="white")
            badge(ax, f"{len(g):,} cells", size=9.5)
        plot_boundaries(ax, isl, lw=1.5)
        frame(ax, zoom, f"{GRID_NAMES[d]}  ·  resolution {r}")
    ax = fig.add_axes([0.722, 0.10, 0.208, 0.336])
    g = runs["h3"]
    plot_cells(ax, g, CELL, lw=0.1, ec="white")
    plot_boundaries(ax, isl, lw=1.0)
    ax.add_patch(
        plt.Rectangle(
            (zoom[0], zoom[2]),
            zoom[1] - zoom[0],
            zoom[3] - zoom[2],
            fc="none",
            ec=NOTE,
            lw=1.5,
        )
    )
    frame(ax, sb, f"Whole feature set  ·  H3 {MODE_GRIDS[0][1]}")
    fig.text(
        0.722 + 0.208,
        0.085,
        "red box = the area shown in the five panels",
        size=9.5,
        color=FAINT,
        ha="right",
        va="top",
    )
    fig.legend(
        handles=[
            Patch(fc=CELL, label="indexed cell"),
            Patch(fc=CELL_DUP, label="cell claimed by multiple features"),
            Patch(fc=FEATURE, label="feature (land cover)"),
            plt.Line2D([], [], color=INK, lw=1.5, label="feature boundary"),
        ],
        loc="lower left",
        bbox_to_anchor=(0.012, 0.012),
        ncol=4,
        frameon=False,
        fontsize=11,
        handlelength=1.6,
        columnspacing=2.0,
    )
    fig.text(
        0.982,
        0.015,
        "vector2dggs · se-island test data",
        size=9,
        color=FAINT,
        ha="right",
    )
    fig.canvas.draw()
    img = Image.fromarray(np.asarray(fig.canvas.buffer_rgba())).convert("RGB")
    plt.close(fig)
    return img


def fig_modes(w):
    """One frame per mode, cross-faded; GIF always, MP4 when ffmpeg is present."""
    isl = island()
    sb = square_bounds(isl)
    zoom = zoom_window(sb, MODE_ZOOM)
    keys = []
    for m in MODES:
        runs = {
            d: cells(f"{w}/runs/mode_{m}_{d}")
            for d, _ in MODE_GRIDS
            if os.path.exists(f"{w}/runs/mode_{m}_{d}")
        }
        keys.append(mode_frame(w, m, isl, sb, zoom, runs))
    hold, fade = 4000, 70
    frames, durations = [], []
    for i, img in enumerate(keys):
        nxt = keys[(i + 1) % len(keys)]
        frames.append(img), durations.append(hold)
        for a in (0.25, 0.5, 0.75):
            frames.append(Image.blend(img, nxt, a)), durations.append(fade)
    pal = [f.quantize(colors=255, dither=Image.Dither.NONE) for f in frames]
    pal[0].save(
        f"{HERE}/containment-modes.gif",
        save_all=True,
        append_images=pal[1:],
        duration=durations,
        loop=0,
        optimize=True,
    )
    if shutil.which("ffmpeg") is None:
        print("ffmpeg not found; containment-modes.mp4 left as is")
        return
    fdir = f"{w}/mode_frames"
    os.makedirs(fdir, exist_ok=True)
    with open(f"{fdir}/list.txt", "w") as lst:
        for i, (f, ms) in enumerate(zip(frames, durations, strict=True)):
            f.save(f"{fdir}/{i:02d}.png")
            lst.write(f"file '{i:02d}.png'\nduration {ms / 1000}\n")
        lst.write("file '00.png'\n")  # concat demuxer needs a terminal entry
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-i",
            f"{fdir}/list.txt",
            "-vf",
            "fps=25,format=yuv420p",
            "-c:v",
            "libx264",
            "-crf",
            "23",
            "-movflags",
            "+faststart",
            f"{HERE}/containment-modes.mp4",
        ],
        check=True,
    )


def quantise(paths):
    """Palette-quantise the PNGs in place when it saves space."""
    for p in paths:
        q = p + ".q"
        Image.open(p).convert("RGB").quantize(
            colors=255, dither=Image.Dither.FLOYDSTEINBERG
        ).save(q, format="PNG", optimize=True)
        if os.path.getsize(q) < os.path.getsize(p):
            shutil.move(q, p)
        else:
            os.remove(q)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--workdir", help="keep/reuse intermediate data here (default: temp dir)"
    )
    ap.add_argument(
        "--only", nargs="*", choices=["indexing", "compaction", "modes"], default=None
    )
    args = ap.parse_args()
    w = args.workdir or tempfile.mkdtemp(prefix="vector2dggs-figs-")
    os.makedirs(f"{w}/runs", exist_ok=True)
    print(f"workdir: {w}")
    index_all(w)
    want = set(args.only or ["indexing", "compaction", "modes"])
    if "indexing" in want:
        fig_indexing(w)
    if "compaction" in want:
        fig_compaction(w)
    if "modes" in want:
        fig_modes(w)
    quantise([f"{HERE}/{n}-example.png" for n in ("indexing", "compaction")])
    for n in os.listdir(HERE):
        if n.startswith(("indexing", "compaction", "containment")):
            print(f"{n}  {os.path.getsize(f'{HERE}/{n}') / 1024:.0f} KB")


if __name__ == "__main__":
    main()
