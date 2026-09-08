#!/usr/bin/env python3
"""Plot the unprojected-filter slice emitted by FooterQuerySweep."""

import argparse
import csv
import html
from collections import defaultdict
from pathlib import Path


COLORS = {"oss": "#94a3b8", "jump": "#f59e0b", "modular": "#2563eb"}
LABELS = {"oss": "OSS", "jump": "Jump table", "modular": "Modular"}
DATASET_LABELS = {
    "us-accidents-00004-of-00007": "US Accidents",
    "fineweb-10bt-000": "FineWeb 10BT",
    "hacker-news-00000-of-00039": "Hacker News",
    "yellow-tripdata-2025-01": "Yellow Taxi",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path)
    parser.add_argument("svg", type=Path)
    parser.add_argument("--kind", choices=("pruned", "selective"), default="pruned")
    args = parser.parse_args()

    rows = list(csv.DictReader(args.csv.open()))
    selected = [row for row in rows
                if row["query_kind"] == args.kind and row["shape"] == "filter_unprojected"]
    grouped = defaultdict(list)
    for row in selected:
        grouped[row["dataset"]].append(row)

    width, height = 1200, 790
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        "<style>",
        "text { font-family: Inter, ui-sans-serif, system-ui, sans-serif; fill: #172033; }",
        ".title { font-size: 25px; font-weight: 700; }",
        ".subtitle { font-size: 14px; fill: #64748b; }",
        ".panel-title { font-size: 17px; font-weight: 650; }",
        ".axis { font-size: 12px; fill: #64748b; }",
        ".value { font-size: 11px; font-weight: 600; }",
        ".grid { stroke: #e2e8f0; stroke-width: 1; }",
        "</style>",
        '<rect width="1200" height="790" fill="#ffffff" rx="12"/>',
        f'<text class="title" x="54" y="43">{title(args.kind)}</text>',
        f'<text class="subtitle" x="54" y="68">{subtitle(args.kind)}</text>',
    ]

    legend_x = 760
    for index, footer in enumerate(("oss", "jump", "modular")):
        x = legend_x + index * 135
        parts.append(f'<rect x="{x}" y="31" width="13" height="13" rx="3" '
                     f'fill="{COLORS[footer]}"/>')
        parts.append(f'<text class="axis" x="{x + 19}" y="42">{LABELS[footer]}</text>')

    for panel, dataset in enumerate(DATASET_LABELS):
        render_panel(parts, panel, dataset, grouped[dataset])

    parts.append('<text class="axis" x="600" y="778" text-anchor="middle">'
                 'Projected output columns (filter column is additional and not returned)</text>')
    parts.append("</svg>")
    args.svg.write_text("\n".join(parts) + "\n")


def title(kind):
    if kind == "selective":
        return "Selective full queries with real data scans"
    return "Full queries dominated by footer work"


def subtitle(kind):
    if kind == "selective":
        return ("One unprojected filter column; statistics prune most row groups and the "
                "survivors are scanned to completion.")
    return ("One unprojected filter column; all row groups are eliminated by statistics, "
            "so no data pages are read.")


def render_panel(parts, panel, dataset, rows):
    left = 55 + (panel % 2) * 580
    top = 105 + (panel // 2) * 325
    chart_left, chart_top = left + 52, top + 64
    chart_width, chart_height = 470, 205
    by_projection = defaultdict(dict)
    projection_order = []
    for row in rows:
        key = row["projection"]
        if key not in by_projection:
            projection_order.append(key)
        by_projection[key][row["footer"]] = row
    maximum = max(float(row["query_ms"]) for row in rows) * 1.18
    filter_name = rows[0]["filter_columns"] if rows else ""

    parts.append(f'<rect x="{left}" y="{top}" width="550" height="300" rx="12" '
                 'fill="#f8fafc" stroke="#e2e8f0"/>')
    parts.append(f'<text class="panel-title" x="{left + 20}" y="{top + 28}">'
                 f'{html.escape(DATASET_LABELS[dataset])}</text>')
    parts.append(f'<text class="subtitle" x="{left + 20}" y="{top + 49}">'
                 f'Filter column: {html.escape(filter_name)} (not projected)</text>')

    for tick in range(5):
        value = maximum * tick / 4
        y = chart_top + chart_height - chart_height * tick / 4
        parts.append(f'<line class="grid" x1="{chart_left}" y1="{y:.1f}" '
                     f'x2="{chart_left + chart_width}" y2="{y:.1f}"/>')
        parts.append(f'<text class="axis" x="{chart_left - 8}" y="{y + 4:.1f}" '
                     f'text-anchor="end">{value:.1f}</text>')

    group_width = chart_width / len(projection_order)
    bar_width = 25
    for group, projection in enumerate(projection_order):
        entries = by_projection[projection]
        center = chart_left + group_width * (group + 0.5)
        actual = next(iter(entries.values()))["projected"]
        label = "max" if projection == "100pct" else projection.replace("pct", "%")
        parts.append(f'<text class="axis" x="{center:.1f}" '
                     f'y="{chart_top + chart_height + 19}" text-anchor="middle">'
                     f'{label} ({actual})</text>')
        for footer_index, footer in enumerate(("oss", "jump", "modular")):
            row = entries[footer]
            value = float(row["query_ms"])
            bar_height = chart_height * value / maximum
            x = center + (footer_index - 1) * (bar_width + 4) - bar_width / 2
            y = chart_top + chart_height - bar_height
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width}" '
                         f'height="{bar_height:.1f}" rx="3" fill="{COLORS[footer]}"/>')
            parts.append(f'<text class="value" x="{x + bar_width / 2:.1f}" y="{y - 5:.1f}" '
                         f'text-anchor="middle">{value:.2f}</text>')

    parts.append(f'<text class="axis" transform="translate({left + 14},'
                 f'{chart_top + chart_height / 2}) rotate(-90)" text-anchor="middle">'
                 'Full query (ms)</text>')


if __name__ == "__main__":
    main()
