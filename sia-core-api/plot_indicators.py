"""
Calls all indicator endpoints of the SIA exploitation API and saves one figure per indicator, named indicador0.png ... indicador10.png.

Usage
-----
    python plot_indicators.py \
        --base-url http://localhost:8000/exploitation \
        --api-key  KEY \
        --date-start 2025-01-01T00:00:00Z \
        --date-end   2026-03-01T00:00:00Z \
        --cpv-prefixes 48 72 73 \
        --output-dir .
"""

import argparse
import os
import sys
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import requests

plt.rcParams.update({
    "text.usetex": False,
    "font.family": "serif",
    "font.serif":  ["DejaVu Serif"],
})

# ---------------------------------------------------------------------------
# Spanish -> English bimester label translation
# ---------------------------------------------------------------------------
_ES_TO_EN = {
    "Ene": "Jan", "Feb": "Feb", "Mar": "Mar", "Abr": "Apr",
    "May": "May", "Jun": "Jun", "Jul": "Jul", "Ago": "Aug",
    "Sep": "Sep", "Oct": "Oct", "Nov": "Nov", "Dic": "Dec",
}

def _tr(label: str) -> str:
    for es, en in _ES_TO_EN.items():
        label = label.replace(es, en)
    return label

def _tr_list(labels):
    return [_tr(l) for l in labels]

# ---------------------------------------------------------------------------
# Figure / font settings
# ---------------------------------------------------------------------------
FONT_SIZE   = 22
TITLE_SIZE  = 23
LEGEND_SIZE = 20
TICK_SIZE   = 19
DPI         = 300
FIG_W       = 11   # inches – single panel
FIG_H       = 7

_COLORS = ["#86d8df", "#d8a2a0", "#b8dba0"]

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _post(base_url: str, endpoint: str, payload: dict,
          api_key: Optional[str] = None, timeout: int = 120) -> dict:
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key
    resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
    resp.raise_for_status()
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"API error from {endpoint}: {body}")
    return body["data"]


def _build_payload(date_start, date_end, date_field, tender_type, cpv_prefixes):
    p = {
        "date_start":   date_start,
        "date_end":     date_end,
        "date_field":   date_field,
        "cpv_prefixes": cpv_prefixes,
    }
    if tender_type:
        p["tender_type"] = tender_type
    return p

# ---------------------------------------------------------------------------
# Low-level plot helpers
# ---------------------------------------------------------------------------

def _style(ax, title, ylabel, xlabels_en):
    #ax.set_title(title, fontsize=TITLE_SIZE, fontweight="bold", pad=10)
    ax.set_ylabel(ylabel, fontsize=FONT_SIZE)
    ax.set_xlabel("Bimester", fontsize=FONT_SIZE)
    ax.tick_params(axis="both", labelsize=TICK_SIZE)
    ax.set_xticklabels(xlabels_en, rotation=38, ha="right", fontsize=TICK_SIZE)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def _grouped_bars(ax, labels_en, series: dict, pct=False):
    """
    series: {label: [values]}  – one entry per source/dataset.
    Returns handles, leg_labels for the caller to place a legend.
    """
    n     = len(labels_en)
    n_s   = len(series)
    width = 0.7 / max(n_s, 1)
    offsets = np.linspace(-(n_s-1)*width/2, (n_s-1)*width/2, n_s)
    x = np.arange(n)
    handles = []
    for idx, (name, vals) in enumerate(series.items()):
        safe = [v if v is not None else 0.0 for v in vals]
        bars = ax.bar(x + offsets[idx], safe, width,
                      label=name, color=_COLORS[idx % len(_COLORS)], alpha=0.85,
                      edgecolor="black", linewidth=2.2)
        handles.append(bars)
    ax.set_xticks(x)
    if pct:
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    return handles


def _save(fig, path):
    fig.savefig(path, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved -> {path}")

# ---------------------------------------------------------------------------
# One function per indicator
# ---------------------------------------------------------------------------
def plot_ind0(data_by_src, out_dir):
    """Indicator 0 – two sub-plots: tender count and aggregated budget."""
    fig, axes = plt.subplots(1, 2, figsize=(FIG_W * 2, FIG_H))
    plt.subplots_adjust(wspace=0.30)

    for col, (key, ylabel, title, div) in enumerate([
        ("by_count",  "Number of tenders",
         "Ind. 0a – Tender count (CPV 48, 72, 73)", 1),
        ("by_budget", "Aggregated budget (B€)",
         "Ind. 0b – Aggregated estimated budget (excl. VAT)", 1e9),
    ]):
        labels_en = None
        series    = {}
        for src, d in data_by_src.items():
            if d is None:
                series[src] = []
                continue
            if labels_en is None:
                labels_en = _tr_list(d.get("bimester_labels", []))
            raw = d.get(key, [])
            series[src] = [v / div if v is not None else None for v in raw]
        labels_en = labels_en or []
        _grouped_bars(axes[col], labels_en, series)
        _style(axes[col], title, ylabel, labels_en)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=len(handles),
               fontsize=LEGEND_SIZE, frameon=True, bbox_to_anchor=(0.5, 1.02))
    _save(fig, os.path.join(out_dir, "indicador0.png"))


def _plot_dual(data_by_src, key_val, key_cov, ylabel_val, ylabel_cov,
               title_val, title_cov, out_path, pct=True, multi_src=True,
               show_legend=False):
    """Generic two-panel plot: indicator value (left) + coverage (right)."""
    fig, axes = plt.subplots(1, 2, figsize=(FIG_W * 2, FIG_H))
    plt.subplots_adjust(wspace=0.30)

    for col, (key, ylabel, title) in enumerate([
        (key_val, ylabel_val, title_val),
        (key_cov, ylabel_cov, title_cov),
    ]):
        labels_en = None
        series    = {}
        if multi_src:
            for src, d in data_by_src.items():
                if d is None:
                    series[src] = []
                    continue
                if labels_en is None:
                    labels_en = _tr_list(d.get("bimester_labels", []))
                series[src] = d.get(key, [])
        else:
            # single source (insiders)
            d = data_by_src
            if d:
                labels_en = _tr_list(d.get("bimester_labels", []))
                series["insiders"] = d.get(key, [])
        labels_en = labels_en or []
        _grouped_bars(axes[col], labels_en, series, pct=pct)
        _style(axes[col], title, ylabel, labels_en)

    if show_legend:
        if multi_src:
            handles, lbl = axes[0].get_legend_handles_labels()
            fig.legend(handles, lbl, loc="upper center", ncol=len(handles),
                       fontsize=LEGEND_SIZE, frameon=True, bbox_to_anchor=(0.5, 1.02))
        else:
            axes[0].legend(fontsize=LEGEND_SIZE, frameon=True)
    _save(fig, out_path)


def _plot_single(data, key_val, ylabel, title, out_path,
                 pct=False, multi_src=True, data_by_src=None, show_legend=False):
    """Generic single-panel plot."""
    fig, ax = plt.subplots(figsize=(FIG_W, FIG_H))

    labels_en = None
    series    = {}

    if multi_src and data_by_src is not None:
        for src, d in data_by_src.items():
            if d is None:
                series[src] = []
                continue
            if labels_en is None:
                labels_en = _tr_list(d.get("bimester_labels", []))
            series[src] = d.get(key_val, [])
    else:
        if data:
            labels_en = _tr_list(data.get("bimester_labels", []))
            series["insiders"] = data.get(key_val, [])

    labels_en = labels_en or []
    _grouped_bars(ax, labels_en, series, pct=pct)
    _style(ax, title, ylabel, labels_en)

    if show_legend:
        handles, lbl = ax.get_legend_handles_labels()
        if handles:
            ax.legend(handles, lbl, fontsize=LEGEND_SIZE, frameon=True)
    _save(fig, out_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def fetch_and_plot(args):
    base    = args.base_url
    ds      = args.date_start
    de      = args.date_end
    df      = args.date_field
    cpvs    = args.cpv_prefixes
    key     = args.api_key
    out_dir = args.output_dir
    os.makedirs(out_dir, exist_ok=True)

    sources = ["insiders", "outsiders", "minors"]

    def fetch_multi(endpoint):
        result = {}
        for src in sources:
            try:
                result[src] = _post(base, endpoint,
                                    _build_payload(ds, de, df, src, cpvs), key)
            except Exception as exc:
                print(f"  WARNING {endpoint}/{src}: {exc}", file=sys.stderr)
                result[src] = None
        return result

    def fetch_one(endpoint, tender_type="insiders"):
        try:
            return _post(base, endpoint,
                         _build_payload(ds, de, df, tender_type, cpvs), key)
        except Exception as exc:
            print(f"  WARNING {endpoint}: {exc}", file=sys.stderr)
            return None

    print("Fetching data from API …")
    d_total     = fetch_multi("indicators/total-procurement")
    d_single    = fetch_multi("indicators/single-bidder")
    d_direct    = fetch_multi("indicators/direct-awards")
    d_ted       = fetch_multi("indicators/ted-publication")
    d_speed     = fetch_one("indicators/decision-speed",    "insiders")
    d_sme_part  = fetch_one("indicators/sme-participation", "insiders")
    d_sme_ratio = fetch_one("indicators/sme-offer-ratio",   "insiders")
    d_lots      = fetch_multi("indicators/lots-division")
    d_miss_sup  = fetch_multi("indicators/missing-supplier-id")
    d_miss_buy  = fetch_multi("indicators/missing-buyer-id")
    print("All data fetched. Generating figures …\n")

    # IO
    plot_ind0(d_total, out_dir)

    # I1
    _plot_dual(
        d_single, "pct_single_bid", "coverage",
        "% lots", "Coverage (%)",
        "Ind. 1a – Single bidder (% lots with one offer)",
        "Ind. 1b – Coverage of offers_received field",
        os.path.join(out_dir, "indicador1.png"), pct=True,
    )

    # I2
    _plot_dual(
        d_direct, "pct_direct", "coverage",
        "% procedures", "Coverage (%)",
        "Ind. 2a – Direct awards (% negotiated w/o publication)",
        "Ind. 2b – Coverage of procedure type field",
        os.path.join(out_dir, "indicador2.png"), pct=True,
    )

    # I3 – TED publication
    _plot_single(
        None, "pct_ted", "% procedures",
        "Ind. 3 – TED publication (% procedures in EU TED)",
        os.path.join(out_dir, "indicador3.png"),
        pct=True, multi_src=True, data_by_src=d_ted,
    )

    # I6
    _plot_dual(
        d_speed, "avg_days", "coverage",
        "Days", "Coverage (%)",
        "Ind. 6a – Decision speed (insiders, open procedures)",
        "Ind. 6b – Coverage of decision speed field",
        os.path.join(out_dir, "indicador6.png"),
        pct=False, multi_src=False,
    )

    # I7
    _plot_dual(
        d_sme_part, "pct_sme", "coverage",
        "% lots", "Coverage (%)",
        "Ind. 7a – SME participation (insiders)",
        "Ind. 7b – Coverage of SME offers field",
        os.path.join(out_dir, "indicador7.png"),
        pct=True, multi_src=False,
    )

    # I8
    _plot_dual(
        d_sme_ratio, "pct_sme_offers", "coverage",
        "% offers", "Coverage (%)",
        "Ind. 8a – SME offer ratio (insiders)",
        "Ind. 8b – Coverage of SME offer ratio field",
        os.path.join(out_dir, "indicador8.png"),
        pct=True, multi_src=False,
    )

    # I9
    _plot_dual(
        d_lots, "pct_multi_lot", "coverage",
        "% procedures", "Coverage (%)",
        "Ind. 9a – Procedures divided into lots (%)",
        "Ind. 9b – Coverage of lots field",
        os.path.join(out_dir, "indicador9.png"), pct=True,
    )

    # I11
    _plot_dual(
        d_miss_sup, "pct_missing", "coverage",
        "% lots", "Coverage (%)",
        "Ind. 11a – Missing supplier ID (% awarded lots)",
        "Ind. 11b – Coverage of supplier ID field",
        os.path.join(out_dir, "indicador11.png"), pct=True,
    )

    # I12
    _plot_dual(
        d_miss_buy, "pct_missing", "coverage",
        "% procedures", "Coverage (%)",
        "Ind. 12a – Missing buyer ID (% procedures w/o buyer identifier)",
        "Ind. 12b – Coverage of buyer ID field",
        os.path.join(out_dir, "indicador12.png"), pct=True,
    )

    print("\nAll figures saved.")

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--api-key", default=None,
                   help="X-API-Key value. Falls back to SIA_API_KEY env var.")
    p.add_argument("--base-url", default="http://kumo01:10083//exploitation")
    p.add_argument("--date-start", default="2025-01-01T00:00:00Z")
    p.add_argument("--date-end",   default="2026-03-01T00:00:00Z")
    p.add_argument("--date-field", default="updated")
    p.add_argument("--cpv-prefixes", nargs="+", default=["48", "72", "73"])
    p.add_argument("--output-dir", default="figures",
                   help="Directory where indicadorN.png files are saved.")
    args = p.parse_args()
    if not args.api_key:
        args.api_key = os.environ.get("SIA_API_KEY")
    return args


if __name__ == "__main__":
    fetch_and_plot(_parse_args())