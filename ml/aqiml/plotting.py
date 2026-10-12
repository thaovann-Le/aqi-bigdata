"""Shared figure style so every chart in the report looks the same."""

from __future__ import annotations

import sys

import matplotlib.pyplot as plt

from .config import FIGURE_DIR, ensure_output_dirs

CITY_COLORS = {"hanoi": "#C0392B", "hcm": "#2471A3"}
CITY_LABELS = {"hanoi": "Hà Nội", "hcm": "TP.HCM"}
MODEL_COLORS = {"actual": "#222222", "xgboost": "#D35400", "persistence": "#7F8C8D", "cams": "#27AE60"}

plt.rcParams.update(
    {
        "figure.dpi": 110,
        "savefig.dpi": 160,
        "savefig.bbox": "tight",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.3,
        "axes.titleweight": "bold",
        "legend.frameon": False,
    }
)


def _in_notebook() -> bool:
    return "ipykernel" in sys.modules


def save(fig: plt.Figure, name: str) -> None:
    """Save to FIGURE_DIR/<name>.png; close the figure when running as a script."""
    ensure_output_dirs()
    fig.savefig(FIGURE_DIR / f"{name}.png")
    if not _in_notebook():
        plt.close(fig)
