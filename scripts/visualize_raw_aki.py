#!/usr/bin/env python3
"""
Visualize raw MIMIC files and estimate AKI stage distribution from raw events.

Outputs (in docs/reports/raw_visualization by default):
1) raw_file_row_counts.png
2) creatinine_distribution.png
3) aki_stage_distribution.png
4) raw_aki_summary.md

Notes:
- Stage assignment here is a raw-data proxy from serum creatinine values.
- This is NOT a full KDIGO implementation (which needs baseline/time windows).
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path


try:
    import matplotlib.pyplot as plt
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "matplotlib is required. Install it with: pip install matplotlib"
    ) from exc


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HOSPITAL = ROOT / "DATA" / "Hospital Data"
DEFAULT_ICU = ROOT / "DATA" / "ICU DATA"
DEFAULT_OUT = ROOT / "docs" / "reports" / "raw_visualization"

RAW_FILES = {
    "patients": ("hospital", "patients.csv"),
    "admissions": ("hospital", "admissions-2.csv"),
    "labevents": ("hospital", "labevents.csv"),
    "d_labitems": ("hospital", "d_labitems.csv"),
    "icustays": ("icu", "icustays.csv"),
    "outputevents": ("icu", "outputevents.csv"),
    "d_items": ("icu", "d_items.csv"),
}

# Curated in current project config for serum creatinine.
CREATININE_ITEMIDS = {"50912", "51081", "52024", "52546"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Visualize raw files and AKI stage proxy distribution."
    )
    parser.add_argument(
        "--hospital-dir",
        type=Path,
        default=DEFAULT_HOSPITAL,
        help="Path to hospital raw CSV directory.",
    )
    parser.add_argument(
        "--icu-dir",
        type=Path,
        default=DEFAULT_ICU,
        help="Path to ICU raw CSV directory.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT,
        help="Output directory for charts/report.",
    )
    parser.add_argument(
        "--target-per-stage",
        type=int,
        default=0,
        help=(
            "Target record count for each AKI stage (1/2/3). "
            "If 0, uses max observed among stages 1/2/3."
        ),
    )
    return parser.parse_args()


def resolve_paths(hospital_dir: Path, icu_dir: Path) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for name, (kind, filename) in RAW_FILES.items():
        root = hospital_dir if kind == "hospital" else icu_dir
        paths[name] = root / filename
    return paths


def count_rows(path: Path) -> int:
    with path.open("r", newline="", encoding="utf-8") as f:
        next(f, None)  # Skip header
        return sum(1 for _ in f)


def stage_from_creatinine(value_mg_dl: float) -> str:
    """
    Raw-data proxy stage from creatinine value only.
    Full KDIGO requires baseline and timing windows.
    """
    if value_mg_dl >= 3.0:
        return "Stage 3"
    if value_mg_dl >= 2.0:
        return "Stage 2"
    if value_mg_dl >= 1.5:
        return "Stage 1"
    return "No AKI (proxy)"


def collect_creatinine_events(labevents_path: Path) -> tuple[list[float], Counter]:
    values: list[float] = []
    stage_counts: Counter = Counter()

    with labevents_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("itemid") not in CREATININE_ITEMIDS:
                continue

            unit = (row.get("valueuom") or "").strip().lower()
            if unit != "mg/dl":
                continue

            raw = row.get("valuenum")
            if raw is None or raw == "":
                continue

            try:
                val = float(raw)
            except ValueError:
                continue

            if val <= 0:
                continue

            values.append(val)
            stage_counts[stage_from_creatinine(val)] += 1

    return values, stage_counts


def plot_raw_row_counts(row_counts: dict[str, int], out_path: Path) -> None:
    names = list(row_counts.keys())
    values = [row_counts[k] for k in names]
    plt.figure(figsize=(10, 5))
    plt.bar(names, values)
    plt.xticks(rotation=30, ha="right")
    plt.ylabel("Row Count")
    plt.title("Raw File Row Counts")
    plt.tight_layout()
    plt.savefig(out_path, dpi=180)
    plt.close()


def plot_creatinine_distribution(values: list[float], out_path: Path) -> None:
    plt.figure(figsize=(8, 5))
    plt.hist(values, bins=50)
    plt.xlabel("Creatinine (mg/dL)")
    plt.ylabel("Frequency")
    plt.title("Serum Creatinine Distribution (Curated ItemIDs)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=180)
    plt.close()


def plot_stage_distribution(stage_counts: Counter, out_path: Path) -> None:
    order = ["No AKI (proxy)", "Stage 1", "Stage 2", "Stage 3"]
    labels = [s for s in order if s in stage_counts]
    values = [stage_counts[s] for s in labels]

    plt.figure(figsize=(7, 5))
    plt.bar(labels, values)
    plt.ylabel("Record Count")
    plt.title("AKI Stage Distribution (Creatinine Proxy)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=180)
    plt.close()


def compute_stage_gaps(stage_counts: Counter, target_per_stage: int) -> dict[str, int]:
    stages = ["Stage 1", "Stage 2", "Stage 3"]
    observed = {s: stage_counts.get(s, 0) for s in stages}

    if target_per_stage <= 0:
        target_per_stage = max(observed.values()) if observed else 0

    return {s: max(target_per_stage - observed[s], 0) for s in stages}


def write_summary(
    out_path: Path,
    row_counts: dict[str, int],
    stage_counts: Counter,
    stage_gaps: dict[str, int],
    target_per_stage: int,
    creatinine_values_count: int,
) -> None:
    if target_per_stage <= 0:
        target_per_stage = max(stage_counts.get(s, 0) for s in ["Stage 1", "Stage 2", "Stage 3"])

    with out_path.open("w", encoding="utf-8") as f:
        f.write("# Raw Data Visualization + AKI Stage Summary\n\n")
        f.write("## Raw File Row Counts\n")
        for name, count in row_counts.items():
            f.write(f"- `{name}`: {count}\n")

        f.write("\n## AKI Stage Counts (Creatinine Proxy)\n")
        f.write(
            "- Proxy rule: Stage 1 >= 1.5, Stage 2 >= 2.0, Stage 3 >= 3.0 mg/dL "
            "(not full KDIGO).\n"
        )
        f.write(f"- Creatinine records used: {creatinine_values_count}\n")
        for stage in ["No AKI (proxy)", "Stage 1", "Stage 2", "Stage 3"]:
            f.write(f"- `{stage}`: {stage_counts.get(stage, 0)}\n")

        f.write("\n## Additional Records Needed (Stage Balance)\n")
        f.write(
            f"- Target records per AKI stage (1/2/3): {target_per_stage} "
            "(auto = max observed if not provided)\n"
        )
        for stage in ["Stage 1", "Stage 2", "Stage 3"]:
            f.write(f"- `{stage}` additional needed: {stage_gaps[stage]}\n")

        f.write("\n## Generated Plots\n")
        f.write("- `raw_file_row_counts.png`\n")
        f.write("- `creatinine_distribution.png`\n")
        f.write("- `aki_stage_distribution.png`\n")


def main() -> None:
    args = parse_args()
    paths = resolve_paths(args.hospital_dir, args.icu_dir)

    missing = [name for name, p in paths.items() if not p.exists()]
    if missing:
        raise SystemExit(f"Missing required raw files: {missing}")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    row_counts = {name: count_rows(path) for name, path in paths.items()}
    creatinine_values, stage_counts = collect_creatinine_events(paths["labevents"])
    stage_gaps = compute_stage_gaps(stage_counts, args.target_per_stage)

    plot_raw_row_counts(row_counts, args.out_dir / "raw_file_row_counts.png")
    plot_creatinine_distribution(creatinine_values, args.out_dir / "creatinine_distribution.png")
    plot_stage_distribution(stage_counts, args.out_dir / "aki_stage_distribution.png")

    write_summary(
        out_path=args.out_dir / "raw_aki_summary.md",
        row_counts=row_counts,
        stage_counts=stage_counts,
        stage_gaps=stage_gaps,
        target_per_stage=args.target_per_stage,
        creatinine_values_count=len(creatinine_values),
    )

    print(f"Saved outputs to: {args.out_dir}")
    print(" - raw_file_row_counts.png")
    print(" - creatinine_distribution.png")
    print(" - aki_stage_distribution.png")
    print(" - raw_aki_summary.md")


if __name__ == "__main__":
    main()
