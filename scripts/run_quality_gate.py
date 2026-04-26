#!/usr/bin/env python3
"""
Rule-based quality gate with pass/fail outputs per source CSV.

Outputs:
- ge/results/<table>_passed.csv
- ge/results/<table>_failed.csv
- ge/results/quality_gate_summary.md
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


ROOT = Path(__file__).resolve().parents[1]
HOSPITAL = ROOT / "DATA" / "Hospital Data"
ICU = ROOT / "DATA" / "ICU DATA"
OUT_DIR = ROOT / "ge" / "results"

CURATED_CREATININE_ITEMIDS = {"50912", "51081", "52024", "52546"}
CURATED_URINE_ITEMIDS = {"226559", "226560", "226561", "226567", "226627", "226631", "227489"}


@dataclass
class RuleResult:
    ok: bool
    reason: str = ""


def _is_float(value: str) -> bool:
    try:
        float(value)
        return True
    except Exception:
        return False


def _check_required(row: dict[str, str], required: list[str]) -> RuleResult:
    missing = [c for c in required if not row.get(c)]
    if missing:
        return RuleResult(False, f"missing_required:{','.join(missing)}")
    return RuleResult(True)


def _row_rule_patients(row: dict[str, str]) -> RuleResult:
    return _check_required(row, ["subject_id"])


def _row_rule_admissions(row: dict[str, str]) -> RuleResult:
    base = _check_required(row, ["subject_id", "hadm_id", "admittime", "dischtime"])
    if not base.ok:
        return base
    if row["admittime"] >= row["dischtime"]:
        return RuleResult(False, "bad_time_order:admittime>=dischtime")
    return RuleResult(True)


def _row_rule_icustays(row: dict[str, str]) -> RuleResult:
    base = _check_required(row, ["subject_id", "hadm_id", "stay_id", "intime", "outtime"])
    if not base.ok:
        return base
    if row["intime"] >= row["outtime"]:
        return RuleResult(False, "bad_time_order:intime>=outtime")
    return RuleResult(True)


def _row_rule_labevents(row: dict[str, str]) -> RuleResult:
    base = _check_required(row, ["subject_id", "itemid", "charttime"])
    if not base.ok:
        return base
    # Apply strict checks only for curated serum creatinine rows.
    itemid = row.get("itemid", "")
    if itemid in CURATED_CREATININE_ITEMIDS:
        strict = _check_required(row, ["hadm_id", "valuenum", "valueuom"])
        if not strict.ok:
            return strict
        if row["valueuom"] != "mg/dL":
            return RuleResult(False, "bad_unit:creatinine_not_mg/dL")
        if not _is_float(row["valuenum"]):
            return RuleResult(False, "bad_type:valuenum_not_numeric")
        value = float(row["valuenum"])
        if value < 0.1 or value > 20.0:
            return RuleResult(False, "out_of_range:creatinine_mg/dL")
    return RuleResult(True)


def _row_rule_outputevents(row: dict[str, str]) -> RuleResult:
    base = _check_required(row, ["subject_id", "hadm_id", "stay_id", "itemid", "charttime", "value"])
    if not base.ok:
        return base
    itemid = row.get("itemid", "")
    if itemid in CURATED_URINE_ITEMIDS:
        if row.get("valueuom") != "mL":
            return RuleResult(False, "bad_unit:urine_not_mL")
        if not _is_float(row["value"]):
            return RuleResult(False, "bad_type:value_not_numeric")
        value = float(row["value"])
        if value < 0.0:
            return RuleResult(False, "out_of_range:urine_negative")
        if value > 10000.0:
            return RuleResult(False, "out_of_range:urine_too_large")
    return RuleResult(True)


def _row_rule_dict(row: dict[str, str]) -> RuleResult:
    return _check_required(row, ["itemid", "label"])


def _process_file(
    file_path: Path,
    table_name: str,
    row_rule: Callable[[dict[str, str]], RuleResult],
    max_rows: int | None = None,
) -> dict[str, int]:
    passed_path = OUT_DIR / f"{table_name}_passed.csv"
    failed_path = OUT_DIR / f"{table_name}_failed.csv"
    reason_counts: dict[str, int] = {}
    processed = 0
    passed = 0
    failed = 0

    with file_path.open(newline="", encoding="utf-8") as src:
        reader = csv.DictReader(src)
        fieldnames = list(reader.fieldnames or [])
        failed_fields = fieldnames + ["failure_reason"]

        with passed_path.open("w", newline="", encoding="utf-8") as p_out, failed_path.open(
            "w", newline="", encoding="utf-8"
        ) as f_out:
            pass_writer = csv.DictWriter(p_out, fieldnames=fieldnames)
            fail_writer = csv.DictWriter(f_out, fieldnames=failed_fields)
            pass_writer.writeheader()
            fail_writer.writeheader()

            for row in reader:
                processed += 1
                result = row_rule(row)
                if result.ok:
                    passed += 1
                    pass_writer.writerow(row)
                else:
                    failed += 1
                    reason_counts[result.reason] = reason_counts.get(result.reason, 0) + 1
                    row["failure_reason"] = result.reason
                    fail_writer.writerow(row)
                if max_rows is not None and processed >= max_rows:
                    break

    summary = {
        "processed": processed,
        "passed": passed,
        "failed": failed,
        "passed_pct": int((passed / processed) * 10000) / 100 if processed else 0,
    }
    for reason, count in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True):
        summary[f"reason:{reason}"] = count
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max_rows_per_table",
        type=int,
        default=None,
        help="Optional cap for smoke testing. If omitted, processes full tables.",
    )
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    table_map: list[tuple[str, Path, Callable[[dict[str, str]], RuleResult]]] = [
        ("patients", HOSPITAL / "patients.csv", _row_rule_patients),
        ("admissions", HOSPITAL / "admissions-2.csv", _row_rule_admissions),
        ("labevents", HOSPITAL / "labevents.csv", _row_rule_labevents),
        ("d_labitems", HOSPITAL / "d_labitems.csv", _row_rule_dict),
        ("icustays", ICU / "icustays.csv", _row_rule_icustays),
        ("outputevents", ICU / "outputevents.csv", _row_rule_outputevents),
        ("d_items", ICU / "d_items.csv", _row_rule_dict),
    ]

    summary_md = OUT_DIR / "quality_gate_summary.md"
    with summary_md.open("w", encoding="utf-8") as out:
        out.write("# Quality Gate Summary\n\n")
        for table_name, path, rule in table_map:
            stats = _process_file(path, table_name, rule, max_rows=args.max_rows_per_table)
            out.write(f"## {table_name}\n")
            out.write(f"- source: `{path}`\n")
            out.write(f"- processed: {stats['processed']}\n")
            out.write(f"- passed: {stats['passed']}\n")
            out.write(f"- failed: {stats['failed']}\n")
            out.write(f"- passed_pct: {stats['passed_pct']}%\n")
            reasons = [k for k in stats if k.startswith("reason:")]
            if reasons:
                out.write("- failure_reasons:\n")
                for key in reasons:
                    out.write(f"  - {key.replace('reason:', '')}: {stats[key]}\n")
            out.write("\n")
            print(f"[{table_name}] processed={stats['processed']} passed={stats['passed']} failed={stats['failed']}")

    print(f"Summary written: {summary_md}")


if __name__ == "__main__":
    main()
