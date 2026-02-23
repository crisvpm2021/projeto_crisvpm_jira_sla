import sys
import pandas as pd
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from src.sla_calculation import business_hours_between

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_FILE = PROJECT_ROOT / "data" / "silver" / "silver_issues.parquet"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

GOLD_OUTPUT = GOLD_DIR / "gold_issues.parquet"
REPORT_BY_ANALYST = GOLD_DIR / "gold_sla_by_analyst.csv"
REPORT_BY_ISSUE_TYPE = GOLD_DIR / "gold_sla_by_issue_type.csv"


def main():
    print("=== GOLD BUILD (V2 - business days, no holidays yet) ===")

    df = pd.read_parquet(SILVER_FILE)

    # Keep only completed issues
    df = df[df["status"].isin(["Done", "Resolved"])].copy()

    # Drop rows without timestamps
    df = df.dropna(subset=["created_at", "resolved_at"]).copy()

    # SLA expected hours by priority
    sla_map = {"High": 24, "Medium": 72, "Low": 120}
    df["sla_expected_hours"] = df["priority"].map(sla_map)
    df = df[df["sla_expected_hours"].notna()].copy()

    # Business-hours resolution (Mon-Fri only)
    df["resolution_business_hours"] = df.apply(
        lambda r: business_hours_between(r["created_at"].to_pydatetime(), r["resolved_at"].to_pydatetime()),
        axis=1,
    )

    # Remove inconsistent records
    df = df[df["resolution_business_hours"] >= 0].copy()

    # SLA compliance (business hours)
    df["is_sla_met"] = df["resolution_business_hours"] <= df["sla_expected_hours"]

    gold_cols = [
        "project_name",
        "issue_id",
        "issue_type",
        "assignee_name",
        "priority",
        "status",
        "created_at",
        "resolved_at",
        "resolution_business_hours",
        "sla_expected_hours",
        "is_sla_met",
    ]
    df_gold = df[gold_cols].copy()

    # Basic cleanup for grouping
    df_gold["assignee_name"] = df_gold["assignee_name"].astype(str)
    df_gold["issue_type"] = df_gold["issue_type"].astype(str)

    df_gold = df_gold[
        (~df_gold["assignee_name"].str.startswith("{")) &
        (~df_gold["issue_type"].str.startswith("{"))
    ].copy()

    df_gold.loc[df_gold["assignee_name"].isin(["None", "nan", "NaT"]), "assignee_name"] = "Unassigned"

    # Save outputs
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    df_gold.to_parquet(GOLD_OUTPUT, index=False)

    report_by_analyst = (
        df_gold.groupby("assignee_name")
        .agg(
            issues_total=("issue_id", "count"),
            sla_met_rate=("is_sla_met", "mean"),
            avg_resolution_business_hours=("resolution_business_hours", "mean"),
            avg_sla_expected_hours=("sla_expected_hours", "mean"),
        )
        .reset_index()
        .sort_values(["sla_met_rate", "issues_total"], ascending=[False, False])
    )
    report_by_analyst.to_csv(REPORT_BY_ANALYST, index=False, encoding="utf-8")

    report_by_issue_type = (
        df_gold.groupby("issue_type")
        .agg(
            issues_total=("issue_id", "count"),
            sla_met_rate=("is_sla_met", "mean"),
            avg_resolution_business_hours=("resolution_business_hours", "mean"),
            avg_sla_expected_hours=("sla_expected_hours", "mean"),
        )
        .reset_index()
        .sort_values(["sla_met_rate", "issues_total"], ascending=[False, False])
    )
    report_by_issue_type.to_csv(REPORT_BY_ISSUE_TYPE, index=False, encoding="utf-8")

    print(f" Gold dataset saved: {GOLD_OUTPUT}")
    print(f" Report saved: {REPORT_BY_ANALYST}")
    print(f" Report saved: {REPORT_BY_ISSUE_TYPE}")
    print(f"Gold rows: {len(df_gold)}")
    print("Overall SLA met rate (%):", round(df_gold["is_sla_met"].mean() * 100, 2))


if __name__ == "__main__":
    main()