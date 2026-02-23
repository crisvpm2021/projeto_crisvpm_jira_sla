import json
from pathlib import Path
import pandas as pd


# ============================
# Project paths
# ============================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_FILE = PROJECT_ROOT / "data" / "bronze" / "bronze_issues.json"
SILVER_OUTPUT = PROJECT_ROOT / "data" / "silver" / "silver_issues.parquet"


def main():
    print("=== SILVER TRANSFORM ===")

    # 1) Read Bronze file
    with BRONZE_FILE.open("r", encoding="utf-8") as f:
        bronze = json.load(f)

    # 2) Extract raw payload
    raw = bronze["data"]
    project_name = raw.get("project")
    issues = raw["issues"]

    rows = []

    # 3) Transform each issue into one row
    for issue in issues:
        # timestamps is a list with a single dict:
        # [{"created_at": "...", "resolved_at": "..."}]
        ts_list = issue.get("timestamps", [])

        if len(ts_list) > 0 and isinstance(ts_list[0], dict):
            ts = ts_list[0]
        else:
            ts = {}

        rows.append(
            {
                "project_name": project_name,
                "issue_id": issue.get("id"),
                "issue_type": issue.get("issue_type"),
                "status": issue.get("status"),
                "priority": issue.get("priority"),
                "assignee_name": issue.get("assignee"),
                "created_at": ts.get("created_at"),
                "resolved_at": ts.get("resolved_at"),
            }
        )

    # 4) Build DataFrame
    df = pd.DataFrame(rows)

    # 5) Convert timestamps to UTC datetime
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df["resolved_at"] = pd.to_datetime(df["resolved_at"], errors="coerce", utc=True)

    # 6) Save Silver dataset
    SILVER_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SILVER_OUTPUT, index=False)

    print(f"✅ Silver generated at: {SILVER_OUTPUT}")
    print("Nulls per column:")
    print(df.isna().sum())


if __name__ == "__main__":
    main()