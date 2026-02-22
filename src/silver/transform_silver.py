import json
from pathlib import Path
import pandas as pd


# ============================
# Caminhos do projeto
# ============================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_FILE = PROJECT_ROOT / "data" / "bronze" / "bronze_issues.json"
SILVER_OUTPUT = PROJECT_ROOT / "data" / "silver" / "silver_issues.parquet"


def main():
    print("=== SILVER TRANSFORM ===")

    # 1) Lê o arquivo Bronze
    with BRONZE_FILE.open("r", encoding="utf-8") as f:
        bronze = json.load(f)

    # 2) Pega o JSON bruto
    raw = bronze["data"]
    project_name = raw.get("project")
    issues = raw["issues"]

    rows = []

    # 3) Transforma cada issue em uma linha
    for issue in issues:
        # timestamps vem como lista com 1 dict:
        # [{"created_at": "...", "resolved_at": "..."}]
        ts_list = issue.get("timestamps", [])

        if len(ts_list) > 0 and isinstance(ts_list[0], dict):
            ts = ts_list[0]
        else:
            ts = {}

        rows.append({
            "project_name": project_name,
            "issue_id": issue.get("id"),
            "issue_type": issue.get("issue_type"),
            "status": issue.get("status"),
            "priority": issue.get("priority"),
            "assignee_name": issue.get("assignee"),
            "created_at": ts.get("created_at"),
            "resolved_at": ts.get("resolved_at"),
        })

    # 4) Cria DataFrame
    df = pd.DataFrame(rows)

    # 5) Converte datas
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df["resolved_at"] = pd.to_datetime(df["resolved_at"], errors="coerce", utc=True)

    # 6) Salva no Silver
    SILVER_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SILVER_OUTPUT, index=False)

    print(f"Silver gerado em: {SILVER_OUTPUT}")
    print("Nulos por coluna:")
    print(df.isna().sum())


if __name__ == "__main__":
    main()