from pathlib import Path
import pandas as pd


# ============================
# Caminhos do projeto
# ============================
PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_FILE = PROJECT_ROOT / "data" / "silver" / "silver_issues.parquet"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

GOLD_OUTPUT = GOLD_DIR / "gold_issues.parquet"
REPORT_BY_ASSIGNEE = GOLD_DIR / "report_sla_by_assignee.csv"
REPORT_BY_TYPE = GOLD_DIR / "report_sla_by_issue_type.csv"


def main():
    print("GOLD BUILD (V1 - horas corridas)")

    # 1) Ler Silver
    df = pd.read_parquet(SILVER_FILE)

    # 2) Filtrar apenas issues finalizadas (regra do projeto)
    df = df[df["status"].isin(["Done", "Resolved"])].copy()

    # 3) Remover linhas sem datas (não dá para calcular SLA)
    df = df.dropna(subset=["created_at", "resolved_at"]).copy()

    # 4) Calcular horas de resolução (corridas)
    df["resolution_hours"] = (df["resolved_at"] - df["created_at"]).dt.total_seconds() / 3600

    # 5) SLA esperado por prioridade
    sla_map = {"High": 24, "Medium": 72, "Low": 120}
    df["sla_expected_hours"] = df["priority"].map(sla_map)

    # 6) Remover linhas com prioridade inválida (fora do SLA)
    df = df[df["sla_expected_hours"].notna()].copy()

    # 7) Remover resoluções negativas (dados inconsistentes)
    df = df[df["resolution_hours"] >= 0].copy()

    # 8) Flag SLA cumprido
    df["is_sla_met"] = df["resolution_hours"] <= df["sla_expected_hours"]

    # 9) Selecionar colunas finais (tabela Gold)
    gold_cols = [
        "project_name",
        "issue_id",
        "issue_type",
        "assignee_name",
        "priority",
        "status",
        "created_at",
        "resolved_at",
        "resolution_hours",
        "sla_expected_hours",
        "is_sla_met",
    ]
    df_gold = df[gold_cols].copy()

    # =================================================
    # Limpeza para evitar "dict/list" e garantir groupby
    # =================================================
    # Converte para string (isso evita erros de unhashable type)
    df_gold["assignee_name"] = df_gold["assignee_name"].astype(str)
    df_gold["issue_type"] = df_gold["issue_type"].astype(str)
    df_gold["priority"] = df_gold["priority"].astype(str)
    df_gold["status"] = df_gold["status"].astype(str)

    # Remove registros "estranhos" (ex.: se vier como dict convertido para string "{...}")
    df_gold = df_gold[
        (~df_gold["assignee_name"].str.startswith("{")) &
        (~df_gold["issue_type"].str.startswith("{"))
    ].copy()

    # Se ficar vazio, coloca "Unassigned"
    df_gold.loc[df_gold["assignee_name"].isin(["None", "nan", "NaT"]), "assignee_name"] = "Unassigned"

    # ============================
    # 10) Salvar tabela Gold
    # ============================
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    df_gold.to_parquet(GOLD_OUTPUT, index=False)

    # ============================
    # 11) Relatórios (CSV)
    # ============================
    # SLA médio por analista
    rep_assignee = (
        df_gold.groupby("assignee_name")
        .agg(
            issues_total=("issue_id", "count"),
            sla_met_rate=("is_sla_met", "mean"),
            avg_resolution_hours=("resolution_hours", "mean"),
            avg_sla_expected_hours=("sla_expected_hours", "mean"),
        )
        .reset_index()
        .sort_values(["sla_met_rate", "issues_total"], ascending=[False, False])
    )
    rep_assignee.to_csv(REPORT_BY_ASSIGNEE, index=False, encoding="utf-8")

    # SLA médio por tipo
    rep_type = (
        df_gold.groupby("issue_type")
        .agg(
            issues_total=("issue_id", "count"),
            sla_met_rate=("is_sla_met", "mean"),
            avg_resolution_hours=("resolution_hours", "mean"),
            avg_sla_expected_hours=("sla_expected_hours", "mean"),
        )
        .reset_index()
        .sort_values(["sla_met_rate", "issues_total"], ascending=[False, False])
    )
    rep_type.to_csv(REPORT_BY_TYPE, index=False, encoding="utf-8")

    # ============================
    # 12) Prints finais
    # ============================
    print(f" Gold criado com sucesso: {GOLD_OUTPUT}")
    print(f" Report por analista: {REPORT_BY_ASSIGNEE}")
    print(f" Report por tipo: {REPORT_BY_TYPE}")
    print(f"Linhas Gold: {len(df_gold)}")
    print("SLA met rate (geral):", round(df_gold["is_sla_met"].mean() * 100, 2), "%")


if __name__ == "__main__":
    main()