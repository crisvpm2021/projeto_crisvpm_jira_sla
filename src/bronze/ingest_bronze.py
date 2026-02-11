# Biblioteca para trabalhar com arquivos JSON
import json

# Biblioteca para pegar data e hora atual
from datetime import datetime

# Biblioteca para trabalhar com caminhos de arquivos de forma segura
from pathlib import Path


# ============================
# 1) DEFININDO CAMINHOS
# ============================

# Descobrimos automaticamente a raiz do projeto
# __file__ é o caminho deste arquivo atual
# parents[2] sobe duas pastas até chegar na raiz do projeto
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Caminho do arquivo JSON original (bruto)
INPUT_FILE = PROJECT_ROOT / "jira_issues_raw.json"

# Caminho onde salvo o arquivo da camada Bronze
BRONZE_OUTPUT = PROJECT_ROOT / "data" / "bronze" / "bronze_issues.json"


# ============================
# 2) FUNÇÃO PARA LER O JSON
# ============================

def read_raw_json(path: Path):
    """
    Lê o arquivo JSON bruto e retorna os dados como dicionário ou lista.
    """

    # Verifica se o arquivo realmente existe
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrad
