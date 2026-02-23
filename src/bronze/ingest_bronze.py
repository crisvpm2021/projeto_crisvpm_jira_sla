# Standard library for working with JSON files
import json

# Standard library for working with timestamps
from datetime import datetime

# Standard library for handling file system paths safely
from pathlib import Path


# ============================
# 1) DEFINE PROJECT PATHS
# ============================

# Automatically detect project root directory
# __file__ represents the current file path
# parents[2] moves two levels up to reach project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Path to original raw JSON file
INPUT_FILE = PROJECT_ROOT / "jira_issues_raw.json"

# Output path for Bronze layer
BRONZE_OUTPUT = PROJECT_ROOT / "data" / "bronze" / "bronze_issues.json"


# ============================
# 2) READ RAW JSON FUNCTION
# ============================

def read_raw_json(path: Path):
    """
    Read raw JSON file and return parsed content.
    """

    # Ensure file exists before attempting to read
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


# ============================
# 3) WRITE BRONZE FILE
# ============================

def write_bronze(data, output_path: Path):
    """
    Wrap raw data with ingestion metadata and save Bronze dataset.
    """

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    bronze_payload = {
        "ingestion_timestamp_utc": datetime.utcnow().isoformat(),
        "source": "local_file",
        "data": data,
    }

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(bronze_payload, file, ensure_ascii=False, indent=2)


# ============================
# 4) MAIN EXECUTION
# ============================

def main():
    print("=== BRONZE INGESTION STARTED ===")

    raw_data = read_raw_json(INPUT_FILE)
    write_bronze(raw_data, BRONZE_OUTPUT)

    print(f"✅ Bronze file generated at: {BRONZE_OUTPUT}")


if __name__ == "__main__":
    main()