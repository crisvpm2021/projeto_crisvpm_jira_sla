\# Python Data Engineering Challenge – JIRA SLA Pipeline



\## Overview



This project implements a complete data pipeline using the Medallion Architecture (Bronze → Silver → Gold) to calculate SLA compliance for JIRA issues.



The pipeline:



\- Ingests raw JSON data

\- Normalizes nested structures

\- Calculates SLA metrics

\- Applies business-day logic

\- Excludes Brazilian national holidays via public API

\- Generates analytical reports



---



\## Architecture



The project follows the Medallion Architecture:



```

project-root/

│

├── data/

│   ├── bronze/

│   ├── silver/

│   └── gold/

│

├── src/

│   ├── bronze/

│   │   └── ingest\_bronze.py

│   ├── silver/

│   │   └── transform\_silver.py

│   ├── gold/

│   │   └── build\_gold.py

│   └── sla\_calculation.py

│

├── requirements.txt

├── README.md

└── .gitignore

```





---



\## Bronze Layer



\*\*File:\*\* `ingest\_bronze.py`



\- Reads raw JSON (`jira\_issues\_raw.json`)

\- Wraps it with ingestion metadata

\- Stores output as: data/bronze/bronze\_issues.json



Purpose:

\- Preserve raw source data

\- Maintain traceability





---



\## Silver Layer



\*\*File:\*\* `transform\_silver.py`



\- Extracts nested JSON fields

\- Normalizes structure

\- Converts timestamps to UTC datetime

\- Produces structured dataset: data/silver/silver\_issues.parquet





Columns include:



\- issue\_id

\- issue\_type

\- priority

\- status

\- assignee\_name

\- created\_at

\- resolved\_at



Purpose:

\- Structured, analysis-ready data



---



\##  Gold Layer



\*\*File:\*\* `build\_gold.py`



\### SLA Logic



SLA is calculated based on:



| Priority | Expected SLA (hours) |

|----------|----------------------|

| High     | 24                   |

| Medium   | 72                   |

| Low      | 120                  |



\### Business Rules



\- Only `Done` and `Resolved` issues are considered

\- Resolution time is calculated in:

&nbsp; - Business days only (Mon–Fri)

&nbsp; - Excluding Brazilian national holidays

\- Holidays are retrieved via public API: https://brasilapi.com.br/api/feriados/v1/{year}





\- All timestamps handled in UTC (ISO 8601)



---



\## Gold Outputs



\### Main Dataset

data/gold/gold\_issues.parquet





Columns:



\- issue\_id

\- issue\_type

\- assignee\_name

\- priority

\- created\_at

\- resolved\_at

\- resolution\_business\_hours

\- sla\_expected\_hours

\- is\_sla\_met



---



\### Analytical Reports

data/gold/gold\_sla\_by\_analyst.csv

data/gold/gold\_sla\_by\_issue\_type.csv





These reports include:



\- Total issues

\- SLA compliance rate

\- Average resolution time

\- Average expected SLA



---



\##  How to Run



\### 1 Create virtual environment

python -m venv .venv





\### 2 Install dependencies

pip install -r requirements.txt



\### 3 Execute pipeline

\### Bronze

```

python src/bronze/ingest\_bronze.py

```



\### Silver

```

python src/silver/transform\_silver.py

```



\### Gold

```

python src/gold/build\_gold.py

```



---



\##  Design Decisions



\- Medallion architecture for clear separation of responsibilities

\- Modular SLA logic (`sla\_calculation.py`)

\- Only standard library + pandas used

\- Holiday API integrated using `urllib` (native Python)

\- UTC-based timestamp handling

\- Clean commit history

\- Business-rule filtering applied at Gold layer

\- SLA calculation implemented using incremental day slicing approach

\- Holiday set cached per execution to avoid redundant API calls



---



\##  Final Result



The pipeline:



\- Processes approximately 1000 issues

\- Applies SLA business logic correctly

\- Excludes weekends and national holidays

\- Produces structured analytical outputs

\- Is fully reproducible via GitHub







\## Author



Project developed as part of the Python Data Engineering Challenge – JIRA.


\*\*Cristiane Vieira Proença Miamoto\*\*  

Data Engineer | Python | Data Pipelines



