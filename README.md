# 🛒 Olist E-Commerce Analysis

End-to-end data pipeline and analytics project built on the Brazilian E-Commerce dataset by Olist.
This project demonstrates production-ready data engineering practices including orchestration, data quality, SQL transformations with dbt, and BI dashboarding with Metabase.

---

## 🏗️ Architecture

Raw CSVs → Ingestion + Quality Checks → DuckDB → dbt Staging → dbt Marts → Metabase Dashboard

| Layer             | Tool                 | Description                                        |
|-------------------|----------------------|----------------------------------------------------|
| **Ingestion**     | Python + DuckDB      | Load raw CSVs into DuckDB with validation          |
| **Quality**       | Custom Python checks | Null, duplicate, and range validations on raw data |
| **Staging**       | dbt views            | Clean and rename raw tables                        |
| **Marts**         | dbt tables           | Business-ready models with joins and metrics       |
| **Orchestration** | pipeline.py          | Single entry point to run the full pipeline        |
| **Dashboard**     | Metabase             | Interactive BI dashboard for business analysis     |

---

## 📊 Dataset

[Brazilian E-Commerce by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 9 tables, ~100k orders from 2016 to 2018.

| Table          | Rows    |
|----------------|---------|
| orders         | 99,441  |
| customers      | 99,441  |
| order_items    | 112,650 |
| order_payments | 103,886 |
| order_reviews  | 99,224  |
| products       | 32,951  |
| sellers        | 3,095   |

---

## 🛠️ Tech Stack

| Tool                  | Purpose                         |
|-----------------------|---------------------------------|
| Python 3.11           | Core language                   |
| DuckDB 1.5            | Local analytical database       |
| dbt-core + dbt-duckdb | SQL transformations and testing |
| pandas                | Data manipulation               |
| Metabase              | BI dashboard                    |

---

## 📁 Project Structure

| Layer             | Tool                 | Description                                        |
|-------------------|----------------------|----------------------------------------------------|
| **Ingestion**     | Python + DuckDB      | Load raw CSVs into DuckDB with validation          |
| **Quality**       | Custom Python checks | Null, duplicate, and range validations on raw data |
| **Staging**       | dbt views            | Clean and rename raw tables                        |
| **Marts**         | dbt tables           | Business-ready models with joins and metrics       |
| **Orchestration** | pipeline.py          | Single entry point to run the full pipeline        |
| **Dashboard**     | Metabase             | Interactive BI dashboard for business analysis     |

---

## 📊 Dataset

[Brazilian E-Commerce by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 9 tables, ~100k orders from 2016 to 2018.

| Table          | Rows    |
|----------------|---------|
| orders         | 99,441  |
| customers      | 99,441  |
| order_items    | 112,650 |
| order_payments | 103,886 |
| order_reviews  | 99,224  |
| products       | 32,951  |
| sellers        | 3,095   |

---

## 🛠️ Tech Stack

| Tool                  | Purpose                         |
|-----------------------|---------------------------------|
| Python 3.11           | Core language                   |
| DuckDB 1.5            | Local analytical database       |
| dbt-core + dbt-duckdb | SQL transformations and testing |
| pandas                | Data manipulation               |
| Metabase              | BI dashboard                    |

---

## 📁 Project Structure

    olist-ecommerce-analysis/
    ├── data/
    │   ├── raw/                  # Original CSV files
    │   └── duckdb/               # DuckDB database (not tracked in git)
    ├── ingestion/
    │   └── load_data.py          # Load CSVs + data quality checks
    ├── dbt_olist/
    │   ├── models/
    │   │   ├── staging/          # Cleaned and renamed raw tables
    │   │   └── marts/            # Business-ready models
    │   └── dbt_project.yml
    ├── analyses/
    │   └── explore.py            # Initial data exploration
    ├── utils/
    │   ├── logger.py             # Centralized logging
    │   └── quality.py           # Reusable quality checks
    ├── pipeline.py               # Orchestrated pipeline entry point
    └── requirements.txt

---

## 🚀 How to Run

### 1. Clone the repository
```bash
git clone git@github.com:DylanRReexx/olist-ecommerce-analysis.git
cd olist-ecommerce-analysis
```

### 2. Set up virtual environment
```bash
python -m venv venv
venv\Scripts\Activate  # Windows
pip install -r requirements.txt
```

### 3. Add raw data
Download the dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place all CSV files in `data/raw/`.

### 4. Run the full pipeline
```bash
python pipeline.py
```

This single command will:
- Load and validate raw data into DuckDB
- Run dbt staging models
- Run dbt data tests
- Run dbt mart models

---

## 🧪 Data Quality

Two layers of validation:

**1. Ingestion checks** (`utils/quality.py`) — validates raw data before loading:
- No nulls in key columns
- No duplicates in primary keys
- Valid value ranges (price > 0, payment_value > 0)

**2. dbt tests** (`models/staging/schema.yml`) — validates transformations:
- Unique and not_null constraints
- Accepted values for categorical columns

---

## 📈 Dashboard

Built with Metabase, the dashboard includes:
- Orders over time (monthly trend)
- Revenue by customer state
- Top 10 product categories by revenue
- Average delivery time by state

---

## 📐 How to Scale

| Current          | Production Scale                            |
|------------------|---------------------------------------------|
| DuckDB local     | BigQuery or Redshift                        |
| pipeline.py      | Apache Airflow or Prefect                   |
| Manual execution | Scheduled runs with cron or cloud scheduler |
| Local Metabase   | Metabase Cloud or Looker                    |
| Single machine   | Distributed processing with Spark           |

---

## 👤 Author

**Dylan** — Systems Engineering Student @ ULATINA  
[GitHub](https://github.com/DylanRReexx)
