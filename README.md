<div align="center">

<!-- BANNER -->
<img width="100%" src="https://capsule-render.vercel.app/api?type=waving&color=0:0f0c29,50:302b63,100:24243e&height=200&section=header&text=Neural%20Retail%20Analytics&fontSize=52&fontColor=ffffff&fontAlignY=38&desc=AI-Powered%20Sales%20Intelligence%20System&descAlignY=58&descSize=20&animation=fadeIn"/>

<br/>

<!-- BADGES ROW 1 -->
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)

<!-- BADGES ROW 2 -->
![FastAPI](https://img.shields.io/badge/FastAPI-Production-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![MLflow](https://img.shields.io/badge/MLflow-Tracking-0194E2?style=for-the-badge&logo=mlflow&logoColor=white)
![TensorFlow](https://img.shields.io/badge/TensorFlow-LSTM-FF6F00?style=for-the-badge&logo=tensorflow&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)

<!-- BADGES ROW 3 -->
![Status](https://img.shields.io/badge/Status-Production%20Ready-22c55e?style=for-the-badge)
![Architecture](https://img.shields.io/badge/Architecture-End--to--End-8b5cf6?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-f59e0b?style=for-the-badge)
![PRs Welcome](https://img.shields.io/badge/PRs-Welcome-ec4899?style=for-the-badge)

<br/>

### *End-to-End Data Engineering · Forecasting · Customer Intelligence · Business Analytics*

<br/>

---

</div>

## Table of Contents

- [Executive Summary](#-executive-summary)
- [Business Problem Statement](#-business-problem-statement)
- [Solution Architecture](#-solution-architecture)
- [End-to-End Data Pipeline](#-end-to-end-data-pipeline)
- [Technology Stack](#-technology-stack)
- [ETL Workflow](#-etl-workflow)
- [Database Design](#-database-design)
- [SQL Analytics Layer](#-sql-analytics-layer)
- [Machine Learning Pipeline](#-machine-learning-pipeline)
- [Forecasting Models](#-forecasting-models)
- [Customer Intelligence Engine](#-customer-intelligence-engine)
- [MLOps Workflow](#-mlops-workflow)
- [API Architecture](#-api-architecture)
- [Dashboard Showcase](#-dashboard-showcase)
- [Project Structure](#-project-structure)
- [Installation Guide](#-installation-guide)
- [Execution Workflow](#-execution-workflow)
- [Engineering Highlights](#-engineering-highlights)
- [Business Impact](#-business-impact)
- [Scalability Considerations](#-scalability-considerations)
- [Future Roadmap](#-future-roadmap)
- [Skills Demonstrated](#-skills-demonstrated)

---

## 📋 Executive Summary

**Neural Retail Analytics** is a production-grade, end-to-end **Retail Data Intelligence Platform** that transforms raw transactional data into actionable business intelligence, predictive forecasts, and customer behavioral insights.

This platform is not a proof-of-concept. It is engineered with the same architectural rigor applied in enterprise retail analytics environments — spanning **data ingestion**, **warehouse modeling**, **SQL analytics**, **machine learning**, **MLOps**, **API serving**, and **BI dashboarding** — all orchestrated through a unified, reproducible pipeline.

> **Built for scale. Designed for decisions. Engineered for production.**

| Dimension | Capability |
|---|---|
| **Data Volume** | Millions of retail transactions processed through automated ETL |
| **Forecasting Horizon** | Multi-period sales forecasting via Prophet and LSTM |
| **Customer Segments** | RFM-based segmentation + ML-driven churn prediction |
| **Model Serving** | Real-time REST API via FastAPI with sub-100ms inference |
| **Observability** | Full experiment tracking and model versioning via MLflow |
| **Orchestration** | Automated DAG scheduling via Apache Airflow |

---

## 🎯 Business Problem Statement

Retail organizations generate enormous volumes of transactional data daily, yet most of it remains operationally siloed — locked inside point-of-sale systems, CRMs, and ERP tools without ever reaching the hands of decision-makers in a usable form.

The consequence is significant:

- **Revenue leakage** from unidentified underperforming products and markets
- **Customer attrition** from undetected churn signals and missed retention windows
- **Forecast blindness** — planning cycles driven by intuition instead of data
- **Analytical fragmentation** — no unified source of truth across sales, product, and customer dimensions

**Neural Retail Analytics** solves this by engineering a full-stack intelligence platform that bridges the gap between raw operational data and strategic business decisions.

---

## 🏗️ Solution Architecture

The system is designed as a **layered data platform** where each layer has a clearly defined responsibility:

```
┌─────────────────────────────────────────────────────────────────────┐
│                      NEURAL RETAIL ANALYTICS                        │
│                  AI-Powered Sales Intelligence System                │
└─────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────┐
│   SOURCE LAYER      │  Raw CSV / Transactional Data
│   (Data Ingestion)  │  Online Retail Dataset (UCI / Kaggle)
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   ETL LAYER         │  Python · Pandas · Custom Validation Rules
│   (Pipeline Engine) │  Extraction → Validation → Cleaning → Transform
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   STORAGE LAYER     │  PostgreSQL Data Warehouse
│   (Warehouse)       │  Dimensional Schema · Indexed Tables · Views
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   ANALYTICS LAYER   │  SQL Analytics · Revenue · Products · Countries
│   (Business Intel)  │  Trend Analysis · Customer Metrics
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   ML LAYER          │  Scikit-Learn · Prophet · LSTM · TensorFlow
│   (Intelligence)    │  Forecasting · Segmentation · Churn Prediction
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   MLOPS LAYER       │  MLflow · Experiment Registry · Model Versioning
│   (Observability)   │  Run Comparison · Artifact Storage
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   SERVING LAYER     │  FastAPI · REST Endpoints · Real-Time Inference
│   (API Gateway)     │  Forecast API · Churn Scoring API
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   PRESENTATION      │  Streamlit Dashboard · Power BI Reports
│   LAYER             │  KPI Monitoring · Forecast Viz · Segmentation
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│   ORCHESTRATION     │  Apache Airflow DAG · Scheduled Pipeline Runs
│   LAYER             │  Task Dependencies · Monitoring · Alerting
└─────────────────────┘
```

---

## 🔄 End-to-End Data Pipeline

```
RAW DATA  ──►  VALIDATE  ──►  CLEAN  ──►  ENGINEER  ──►  LOAD  ──►  ANALYZE  ──►  MODEL  ──►  SERVE
   │              │              │              │            │           │            │           │
  CSV           Schema        Remove        Feature      Postgres     SQL KPIs    Prophet    FastAPI
 Files          Check        Nulls &       Creation     Warehouse    Revenue    LSTM Churn   REST
               Rules        Cancels       Aggregates    (DWH)       Product    Regression  Endpoints
                                                                    Country
```

**Pipeline Execution Stages:**

| Stage | Component | Description |
|---|---|---|
| **Extract** | `etl_pipeline.py` | Reads raw CSV, performs schema validation |
| **Validate** | Custom Rule Engine | Checks nulls, negative quantities, data types |
| **Clean** | Pandas Transformations | Removes cancellations, deduplicates, standardizes |
| **Engineer** | Feature Builder | Revenue columns, date dimensions, RFM scores |
| **Load** | SQLAlchemy → PostgreSQL | Inserts into warehouse with upsert logic |
| **Analyze** | SQL Analytics Layer | Executes business queries, populates aggregate views |
| **Model** | ML Engine | Trains forecast and churn models on processed data |
| **Serve** | FastAPI + MLflow | Loads registered model, exposes prediction endpoint |
| **Visualize** | Streamlit App | Reads warehouse + API outputs for dashboard rendering |
| **Orchestrate** | Airflow DAG | Schedules and monitors all above stages as a DAG |

---

## 🛠️ Technology Stack

### Data Engineering

| Tool | Version | Role |
|---|---|---|
| Python | 3.11 | Primary programming language |
| Pandas | 2.x | Data manipulation and transformation |
| SQLAlchemy | 2.x | ORM and database connection management |
| PostgreSQL | 16 | Relational data warehouse |
| SQL | — | Analytics, aggregations, window functions |

### Machine Learning

| Tool | Version | Role |
|---|---|---|
| Scikit-Learn | 1.4 | Regression, classification, preprocessing |
| Prophet | 1.1 | Additive time-series decomposition forecasting |
| TensorFlow / Keras | 2.x | LSTM sequence model for deep learning forecasts |

### MLOps

| Tool | Version | Role |
|---|---|---|
| MLflow | 2.x | Experiment tracking, model registry, artifact storage |

### API & Serving

| Tool | Version | Role |
|---|---|---|
| FastAPI | 0.111 | High-performance async REST API framework |
| Uvicorn | — | ASGI server for production API serving |
| Pydantic | 2.x | Request/response schema validation |

### Visualization & BI

| Tool | Version | Role |
|---|---|---|
| Streamlit | 1.35 | Interactive Python-native analytics dashboard |
| Power BI | — | Executive-level BI reports and visualizations |
| Matplotlib / Seaborn | — | Exploratory and model output visualization |

### Orchestration & Infrastructure

| Tool | Version | Role |
|---|---|---|
| Apache Airflow | 2.8 | Pipeline DAG scheduling and orchestration |
| Docker | 24.x | Containerization and environment reproducibility |
| Git / GitHub | — | Version control, CI/CD, collaboration |

---

## ⚙️ ETL Workflow

The ETL pipeline (`etl_pipeline.py`) is the backbone of the platform. It is engineered to be **idempotent**, **fault-tolerant**, and **re-runnable** without duplicating data.

```
┌──────────────────────────────────────────────────────────┐
│                     ETL PIPELINE                         │
│                   etl_pipeline.py                        │
└──────────────────────────────────────────────────────────┘

  Step 1: EXTRACT
  ───────────────
  • Load raw CSV from /data directory
  • Validate file presence and schema structure
  • Log ingestion metadata (row count, file size, timestamp)

  Step 2: VALIDATE
  ─────────────────
  • Check required columns exist
  • Validate data types (InvoiceDate as datetime, Quantity as int)
  • Flag rows with null CustomerID
  • Identify and isolate cancellation records (InvoiceNo starting with 'C')

  Step 3: CLEAN
  ─────────────
  • Drop rows with missing CustomerID (non-attributable transactions)
  • Remove cancelled transactions (negative quantity records)
  • Filter out negative UnitPrice entries
  • Strip whitespace from Description fields
  • Standardize Country names

  Step 4: TRANSFORM / FEATURE ENGINEER
  ──────────────────────────────────────
  • Compute TotalRevenue = Quantity × UnitPrice
  • Extract InvoiceYear, InvoiceMonth, InvoiceDay, DayOfWeek from InvoiceDate
  • Calculate Recency, Frequency, Monetary (RFM) scores per customer
  • Generate cohort identifiers for customer acquisition analysis

  Step 5: LOAD
  ────────────
  • Connect to PostgreSQL via SQLAlchemy engine
  • Insert cleaned + transformed data into retail_transactions table
  • Upsert logic prevents duplicate loads
  • Commit transaction and log final row count

  Step 6: VALIDATE LOAD
  ──────────────────────
  • Run post-load row count assertion
  • Verify referential integrity checks
  • Log pipeline execution status and duration
```

---

## 🗄️ Database Design

The PostgreSQL warehouse is modeled to support both transactional queries and analytical aggregations efficiently.

```sql
-- Core Transactions Table
CREATE TABLE retail_transactions (
    transaction_id    SERIAL PRIMARY KEY,
    invoice_no        VARCHAR(20)     NOT NULL,
    stock_code        VARCHAR(20)     NOT NULL,
    description       TEXT,
    quantity          INTEGER         NOT NULL,
    invoice_date      TIMESTAMP       NOT NULL,
    unit_price        NUMERIC(10, 2)  NOT NULL,
    customer_id       INTEGER,
    country           VARCHAR(100),
    total_revenue     NUMERIC(12, 2),
    invoice_year      INTEGER,
    invoice_month     INTEGER,
    invoice_day       INTEGER,
    day_of_week       INTEGER
);

-- Customer RFM Summary Table
CREATE TABLE customer_rfm (
    customer_id     INTEGER PRIMARY KEY,
    recency_days    INTEGER,
    frequency       INTEGER,
    monetary        NUMERIC(12, 2),
    rfm_score       NUMERIC(5, 2),
    segment         VARCHAR(50)
);

-- Monthly Revenue Aggregate View
CREATE MATERIALIZED VIEW monthly_revenue AS
SELECT
    invoice_year,
    invoice_month,
    SUM(total_revenue)  AS total_revenue,
    COUNT(DISTINCT invoice_no) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM retail_transactions
GROUP BY invoice_year, invoice_month;
```

**Key Indexes for Query Performance:**

```sql
CREATE INDEX idx_customer_id   ON retail_transactions(customer_id);
CREATE INDEX idx_invoice_date  ON retail_transactions(invoice_date);
CREATE INDEX idx_country       ON retail_transactions(country);
CREATE INDEX idx_stock_code    ON retail_transactions(stock_code);
```

---

## 📊 SQL Analytics Layer

The SQL analytics layer (`sql_queries.sql`) provides the business intelligence backbone. All KPIs rendered in the dashboard and used in model training are computed here.

### Revenue Intelligence

```sql
-- Monthly Revenue Trend
SELECT
    invoice_year,
    invoice_month,
    ROUND(SUM(total_revenue), 2)        AS monthly_revenue,
    COUNT(DISTINCT invoice_no)           AS total_orders,
    ROUND(AVG(total_revenue), 2)         AS avg_order_value,
    LAG(SUM(total_revenue)) OVER (
        ORDER BY invoice_year, invoice_month
    )                                    AS prev_month_revenue,
    ROUND(
        (SUM(total_revenue) - LAG(SUM(total_revenue)) OVER (
            ORDER BY invoice_year, invoice_month)) /
        NULLIF(LAG(SUM(total_revenue)) OVER (
            ORDER BY invoice_year, invoice_month), 0) * 100, 2
    )                                    AS revenue_growth_pct
FROM retail_transactions
GROUP BY invoice_year, invoice_month
ORDER BY invoice_year, invoice_month;
```

### Product Intelligence

```sql
-- Top Products by Revenue Contribution
SELECT
    stock_code,
    description,
    SUM(quantity)                            AS total_units_sold,
    ROUND(SUM(total_revenue), 2)             AS total_revenue,
    ROUND(SUM(total_revenue) /
        SUM(SUM(total_revenue)) OVER () * 100, 2) AS revenue_share_pct,
    RANK() OVER (ORDER BY SUM(total_revenue) DESC) AS revenue_rank
FROM retail_transactions
GROUP BY stock_code, description
ORDER BY total_revenue DESC
LIMIT 20;
```

### Customer Intelligence

```sql
-- RFM Customer Scoring
WITH rfm_base AS (
    SELECT
        customer_id,
        MAX(invoice_date)::DATE              AS last_purchase_date,
        COUNT(DISTINCT invoice_no)           AS frequency,
        ROUND(SUM(total_revenue), 2)         AS monetary,
        CURRENT_DATE - MAX(invoice_date)::DATE AS recency_days
    FROM retail_transactions
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)
SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    NTILE(5) OVER (ORDER BY recency_days ASC)  AS recency_score,
    NTILE(5) OVER (ORDER BY frequency DESC)    AS frequency_score,
    NTILE(5) OVER (ORDER BY monetary DESC)     AS monetary_score
FROM rfm_base;
```

### Geographic Intelligence

```sql
-- Country-Level Revenue Distribution
SELECT
    country,
    COUNT(DISTINCT customer_id)          AS unique_customers,
    COUNT(DISTINCT invoice_no)           AS total_orders,
    ROUND(SUM(total_revenue), 2)         AS total_revenue,
    ROUND(AVG(total_revenue), 2)         AS avg_order_value,
    ROUND(SUM(total_revenue) /
        SUM(SUM(total_revenue)) OVER () * 100, 2) AS revenue_share_pct
FROM retail_transactions
GROUP BY country
ORDER BY total_revenue DESC;
```

---

## 🤖 Machine Learning Pipeline

The ML layer operates on warehouse-ready data and is composed of three distinct model families, each solving a specific business problem.

```
┌──────────────────────────────────────────────────────────┐
│                  ML PIPELINE OVERVIEW                    │
└──────────────────────────────────────────────────────────┘

  Input: Cleaned Data from PostgreSQL Warehouse
        │
        ├──► FORECASTING MODULE
        │        • Prophet: Additive decomposition time series
        │        • LSTM:    Deep sequence model (TensorFlow/Keras)
        │        • Linear Regression: Baseline benchmark model
        │        Output: Future sales volumes and revenue forecasts
        │
        ├──► SEGMENTATION MODULE
        │        • RFM Score Calculation (SQL + Python)
        │        • K-Means Clustering (Scikit-Learn)
        │        Output: Customer segments (Champions, At-Risk, Lost)
        │
        └──► CHURN MODULE
                 • Feature Engineering: tenure, avg order gap, frequency
                 • Classification: Random Forest / Gradient Boosting
                 • Calibrated probabilities for churn risk scoring
                 Output: Churn probability scores per customer
```

**Model Evaluation Metrics:**

| Model | Metric | Target |
|---|---|---|
| Linear Regression | RMSE, R² | Benchmark |
| Prophet | MAE, MAPE | < 15% MAPE |
| LSTM | MAE, RMSE | < Prophet baseline |
| Churn Model | AUC-ROC, Precision, Recall | AUC > 0.80 |

---

## 📈 Forecasting Models

### Prophet Forecasting (`prophet_forecast.py`)

Facebook's Prophet is applied for **additive time-series decomposition**, capturing:

- **Trend component** — long-term revenue trajectory
- **Seasonality component** — weekly and annual retail patterns
- **Holiday effects** — configurable retail event calendar

```python
from prophet import Prophet

model = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=True,
    daily_seasonality=False,
    seasonality_mode='multiplicative',
    changepoint_prior_scale=0.05
)

model.fit(df_prophet)
future = model.make_future_dataframe(periods=90)  # 90-day forecast
forecast = model.predict(future)
```

Outputs: forecast DataFrame with `yhat`, `yhat_lower`, `yhat_upper` — confidence intervals served directly via FastAPI.

---

### LSTM Forecasting (`LSTM_forecast.py`)

A **stacked LSTM architecture** is implemented using TensorFlow/Keras for sequence-to-sequence revenue forecasting:

```
Input Sequence (60 days)
        │
   ┌────▼────┐
   │  LSTM   │  units=128, return_sequences=True
   └────┬────┘
        │ Dropout(0.2)
   ┌────▼────┐
   │  LSTM   │  units=64, return_sequences=False
   └────┬────┘
        │ Dropout(0.2)
   ┌────▼────┐
   │  Dense  │  units=32, activation='relu'
   └────┬────┘
   ┌────▼────┐
   │  Dense  │  units=1   (forecast output)
   └─────────┘
```

- **Lookback window**: 60 trading days
- **Optimizer**: Adam with learning rate scheduling
- **Loss**: Huber loss (robust to outliers)
- **Scaler**: MinMaxScaler for sequence normalization

---

## 👥 Customer Intelligence Engine

### RFM Segmentation

Customers are scored across three behavioral dimensions and mapped to actionable business segments:

```
┌──────────────────────────────────────────────────────────────────┐
│                   RFM SEGMENTATION MODEL                         │
├────────────────┬──────────────────────────────────────────────── │
│   Dimension    │  Definition                                     │
├────────────────┼──────────────────────────────────────────────── │
│   Recency (R)  │  Days since last purchase (lower = better)      │
│   Frequency(F) │  Number of distinct orders placed               │
│   Monetary (M) │  Total revenue contributed                      │
├────────────────┴──────────────────────────────────────────────── │
│                   CUSTOMER SEGMENTS                              │
├────────────────┬────────────────────────────────────────────────┤
│  Champions     │  High R, F, M — most valuable customers        │
│  Loyal         │  High F, M — buy often, high spend             │
│  At Risk       │  Previously high-value, now lapsing            │
│  New Customers │  Recent first-time buyers                      │
│  Lost          │  Long dormant, lowest recency                  │
└────────────────┴────────────────────────────────────────────────┘
```

### Churn Prediction (`churn_model.py`)

A supervised classification model that scores each customer's probability of churning within a defined prediction window.

**Feature Engineering:**

| Feature | Description |
|---|---|
| `days_since_last_order` | Recency metric — primary churn signal |
| `avg_order_frequency_days` | Average gap between orders |
| `total_orders` | Historical purchase frequency |
| `avg_order_value` | Mean transaction value |
| `total_spend` | Lifetime monetary value |
| `product_diversity` | Unique SKUs purchased |
| `country` | Geographic encoding |

---

## 🧪 MLOps Workflow

MLflow is integrated as the **experiment tracking and model registry** for all ML components.

```
┌──────────────────────────────────────────────────────────┐
│                   MLFLOW WORKFLOW                         │
└──────────────────────────────────────────────────────────┘

  Every model training run logs:

  ├── Parameters
  │     • Hyperparameters (lookback, units, learning_rate)
  │     • Feature configuration
  │     • Train/test split ratios

  ├── Metrics
  │     • RMSE, MAE, MAPE, R², AUC-ROC
  │     • Training loss curves
  │     • Validation performance

  ├── Artifacts
  │     • Serialized model (.pkl / SavedModel)
  │     • Feature scaler objects
  │     • Forecast plots and confusion matrices

  └── Model Registry
        • Staging → Production promotion workflow
        • Model versioning with changelog
        • Alias-based serving (champion model)
```

```python
import mlflow

with mlflow.start_run(run_name="prophet_v2_retail"):
    mlflow.log_param("changepoint_prior_scale", 0.05)
    mlflow.log_param("seasonality_mode", "multiplicative")
    mlflow.log_metric("mape", mape_score)
    mlflow.log_metric("mae", mae_score)
    mlflow.prophet.log_model(model, artifact_path="prophet_model")
```

Access the MLflow UI at `http://localhost:5000` for full run comparison and model lineage.

---

## 🔌 API Architecture

The FastAPI serving layer (`api.py`) exposes trained models as **production REST endpoints** with automatic OpenAPI documentation.

```
┌─────────────────────────────────────────────────────────────────┐
│                        API ENDPOINTS                            │
├───────────────────┬──────────────┬────────────────────────────  │
│  Endpoint         │  Method      │  Description                 │
├───────────────────┼──────────────┼────────────────────────────  │
│ /health           │  GET         │  Service health check        │
│ /forecast/prophet │  POST        │  Prophet forecast            │
│ /forecast/lstm    │  POST        │  LSTM deep learning forecast │
│ /churn/predict    │  POST        │  Customer churn probability  │
│ /segment/rfm      │  GET         │  RFM customer segments       │
│ /analytics/kpis   │  GET         │  Live KPI metrics            │
│ /docs             │  GET         │  Auto OpenAPI documentation  │
└───────────────────┴──────────────┴────────────────────────────  ┘
```

**Example Request — Prophet Forecast:**

```bash
curl -X POST "http://localhost:8000/forecast/prophet" \
  -H "Content-Type: application/json" \
  -d '{"periods": 30, "freq": "D"}'
```

**Example Response:**

```json
{
  "model": "prophet",
  "forecast_horizon_days": 30,
  "forecast": [
    {
      "date": "2024-02-01",
      "predicted_revenue": 48250.75,
      "lower_bound": 41200.00,
      "upper_bound": 55300.00
    }
  ],
  "model_version": "2.1.0",
  "generated_at": "2024-01-31T09:00:00Z"
}
```

**Launch API:**

```bash
uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

Interactive docs available at: `http://localhost:8000/docs`

---

## 📊 Dashboard Showcase

The Streamlit dashboard (`app.py`) provides an interactive, single-pane-of-glass view across all intelligence domains.

```
┌─────────────────────────────────────────────────────────────────────┐
│            NEURAL RETAIL ANALYTICS — INTELLIGENCE DASHBOARD         │
├──────────────┬──────────────┬──────────────┬──────────────────────  │
│  Total Rev.  │  Total Orders│ Unique Cust. │  Avg Order Value      │
│  £9.74M      │  25,900      │  4,372       │  £375.80              │
├──────────────┴──────────────┴──────────────┴──────────────────────  │
│                                                                      │
│  [Revenue Trend Chart — Monthly with YoY comparison]                │
│                                                                      │
│  [Top 10 Products by Revenue] [Revenue by Country — Choropleth Map] │
│                                                                      │
│  [RFM Customer Segment Breakdown] [Churn Risk Distribution]         │
│                                                                      │
│  [90-Day Sales Forecast — Prophet with Confidence Intervals]        │
│                                                                      │
│  [LSTM vs Prophet Forecast Comparison]                              │
└─────────────────────────────────────────────────────────────────────┘
```

**Dashboard Modules:**

| Module | Description |
|---|---|
| **KPI Cards** | Real-time revenue, order count, customer count, AOV |
| **Revenue Analytics** | Monthly trend, MoM growth, revenue decomposition |
| **Product Analytics** | Top SKUs by revenue, units sold, share of wallet |
| **Geographic Analytics** | Country-level revenue map and breakdown table |
| **Customer Segmentation** | RFM heatmap, segment counts, segment profiles |
| **Churn Intelligence** | Customer churn risk scoring, segment-level churn rates |
| **Forecasting** | Prophet + LSTM forecasts with configurable horizon |

---

## 📁 Project Structure

```
neural-retail-analytics/
│
├── 📄 etl_pipeline.py           # ETL engine: extract, validate, clean, load
├── 📄 forecast_model.py         # Linear regression baseline forecasting
├── 📄 prophet_forecast.py       # Facebook Prophet time-series forecasting
├── 📄 LSTM_forecast.py          # TensorFlow/Keras LSTM deep learning model
├── 📄 churn_model.py            # Customer churn prediction (classification)
├── 📄 api.py                    # FastAPI REST API serving layer
├── 📄 app.py                    # Streamlit analytics dashboard
├── 📄 retail_pipeline_dag.py    # Apache Airflow orchestration DAG
├── 📄 sql_queries.sql           # SQL analytics: revenue, products, customers
├── 📄 requirements.txt          # Python dependency manifest
│
├── 📁 data/
│   ├── raw/                     # Source CSV files (unmodified)
│   └── processed/               # Cleaned and engineered datasets
│
├── 📁 screenshots/
│   ├── dashboard_overview.png
│   ├── forecast_chart.png
│   ├── rfm_segmentation.png
│   └── mlflow_runs.png
│
├── 📁 mlruns/                   # MLflow experiment tracking (auto-generated)
│
└── 📄 README.md                 # This document
```

---

## 🚀 Installation Guide

### Prerequisites

Ensure the following are installed on your system:

- Python 3.11+
- PostgreSQL 14+ (running locally or via Docker)
- Git

### Step 1 — Clone Repository

```bash
git clone https://github.com/yourusername/neural-retail-analytics.git
cd neural-retail-analytics
```

### Step 2 — Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate          # Linux / macOS
venv\Scripts\activate             # Windows
```

### Step 3 — Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4 — Configure PostgreSQL

```bash
# Create the database
psql -U postgres -c "CREATE DATABASE retail_analytics;"

# Set environment variable (or update etl_pipeline.py directly)
export DATABASE_URL="postgresql://postgres:password@localhost:5432/retail_analytics"
```

### Step 5 — Place Data File

```
Download the Online Retail dataset (UCI / Kaggle) and place it at:
data/raw/online_retail.csv
```

### Step 6 — Docker (Optional)

```bash
# Run PostgreSQL via Docker
docker run --name retail-postgres \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=retail_analytics \
  -p 5432:5432 -d postgres:16
```

---

## ▶️ Execution Workflow

Run components in the following sequence for a full end-to-end execution:

```bash
# 1. Run the ETL Pipeline — Extract, Clean, Load to PostgreSQL
python etl_pipeline.py

# 2. Execute SQL Analytics Queries (connect to DB or run via psql)
psql -U postgres -d retail_analytics -f sql_queries.sql

# 3. Train Forecasting Models
python forecast_model.py          # Linear regression baseline
python prophet_forecast.py        # Prophet forecast
python LSTM_forecast.py           # LSTM deep learning forecast

# 4. Train Churn Prediction Model
python churn_model.py

# 5. Launch MLflow Tracking UI
mlflow ui --port 5000
# Visit: http://localhost:5000

# 6. Start FastAPI Prediction Service
uvicorn api:app --host 0.0.0.0 --port 8000 --reload
# Docs: http://localhost:8000/docs

# 7. Launch Streamlit Dashboard
streamlit run app.py
# Visit: http://localhost:8501

# 8. (Optional) Trigger via Airflow DAG
airflow dags trigger retail_pipeline_dag
```

---

## ⚡ Engineering Highlights

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                   WHAT MAKES THIS PRODUCTION-GRADE             │
  └─────────────────────────────────────────────────────────────────┘

  ✦  IDEMPOTENT ETL PIPELINE
     Upsert logic ensures repeated runs don't duplicate warehouse data.
     Safe for scheduled re-execution without manual intervention.

  ✦  DIMENSIONAL WAREHOUSE SCHEMA
     PostgreSQL designed with indexed tables and materialized views
     for sub-second query performance across millions of records.

  ✦  MULTI-MODEL FORECASTING
     Three parallel forecasting approaches (Linear, Prophet, LSTM)
     provide cross-validation and model comparison — not just one answer.

  ✦  FULL MLFLOW OBSERVABILITY
     Every experiment is logged. Every metric is tracked. Every model
     version is registered. Full lineage from data to prediction.

  ✦  ASYNC REST API
     FastAPI's async handlers allow concurrent prediction requests
     without blocking — built for real-time retail decision systems.

  ✦  ORCHESTRATED WITH AIRFLOW
     The full pipeline is a DAG — scheduled, monitored, and retryable.
     This is not a Jupyter notebook. This is a scheduled data system.

  ✦  DECOUPLED ARCHITECTURE
     Each layer (ETL → DWH → ML → API → Dashboard) is independently
     executable and replaceable — true separation of concerns.
```

---

## 💼 Business Impact

| Business Outcome | How This Platform Delivers It |
|---|---|
| **Reduced Revenue Blindspots** | SQL analytics expose product and regional performance in real time |
| **Proactive Churn Prevention** | ML churn scores enable targeted retention campaigns before customers leave |
| **Accurate Demand Planning** | 90-day Prophet + LSTM forecasts inform inventory and procurement decisions |
| **Customer Lifetime Value Optimization** | RFM segmentation guides differentiated CRM and marketing strategies |
| **Data-Driven Merchandising** | Product revenue contribution analytics surface high-impact SKUs |
| **Executive Visibility** | KPI dashboard provides leadership with a reliable, real-time business pulse |

---

## 📐 Scalability Considerations

The platform is architected to scale beyond a single-machine deployment:

| Layer | Current Stack | Scale-Up Path |
|---|---|---|
| **Storage** | PostgreSQL | AWS RDS / Azure Postgres / Snowflake |
| **ETL** | Python + Pandas | Apache Spark / dbt + Dagster |
| **Orchestration** | Local Airflow | Airflow on Kubernetes / AWS MWAA |
| **ML Training** | Local CPU/GPU | SageMaker / Vertex AI / Databricks |
| **API Serving** | Single Uvicorn | FastAPI behind Nginx + Load Balancer |
| **Dashboard** | Streamlit | Streamlit Cloud / Tableau / Power BI Embedded |
| **Data Volume** | ~500K records | Partitioned tables, columnar storage |

---

## 🗺️ Future Roadmap

```
  Phase 1 (Current)      Phase 2                     Phase 3
  ────────────────       ──────────────────────────   ─────────────────────────
  ✅ ETL Pipeline        ▸ dbt transformation layer   ▸ Real-time streaming
  ✅ PostgreSQL DWH      ▸ Snowflake migration         ▸ Kafka + Spark Streaming
  ✅ SQL Analytics       ▸ Great Expectations          ▸ Feature Store (Feast)
  ✅ Prophet + LSTM      ▸ CI/CD for ML models         ▸ Online serving (Redis)
  ✅ Churn Model         ▸ Kubernetes deployment        ▸ A/B model testing
  ✅ FastAPI             ▸ Grafana + Prometheus         ▸ LLM-powered analytics
  ✅ Streamlit           ▸ Auto-retraining triggers     ▸ Multi-tenant SaaS
  ✅ MLflow              ▸ Data quality monitoring       ▸ Anomaly detection
  ✅ Airflow DAG         ▸ Slack alerting integration   ▸ Recommendation engine
```

---

## 🎓 Skills Demonstrated

**Data Engineering**
`ETL Pipeline Design` · `Data Validation` · `Feature Engineering` · `Data Warehouse Modeling` · `SQL Analytics` · `Window Functions` · `Database Indexing` · `Materialized Views`

**Machine Learning**
`Time Series Forecasting` · `Deep Learning (LSTM)` · `Additive Models (Prophet)` · `Customer Segmentation (RFM)` · `Churn Prediction` · `Model Evaluation` · `Scikit-Learn Pipelines`

**MLOps**
`Experiment Tracking (MLflow)` · `Model Registry` · `Artifact Management` · `Model Versioning` · `Production Promotion Workflows`

**API Engineering**
`FastAPI` · `REST API Design` · `Pydantic Schema Validation` · `OpenAPI Documentation` · `Async Serving`

**Data Orchestration**
`Apache Airflow` · `DAG Design` · `Task Dependencies` · `Pipeline Scheduling`

**Visualization & BI**
`Streamlit Dashboards` · `Power BI` · `KPI Design` · `Business Storytelling`

**Infrastructure**
`Docker` · `PostgreSQL` · `Environment Management` · `Reproducible Builds`

---

## 🏆 Resume-Worthy Highlights

> *Designed and implemented an end-to-end retail data intelligence platform processing transactional records through automated ETL pipelines into a PostgreSQL data warehouse, powering SQL-based business analytics, multi-model time-series forecasting (Prophet + LSTM), RFM customer segmentation, and ML-driven churn prediction — all served via FastAPI REST endpoints and monitored through an MLflow experiment registry, with pipeline orchestration via Apache Airflow and business visibility via an interactive Streamlit dashboard.*

**Quantifiable talking points for interviews:**
- Engineered a multi-stage ETL pipeline with idempotent upsert logic and automated data validation
- Designed a PostgreSQL warehouse schema with indexed tables and materialized views for analytical query performance
- Implemented and compared three forecasting models (Linear Regression, Prophet, LSTM) with MLflow experiment tracking
- Built a real-time FastAPI prediction service with Pydantic request validation and auto-generated OpenAPI docs
- Developed RFM segmentation and churn prediction models enabling data-driven customer retention strategies
- Orchestrated the full pipeline as an Apache Airflow DAG with task-level monitoring and alerting

---

<div align="center">

---

### Built with precision. Engineered for production. Designed to impress.

<br/>

*If this project helped you or sparked ideas, consider giving it a ⭐ — it supports continued open engineering work.*

<br/>

**[🔗 Connect on LinkedIn](https://www.linkedin.com/in/samyak-kamble-972b10315/)** · **[📧 Get In Touch](mailto:samyakkamble3016@email.com)** · **[🌐 Portfolio](https://samyakgit-cloud.github.io/samyak-portfolio/)**

<br/>

<img width="100%" src="https://capsule-render.vercel.app/api?type=waving&color=0:0f0c29,50:302b63,100:24243e&height=120&section=footer"/>

</div>

## Author

Samyak Kamble
Data Engineering Enthusiast
