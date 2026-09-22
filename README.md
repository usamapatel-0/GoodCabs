
# GoodCabs – Incremental Data Pipeline using Databricks Lakeflow SDP

A Databricks **Lakeflow Spark Declarative Pipeline (SDP)** for processing GoodCabs taxi trip data across 10 Indian cities using **incremental data processing, Auto Loader, and Auto CDC**.

The project follows the **Medallion Architecture**:

**Amazon S3 → Bronze → Silver → Gold → Dashboard**

Raw CSV data is ingested from Amazon S3, processed incrementally where applicable, cleaned and validated in the Silver layer, and transformed into analytics-ready Gold views.

---

## Problem Statement

Good Cabs is a fast-growing transportation company operating across multiple cities in India. As the business scales, it faces challenges in providing timely and reliable city-level data to regional managers.

The existing data pipelines are complex, manual, and error-prone, resulting in slow data processing and reduced stakeholder trust.

This project addresses these challenges by implementing **Lakeflow Spark Declarative Pipelines (SDP)** with **incremental data processing, Auto Loader, and Auto CDC** to build an automated, scalable, and reliable data pipeline that delivers **BI-ready data for faster and better decision-making**.

---

## Table of Contents

- [Pipeline Overview](#pipeline-overview)
- [Architecture](#architecture)
- [Source Data](#source-data)
- [Screenshots](#screenshots)
- [Pipeline Configuration](#pipeline-configuration)
- [Bronze Layer](#bronze-layer)
- [Silver Layer](#silver-layer)
- [Gold Layer](#gold-layer)
- [City Coverage](#city-coverage)
- [Data Quality](#data-quality)
- [Project Setup](#project-setup)
- [Directory Structure](#directory-structure)
- [End-to-End Flow](#end-to-end-flow)
- [Summary](#summary)

---

# Pipeline Overview

| Property | Value |
|---|---|
| Pipeline Name | `transportation_pipeline` |
| Unity Catalog | `transportation` |
| Default Schema | `bronze` |
| Compute | Serverless + Photon |
| Channel | CURRENT |
| Mode | Triggered |
| Processing | Incremental where applicable |
| Source Libraries | `transformations/**` |
| Source | Amazon S3 |
| Architecture | Bronze → Silver → Gold |

---

# Architecture

```text
                         AMAZON S3
                    goodcabs---1 bucket
                           │
                  ┌────────┴─────────┐
                  │                  │
              Trips CSV           City CSV
                  │                  │
             Auto Loader        Batch Read
                  │                  │
          Incremental Ingestion     │
                  │                  │
                  ▼                  ▼
           ┌─────────────┐    ┌─────────────┐
           │   BRONZE    │    │   BRONZE    │
           │    trips    │    │    city     │
           │  Streaming  │    │ Materialized│
           │    Table    │    │     View    │
           └──────┬──────┘    └──────┬──────┘
                  │                  │
                  ▼                  ▼
           ┌─────────────┐    ┌─────────────┐
           │   SILVER    │    │   SILVER    │
           │    trips    │    │    city     │
           │  Auto CDC   │    │ Materialized│
           │  SCD Type 1 │    │     View    │
           └──────┬──────┘    └──────┬──────┘
                  │                  │
                  │           ┌──────▼──────┐
                  │           │   SILVER    │
                  │           │   calendar  │
                  │           │ Materialized│
                  │           │     View    │
                  │           └──────┬──────┘
                  │                  │
                  └─────────┬────────┘
                            ▼
                     ┌───────────────┐
                     │     GOLD      │
                     │  fact_trips   │
                     │      View     │
                     │       +       │
                     │ 10 city views │
                     └───────┬───────┘
                             │
                             ▼
                         DASHBOARD
````

---

# Source Data

The source data consists of CSV files stored in an Amazon S3 bucket.

| Source | S3 Path                              | Format | Ingestion Method |
| ------ | ------------------------------------ | ------ | ---------------- |
| Trips  | `s3://goodcabs---1/data-store/trips` | CSV    | Auto Loader      |
| City   | `s3://goodcabs---1/data-store/city`  | CSV    | Batch CSV Read   |

## Trips Raw Columns

| Raw Column               | Bronze Column           | Silver Alias              | Description                |
| ------------------------ | ----------------------- | ------------------------- | -------------------------- |
| `trip_id`                | `trip_id`               | `id`                      | Unique trip identifier     |
| `date`                   | `date`                  | `business_date`           | Date of trip               |
| `city_id`                | `city_id`               | `city_id`                 | City identifier            |
| `passenger_type`         | `passenger_type`        | `passenger_category`      | Passenger type             |
| `distance_travelled(km)` | `distance_travelled_km` | `distance_kms`            | Distance travelled in km   |
| `fare_amount`            | `fare_amount`           | `sales_amt`               | Fare charged               |
| `passenger_rating`       | `passenger_rating`      | `passenger_rating`        | Passenger rating           |
| `driver_rating`          | `driver_rating`         | `driver_rating`           | Driver rating              |
| `_metadata.file_path`    | `file_name`             | —                         | Source file path           |
| Generated                | `ingest_datetime`       | `bronze_ingest_timestamp` | Bronze ingestion timestamp |

## City Raw Columns

| Column      | Description            |
| ----------- | ---------------------- |
| `city_id`   | Unique city identifier |
| `city_name` | City name              |

---

# Screenshots

This section provides visual evidence of the project setup, source data, pipeline processing, architecture, catalog objects, and final dashboard.

## 1. Architecture

This screenshot shows the architecture of the project.

<img width="14204" height="4628" alt="architecture" src="https://github.com/user-attachments/assets/4daceb87-b2b5-474d-adc7-1d048cdc40b4" />

---

## 2. AWS S3 Source Data

This screenshot shows the GoodCabs source CSV files stored in the Amazon S3 bucket.

<img width="1917" height="1027" alt="Screenshot 2026-09-18 164510" src="https://github.com/user-attachments/assets/6851183f-a27d-40f0-8af3-7f2298b3fe9b" />

<img width="1917" height="1027" alt="Screenshot 2026-09-18 164454" src="https://github.com/user-attachments/assets/80d7ede1-ac26-4cfb-aa7f-acbad99f94a3" />

---

## 3. Databricks Pipeline

This screenshot shows the `transportation_pipeline` configuration.

<img width="1917" height="1007" alt="image" src="https://github.com/user-attachments/assets/95bdb64d-5018-4349-9f2a-5636b312073a" />

---

## 4. Pipeline Architecture / DAG

This screenshot shows the Bronze → Silver → Gold dependency graph and data flow.

<img width="1917" height="1011" alt="Screenshot 2026-09-18 164322" src="https://github.com/user-attachments/assets/7de14b6a-ce3c-40dd-b425-ec77c065c663" />

<img width="1917" height="1020" alt="Screenshot 2026-09-18 164353" src="https://github.com/user-attachments/assets/e18729bf-7128-4000-a96c-0ae549f64039" />

---

## 5. Unity Catalog

This screenshot shows the `transportation` catalog and its Bronze, Silver, and Gold schemas.

<img width="1917" height="1025" alt="image" src="https://github.com/user-attachments/assets/5de6c5ad-a459-4172-b9e9-8327053afcbf" />

---

## 6. Gold Dashboard

This screenshot shows the final analytics dashboard built using the Gold layer data.

<img width="1917" height="1022" alt="Screenshot 2026-09-21 194220" src="https://github.com/user-attachments/assets/e3eaa74e-30f7-41be-8643-4d6800726f22" />


---

# Pipeline Configuration

The pipeline uses two parameters for generating the calendar dimension.

| Parameter    | Default Value | Description            |
| ------------ | ------------- | ---------------------- |
| `start_date` | `2025-01-01`  | Start date of calendar |
| `end_date`   | `2025-12-31`  | End date of calendar   |

---

# Bronze Layer

The Bronze layer stores raw source data with minimal transformations.

The main purpose is to preserve source information and add ingestion metadata for traceability.

## `transportation.bronze.trips`

**File:** `transformations/bronze/trips.py`

**Type:** Streaming Table

### Ingestion

* Auto Loader
* CSV format
* Schema inference enabled
* Schema evolution/rescue enabled
* Maximum 100 files per trigger
* Incremental ingestion of newly arriving files

### Transformations

* `distance_travelled(km)` → `distance_travelled_km`
* Adds `file_name` using `_metadata.file_path`
* Adds `ingest_datetime` using the current timestamp

### Table Properties

* `quality = bronze`
* `layer = bronze`
* Change Data Feed enabled
* Auto Optimize enabled

### Incremental Processing

Auto Loader processes newly arriving source files incrementally rather than requiring the complete source dataset to be processed again on every run.

```text
New CSV Files
     ↓
 Auto Loader
     ↓
Incremental Ingestion
     ↓
Bronze Trips
```

---

## `transportation.bronze.city`

**File:** `transformations/bronze/city.py`

**Type:** Materialized View

### Ingestion

* Batch CSV read
* Header enabled
* Schema inference enabled
* PERMISSIVE mode
* Corrupt record capture

### Transformations

* Adds `file_name`
* Adds `ingest_datetime`

### Table Properties

* `quality = bronze`
* `layer = bronze`
* Change Data Feed enabled
* Auto Optimize enabled

---

# Silver Layer

The Silver layer cleans, validates, standardizes, and enriches the Bronze data.

## `transportation.silver.trips`

**File:** `transformations/silver/trips.py`

**Type:** Streaming Table with Auto CDC

### CDC Configuration

| Property     | Value                        |
| ------------ | ---------------------------- |
| CDC Key      | `id`                         |
| Sequence By  | `silver_processed_timestamp` |
| SCD Type     | Type 1                       |
| Staging View | `trips_silver_staging`       |

### Incremental CDC Processing

The Silver trips table uses **Auto CDC** to process new and changed records incrementally.

```text
New / Changed Records
        ↓
trips_silver_staging
        ↓
     Auto CDC
        ↓
Silver Trips
```

The CDC flow applies incoming changes to the target Silver table using the trip ID as the key.

### SCD Type 1

SCD Type 1 keeps the latest record for each key.

```text
Old Record
    ↓
New Record with same ID
    ↓
Latest value replaces old value
```

Historical versions are not retained.

### Staging View

The Bronze data is transformed in the `trips_silver_staging` view before the Auto CDC flow writes data into `transportation.silver.trips`.

```text
Bronze Trips
     ↓
trips_silver()
     ↓
trips_silver_staging
     ↓
Auto CDC
     ↓
Silver Trips
```

---

## `transportation.silver.city`

**File:** `transformations/silver/city.py`

**Type:** Materialized View

### Transformations

* Selects `city_id`
* Selects `city_name`
* Renames Bronze `ingest_datetime` → `bronze_ingest_timestamp`
* Adds `silver_processed_timestamp`

---

## `transportation.silver.calendar`

**File:** `transformations/silver/calendar.py`

**Type:** Materialized View

The calendar dimension is generated using the pipeline parameters `start_date` and `end_date`.

### Generated Columns

* `date`
* `date_key`
* `year`
* `month`
* `day_of_month`
* `day_of_week`
* `day_of_week_abbr`
* `month_name`
* `month_year`
* `quarter`
* `quarter_year`
* `week_of_year`
* `day_of_year`
* `is_weekday`
* `is_weekend`
* `is_holiday`
* `holiday_name`
* `silver_processed_timestamp`

### Indian National Holidays

* January 26 — Republic Day
* August 15 — Independence Day
* October 2 — Gandhi Jayanti

---

# Gold Layer

The Gold layer contains analytics-ready **normal SQL Views**.

The main fact view combines trip data with city and calendar information.

## `transportation.gold.fact_trips`

**File:** `transformations/gold/trips_gold.sql`

**Type:** Normal SQL View

### Source Tables

```text
transportation.silver.trips
              │
              ├──── JOIN ──── transportation.silver.city
              │
              └──── JOIN ──── transportation.silver.calendar
                                │
                                ▼
                     transportation.gold.fact_trips
```

### Joins

| Source            | Join Condition              |
| ----------------- | --------------------------- |
| `silver.trips`    | Main trip/fact data         |
| `silver.city`     | `t.city_id = c.city_id`     |
| `silver.calendar` | `t.business_date = ca.date` |

### Main Output Columns

* `id`
* `business_date`
* `city_id`
* `city_name`
* `passenger_category`
* `distance_kms`
* `sales_amt`
* `passenger_rating`
* `driver_rating`
* `month`
* `day_of_month`
* `day_of_week`
* `month_name`
* `month_year`
* `quarter`
* `quarter_year`
* `week_of_year`
* `is_weekday`
* `is_weekend`
* `national_holiday`

### Gold View SQL

```sql
CREATE VIEW transportation.gold.fact_trips
AS
SELECT
    t.id,
    t.business_date,
    t.city_id,
    c.city_name,
    t.passenger_category,
    t.distance_kms,
    t.sales_amt,
    t.passenger_rating,
    t.driver_rating,
    ca.month,
    ca.day_of_month,
    ca.day_of_week,
    ca.month_name,
    ca.month_year,
    ca.quarter,
    ca.quarter_year,
    ca.week_of_year,
    ca.is_weekday,
    ca.is_weekend,
    ca.is_holiday AS national_holiday
FROM transportation.silver.trips t
JOIN transportation.silver.city c
    ON t.city_id = c.city_id
JOIN transportation.silver.calendar ca
    ON t.business_date = ca.date;
```
<img width="1917" height="1028" alt="gold" src="https://github.com/user-attachments/assets/d40e7306-e0f2-4138-ac6a-f3dea2cad484" />

---

# City-Specific Gold Views

Each city has its own filtered normal SQL View derived from `fact_trips`.

| View Name                                      | City ID | City          |
| ---------------------------------------------- | ------- | ------------- |
| `transportation.gold.fact_trips_indore`        | MP01    | Indore        |
| `transportation.gold.fact_trips_visakhapatnam` | AP01    | Visakhapatnam |
| `transportation.gold.fact_trips_jaipur`        | RJ01    | Jaipur        |
| `transportation.gold.fact_trips_surat`         | GJ01    | Surat         |
| `transportation.gold.fact_trips_kochi`         | KL01    | Kochi         |
| `transportation.gold.fact_trips_vadodara`      | GJ02    | Vadodara      |
| `transportation.gold.fact_trips_mysore`        | KA01    | Mysore        |
| `transportation.gold.fact_trips_lucknow`       | UP01    | Lucknow       |
| `transportation.gold.fact_trips_chandigarh`    | CH01    | Chandigarh    |
| `transportation.gold.fact_trips_coimbatore`    | TN01    | Coimbatore    |

Example:

```sql
CREATE VIEW transportation.gold.fact_trips_jaipur AS
SELECT *
FROM transportation.gold.fact_trips
WHERE city_id = 'RJ01';
```

---

# City Coverage

The pipeline covers 10 Indian cities across 8 states/UTs.

| City          | City ID | State/UT       |
| ------------- | ------- | -------------- |
| Indore        | MP01    | Madhya Pradesh |
| Visakhapatnam | AP01    | Andhra Pradesh |
| Jaipur        | RJ01    | Rajasthan      |
| Surat         | GJ01    | Gujarat        |
| Vadodara      | GJ02    | Gujarat        |
| Kochi         | KL01    | Kerala         |
| Mysore        | KA01    | Karnataka      |
| Lucknow       | UP01    | Uttar Pradesh  |
| Chandigarh    | CH01    | Chandigarh     |
| Coimbatore    | TN01    | Tamil Nadu     |

---

# Data Quality

Data quality is enforced in the Silver trips staging view.

| Expectation              | Rule                                | Action |
| ------------------------ | ----------------------------------- | ------ |
| `valid_date`             | `year(business_date) >= 2020`       | Drop   |
| `valid_driver_rating`    | `driver_rating BETWEEN 1 AND 10`    | Drop   |
| `valid_passenger_rating` | `passenger_rating BETWEEN 1 AND 10` | Drop   |

Invalid records are removed before the Auto CDC upsert into `transportation.silver.trips`.

### Data Quality Flow

```text
Bronze Trips
     ↓
Transformation
     ↓
Data Quality Expectations
     ↓
Invalid rows dropped
     ↓
Auto CDC
     ↓
Silver Trips
```

---

# Project Setup

Before running the pipeline, create the Unity Catalog and schemas.

```sql
CREATE CATALOG IF NOT EXISTS transportation;

CREATE SCHEMA IF NOT EXISTS transportation.bronze;

CREATE SCHEMA IF NOT EXISTS transportation.silver;

CREATE SCHEMA IF NOT EXISTS transportation.gold;
```

A setup notebook is available at:

```text
GoodCabs/project_setup
```

---

# Directory Structure

```text
transportation_pipeline/
│
├── README.md
│
├── screenshots/
│   ├── s3.png
│   ├── pipeline.png
│   ├── architecture.png
│   ├── catalog.png
│   └── dashboard.png
│
└── transformations/
    │
    ├── bronze/
    │   ├── city.py
    │   └── trips.py
    │
    ├── silver/
    │   ├── calendar.py
    │   ├── city.py
    │   └── trips.py
    │
    └── gold/
        ├── trips_gold.sql
        ├── trips_indore.sql
        ├── trips_visakhapatnam.sql
        ├── trips_jaipur.sql
        ├── trips_surat.sql
        ├── trips_kochi.sql
        ├── trips_vadodara.sql
        ├── trips_mysore.sql
        ├── trips_lucknow.sql
        ├── trips_chandigarh.sql
        └── trips_coimbatore.sql
```

---

# End-to-End Flow

```text
                         Amazon S3
                            │
                   ┌────────┴────────┐
                   │                 │
                Trips CSV         City CSV
                   │                 │
              Auto Loader        Batch Read
                   │                 │
          Incremental Ingestion     │
                   ▼                 ▼
                BRONZE            BRONZE
                 Trips              City
                   │                 │
                   └────────┬────────┘
                            ▼
                         SILVER
              ┌─────────────┼─────────────┐
              │             │             │
            Trips          City        Calendar
              │
           Auto CDC
         SCD Type 1
              │
       Incremental Updates
              │
              └─────────────┬─────────────┘
                            ▼
                           GOLD
                            │
                     fact_trips View
                            │
              ┌─────────────┼─────────────┐
              │             │             │
           Indore         Jaipur        Surat
              │             │             │
              └────── 10 City Views ──────┘
                            │
                            ▼
                        Dashboard
```

---

# Summary

The GoodCabs Transportation Pipeline demonstrates an end-to-end Databricks data engineering workflow using **Lakeflow Spark Declarative Pipelines**.

### Key Technologies

* Databricks Lakeflow Spark Declarative Pipelines
* PySpark
* Spark SQL
* Amazon S3
* Auto Loader
* Incremental Data Processing
* Auto CDC
* SCD Type 1
* Unity Catalog
* Medallion Architecture
* Databricks SQL Views
* Dashboard / Analytics

### Layer Responsibilities

| Layer     | Purpose                                          |
| --------- | ------------------------------------------------ |
| Bronze    | Raw ingestion + metadata + incremental ingestion |
| Silver    | Cleaning + validation + CDC + enrichment         |
| Gold      | Analytics-ready views                            |
| Dashboard | Business reporting and visualization             |

### Key Data Flow

**S3 → Auto Loader → Incremental Bronze Ingestion → Silver Streaming + Auto CDC → Gold Views → Dashboard**

The project provides a complete flow from **raw S3 data to analytics-ready Gold views and dashboard reporting**.

```
```
