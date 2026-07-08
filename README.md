# 🏬 FMCG Data Warehouse Project
### Enterprise Data Warehouse using Medallion Architecture (Bronze → Silver → Gold)

![SQL Server](https://img.shields.io/badge/SQL%20Server-CC2927?style=for-the-badge&logo=microsoftsqlserver&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

---

# 📌 Project Overview

This project demonstrates the design and implementation of an enterprise-grade **Data Warehouse** for a **Fast-Moving Consumer Goods (FMCG)** company using the **Medallion Architecture (Bronze → Silver → Gold).**

The warehouse integrates data from two business entities:

- 🏢 **Atliqon Parent**
- 🛒 **SportsBar Acquired**

The goal is to consolidate transactional data, enforce data quality, apply business transformations, and deliver an optimized analytics layer for Business Intelligence reporting.

This project was developed as a professional data engineering portfolio project and follows modern Data Warehouse best practices.

---

# 🎯 Project Objectives

- Build an enterprise Data Warehouse
- Integrate multiple business entities
- Apply Data Quality rules
- Track historical customer changes using SCD Type 2
- Standardize currencies into USD
- Build optimized Gold Layer tables
- Prepare data for Power BI dashboards
- Design a scalable architecture ready for cloud migration

---

# 🏗️ Architecture

```
                Source Systems
                      │
                      ▼
            ┌──────────────────┐
            │   Bronze Layer   │
            │ Raw Source Data  │
            └──────────────────┘
                      │
                      ▼
            ┌──────────────────┐
            │   Silver Layer   │
            │ Clean & Conform  │
            └──────────────────┘
                      │
                      ▼
            ┌──────────────────┐
            │    Gold Layer    │
            │ Analytics Ready  │
            └──────────────────┘
                      │
                      ▼
                 Power BI
```

---

# 🥉 Bronze Layer

The Bronze Layer stores raw source data exactly as received.

### Schemas

- bronze_parent
- bronze_acquired
- bronze_shared

### Responsibilities

- Preserve raw records
- Historical archive
- No transformations
- Source of truth

---

# ⚪ Silver Layer

The Silver Layer applies data quality rules and business transformations.

## Data Cleaning

- Standardized product categories
- Removed numeric suffixes from product names
- Normalized gender values
- Fixed inconsistent text formatting

## Deduplication

- Identified duplicate sales
- Removed over **2,000 duplicate records**

## Business Rules

Negative profit prevention by limiting product cost to:

```
Product Cost ≤ 70% of Net Price
```

## Currency Standardization

Converted all transactions into **USD** using shared exchange rate tables.

## Slowly Changing Dimensions (SCD Type 2)

Customer history is preserved using:

- valid_from
- valid_to
- is_current

---

# 🥇 Gold Layer

The Gold Layer is optimized for reporting and analytics.

### Features

✔ Unified Parent & Acquired data

✔ Materialized Fact Tables

✔ Optimized Dimension Tables

✔ Clustered Indexes

✔ Non-Clustered Indexes

✔ Audit Columns

### Main Tables

- fact_sales_table
- dim_customer
- dim_product
- dim_store
- dim_date

---

# 📊 Data Pipeline

```
CSV Files
     │
     ▼
Bronze Layer
     │
     ▼
Data Cleaning
     │
     ▼
Business Rules
     │
     ▼
Currency Conversion
     │
     ▼
SCD Type 2
     │
     ▼
Gold Tables
     │
     ▼
Power BI Dashboard
```

---

# ⚙️ Technologies Used

| Category | Technology |
|-----------|------------|
| Database | SQL Server |
| Language | T-SQL |
| Data Generation | Python |
| BI | Power BI |
| Version Control | Git & GitHub |

---

# 🚀 Future Cloud Roadmap

The current implementation is an **on-premise SQL Server prototype**.

The future architecture will migrate to a modern cloud data stack.

| Current | Future |
|----------|--------|
| SQL Server | Google BigQuery |
| Stored Procedures | dbt |
| Manual Execution | Apache Airflow |
| Local Database | Cloud Data Warehouse |
| Power BI | Power BI |

---

# 🔮 Planned Enhancements

- Apache Airflow DAGs
- Google BigQuery
- dbt Models
- Automated Testing
- Data Lineage
- Incremental Loading
- CI/CD Pipeline
- Docker Deployment
- Data Catalog
- Monitoring & Logging

---

# 📁 Repository Structure

```
FMCG-Data-Warehouse/
│
├── SQLQuery_Clean-1.sql
│
├── FMCG_Data_Warehouse_Project_Roadmap.pdf
│
├── README.md
│
└── images/
      architecture.png
      pipeline.png
```

---

# 📈 Key Features

- Enterprise Medallion Architecture
- Data Quality Framework
- Multi-company Integration
- SCD Type 2 Implementation
- Currency Normalization
- Materialized Gold Layer
- Query Performance Optimization
- Audit & Lineage Columns
- BI Ready Dataset

---

# 📚 Skills Demonstrated

- Data Warehousing
- ETL Development
- SQL Server
- T-SQL
- Data Modeling
- Star Schema Design
- SCD Type 2
- Data Cleaning
- Performance Tuning
- Indexing
- Business Rules
- Data Governance
- Power BI

---

# 📌 Project Status

✅ Bronze Layer Completed

✅ Silver Layer Completed

✅ Gold Layer Completed

✅ Performance Optimization Completed

🚧 Cloud Migration (Planned)

🚧 Apache Airflow

🚧 Google BigQuery

🚧 dbt Integration

---

# 👨‍💻 Authors
**Yassa**

**Mohamed Minyar**

**ElSayed**

**Amr Elaymani**

**Ali**

**Mohamed Elshabasy**

## ⭐ If you found this project helpful, consider giving it a Star!
