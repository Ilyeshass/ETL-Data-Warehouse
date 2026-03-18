# ETL-Data-Warehouse

This repository contains anend-to-end Data Warehouse solution engineered using Microsoft SQL Server (T-SQL). The project simulates a real-world data engineering environment by extracting raw business data (CRM and ERP systems), processing it through a structured pipeline, and modeling it for high-performance business intelligence analytics.

The primary goal of this project is to showcase robust ETL/ELT methodologies, database management, and dimensional modeling techniques without relying on external orchestration tools.

---

## 🏗️ System Architecture

To ensure data integrity, maintainability, and optimal query performance, the data warehouse is built on a strict three-layer architecture:

* **Layer 1: Staging (The Raw Zone)**
    * **Purpose:** Fast, direct ingestion of raw flat files (.csv) into the database.
    * **Implementation:** Utilizes highly optimized T-SQL `BULK INSERT` commands with table-level locking (`TABLOCK`) to minimize logging and maximize throughput. Data is stored exactly as it appears in the source systems.
* **Layer 2: Integration (The Cleansed Zone)**
    * **Purpose:** Data quality enforcement and standardization.
    * **Implementation:** Automated stored procedures handle data type casting, handle NULL values, remove duplicates, and standardize formatting across different source systems (merging ERP and CRM logic).
* **Layer 3: Presentation (The Analytical Zone)**
    * **Purpose:** Business-ready data optimized for BI tools and complex aggregations.
    * **Implementation:** Data is reshaped into a **Star Schema** consisting of Fact and Dimension tables. This layer supports rapid querying for customer behavior, sales trends, and product performance.

---

## 🛠️ Core Technologies & Techniques

* **Database Engine:** Microsoft SQL Server
* **Language:** Advanced T-SQL
* **Key Techniques Demonstrated:**
    * Custom Schema management for data isolation (e.g., `layer_one`, `layer_two`).
    * Automated ETL pipeline execution via Stored Procedures.
    * Robust error handling using `TRY...CATCH` blocks and transaction rollbacks.
    * Dimensional Data Modeling (Fact/Dimension creation).

---

## 📂 Repository Structure

```text
📦 ETL-Data-Warehouse
 ┣ 📂 datasets               # Simulated source data (CRM/ERP exports)
 ┣ 📂 docs                   # Architecture diagrams and schema documentation
 ┣ 📂 src
 ┃ ┣ 📂 Setup             # DDL scripts for database and schema creation
 ┃ ┣ 📂 layer_one         # Ingestion scripts and BULK INSERT procedures
 ┃ ┣ 📂 layer_two         # Cleansing and transformation logic
 ┃ ┗ 📂 layer_three       # Star schema definitions and analytical views
 ┣ 📜 README.md              # Project documentation
 ┗ 📜 .gitignore
