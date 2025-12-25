
Week 2 — ETL & EDA Bootcamp Project

This repository contains my Week 2 project for the data bootcamp.  
The goal is to build an end-to-end ETL pipeline and perform exploratory data analysis (EDA)on orders and users data.

Project Structure

week2-data-work/
├─ src/bootcamp_data/     # ETL logic and helpers
├─ data/
│  ├─ raw/               # Raw CSV inputs
│  └─ processed/         # ETL outputs
├─ notebooks/            # EDA notebook
├─ reports/              # Summary and figures
├─ scripts/              # ETL runner
└─ README.md


Setup (First time only)

From the project root:
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt


Daily Workflow
 Day 1 — Project Setup & Raw Data

Goal: Initialize the project and understand raw data.
Steps:
cd week2-data-work
.venv\Scripts\Activate.ps1

* Create project folders: `data/raw`, `data/processed`, `src/bootcamp_data`
* Place raw CSV files in:

data/raw/orders.csv
data/raw/users.csv

 Day 2 — IO Helpers

Goal: Read raw CSV files into pandas.

Open:

src/bootcamp_data/io.py

Run example test:
python -c "from bootcamp_data.io import read_orders_csv; print(read_orders_csv('data/raw/orders.csv').head())"


Day 3 — Transforms & Quality Checks

Goal:Implement cleaning and validation helpers.

Work in:

src/bootcamp_data/transforms.py
src/bootcamp_data/quality.py


Test functions using small snippets or notebooks.

Day 4 — EDA Notebook
Goal: Explore processed data.
Open:
notebooks/eda.ipynb
Run all cells to:

* Load `data/processed/analytics_table.parquet`
* Generate statistics and plots
* Save figures to `reports/figures/`
Day 5 — Full ETL Pipeline
Goal: Run the end-to-end pipeline and generate outputs.
Run:
bash
python scripts/run_etl.py

This will:

* Load raw data
* Transform and join tables
* Write outputs to:

data/processed/


Check:

data/processed/analytics_table.parquet
data/processed/_run_meta.json


EDA After ETL

Always run ETL first, then:

code notebooks/eda.ipynb

Select `.venv` kernel and run all cells.

 Summary Report

Open:

reports/summary.md
This file documents:
* Key findings
* Definitions
* Data quality issues
* Next questions






