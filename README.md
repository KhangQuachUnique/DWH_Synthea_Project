# DWH Project (SQL Server + SSIS + SSAS) - Synthea

This repository contains a starter folder structure for a Data Warehouse project built on SQL Server BI stack:
- SQL Server Database Engine (staging, ODS, DWH, marts)
- SSIS for ETL orchestration
- SSAS for semantic model (Tabular + optional Multidimensional)
- Optional SSRS and CI/CD scaffolding

## High-level structure
- `data/`: source and intermediate data files
- `docs/`: architecture, mappings, and runbook documentation
- `sql/`: DDL/DML scripts and database objects
- `ssis/`: integration project assets and deployment outputs
- `ssas/`: analysis services model/cube assets
- `ssrs/`: reporting services assets
- `scripts/`: automation scripts for setup and deployment
- `tests/`: SQL and data quality validations
- `ci-cd/`: pipeline definitions

## Suggested next steps
1. Copy Synthea CSV files into `data/raw/synthea/csv/`.
2. Build staging tables using scripts in `sql/01_ddl/staging/`.
3. Create and save your SSDT solution under `ssis/solution/` and `ssas/tabular/model/`.
4. Implement ETL packages by layer in `ssis/packages/`.
5. Add post-load quality checks in `tests/data_quality/`.
