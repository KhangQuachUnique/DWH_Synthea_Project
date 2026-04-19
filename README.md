# DWH Synthea Project

Pipeline ETL du lieu Synthea CSV vao SQL Server theo 3 lop:
1. Landing (raw)
2. Staging (clean + typed)
3. DWH (star schema)

## Requirements
1. SQL Server (Windows Authentication)
2. Python 3.10+
3. ODBC Driver 17/18
4. Packages: pandas, pyodbc, tqdm

## CSV Input
Script extract tu do path theo thu tu:
1. data
2. data/raw/synthea/csv
3. data/raw/synthea
4. data/csv

Co the override bang env var:
- SYNTHEA_CSV_PATH=<your_path>

## Run Order
1. Chay 01_Schema_Landing.sql
2. Chay python 02_ETL_Synthea_Extract.py
3. Chay 03_Schema_Staging.sql
4. Chay 04_deploy_transform_procs.ps1, sau do 05_run_all_transform_procs.sql
5. Chay 06_Schema_DWH_v2.sql va 07_Procedures_v2.sql
6. Chay 08_Run_full_load_v2.sql hoac 08_Run_partial_load_v2.sql

## Idempotent
1. Schema scripts co check ton tai truoc khi tao/them
2. FK tao co dieu kien, tranh loi khi chay lai
3. Transform procs dung CREATE OR ALTER
4. Extract dung path dong, chay lai an toan

## Logging
1. File log trong thu muc logs
2. ETL_Control: watermark/batch
3. ETL_Run_Log: run status
