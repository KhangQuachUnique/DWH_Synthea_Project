
# SYNTHEA ETL - Hướng dẫn Triển khai Đầy đủ

## 📋 Tổng quan

**Mục đích**: Load dữ liệu Synthea (16 file CSV) vào SQL Server qua hai layer:
- **Landing Layer**: Lưu dữ liệu thô từ CSV (toàn bộ cột = VARCHAR)
- **Staging Layer**: Dữ liệu đã transform với kiểu dữ liệu chính xác

---

## 🗂️ Cấu trúc Thư mục

```
DWH_Synthea_Project/
├── 01_Schema_Landing.sql          # Tạo Landing database
├── 02_ETL_Synthea_Extract.py      # Script load CSV → Landing
├── 03_Schema_Staging.sql          # Tạo Staging database
├── data/
│   └── raw/
│       └── synthea/
│           └── csv/
│               ├── patients.csv
│               ├── encounters.csv
│               ├── conditions.csv
│               ├── medications.csv
│               ├── observations.csv
│               └── ... (12 files khác)
├── logs/                           # Thư mục log (tự tạo)
│   └── errors/                     # Log lỗi (tự tạo)
├── SYNTHEA_CSV_DATA_SCHEMA.md     # Tài liệu schema
└── README.md                       # File này
```

---

## 🚀 Hướng dẫn Cài đặt

### 1. **Cấu hình SQL Server**

#### Option A: Dùng Windows Authentication (Khuyến nghị)
- Không cần nhập credential
- Chỉnh `Config.USE_WINDOWS_AUTH = True` trong script

#### Option B: Dùng SQL Server Authentication
Sửa trong file `02_ETL_Synthea_Extract.py`:
```python
class Config:
    USE_WINDOWS_AUTH = False
    SQL_USER = "sa"
    SQL_PASSWORD = "YourPassword123"
```

### 2. **Tạo Folder Dữ liệu SQL Server**

```bash
# Windows Command Prompt
mkdir C:\SQLServerData

# Hoặc PowerShell
New-Item -ItemType Directory -Force -Path "C:\SQLServerData"
```

### 3. **Cài đặt Python Dependencies**

```bash
# Tạo virtual environment (tùy chọn)
python -m venv venv
venv\Scripts\activate

# Cài đặt thư viện
pip install pandas pyodbc sqlalchemy tqdm

# Kiểm tra cài đặt
python -c "import pandas, pyodbc, sqlalchemy; print('✓ All libraries installed')"
```

### 4. **Chuẩn bị File CSV**

Đảm bảo các file CSV đã được copy vào:
```
data/raw/synthea/csv/
```

**Danh sách 16 file cần có:**
1. allergies.csv (5,417 dòng)
2. careplans.csv (37,715 dòng)
3. conditions.csv (114,544 dòng)
4. devices.csv (2,360 dòng)
5. encounters.csv (321,528 dòng)
6. imaging_studies.csv (4,504 dòng)
7. immunizations.csv (16,481 dòng)
8. medications.csv (431,262 dòng) **LỚN**
9. observations.csv (1,659,750 dòng) **RẤT LỚN**
10. organizations.csv (5,499 dòng)
11. patients.csv (12,352 dòng)
12. payer_transitions.csv (41,392 dòng)
13. payers.csv (10 dòng)
14. procedures.csv (100,427 dòng)
15. providers.csv (31,764 dòng)
16. supplies.csv (143,110 dòng)

---

## ⚡ Chạy ETL

### Step 1: Tạo Landing Database Schema

```bash
# SQL Server Management Studio (SSMS)
# Mở 01_Schema_Landing.sql và chạy (F5)

# Hoặc dùng Command Line
sqlcmd -S localhost -U sa -P YourPassword -i 01_Schema_Landing.sql
```

**Output mong đợi:**
```
Landing Database Schema Created Successfully!
```

### Step 2: Chạy ETL Script Load CSV

```bash
# Từ dòng lệnh
python 02_ETL_Synthea_Extract.py

# Hoặc từ IDE (VS Code, PyCharm)
```

**Output mong đợi:**
```
================================================================================
SYNTHEA ETL PROCESS STARTED
Log file: logs/etl_20260415_143022.log
CSV data path: d:\DWH_Synthea_Project\data\raw\synthea\csv
================================================================================

[STEP 1] Creating databases...
✓ Database 'DW_Synthea_Landing' already exists

[STEP 2] Resetting landing tables...
✓ Truncated Landing_Patients
✓ Truncated Landing_Encounters
...

[STEP 3] Loading CSV files to Landing layer...
================================================================================
Loading Patients          | File: patients.csv           | Size: 2.15 MB
================================================================================
Total rows to load: 12,352

Loading Patients |████████████| 12,352/12,352 [00:05<00:00, 2,200 rows/s]
✓ Successfully loaded 12,352 rows to Landing_Patients

================================================================================
ETL SUMMARY REPORT
================================================================================

Landing Layer:
  ✓ Successful loads: 15/16
  ✗ Failed loads:     1/16
  Total rows loaded:  2,300,000 (approx)

Detailed Summary:
Table                          Rows     Status   Duration
────────────────────────────────────────────────────────
Landing_Patients              12,352   SUCCESS     5.23s
Landing_Encounters           321,528   SUCCESS    15.40s
...
```

### Step 3: Tạo Staging Database Schema

```bash
# SQL Server Management Studio (SSMS)
# Mở 03_Schema_Staging.sql và chạy (F5)
```

---

## 📊 Kiểm tra Kết quả

### 1. **Kiểm tra dữ liệu Landing**

```sql
-- Mở SQL Server Management Studio

-- Kiểm tra số lượng dòng
USE DW_Synthea_Landing;
GO

SELECT 'Patients' AS TableName, COUNT(*) AS RowCount FROM Landing_Patients
UNION ALL
SELECT 'Encounters', COUNT(*) FROM Landing_Encounters
UNION ALL
SELECT 'Medications', COUNT(*) FROM Landing_Medications
UNION ALL
SELECT 'Observations', COUNT(*) FROM Landing_Observations
UNION ALL
SELECT 'Conditions', COUNT(*) FROM Landing_Conditions;

-- Output:
-- TableName        RowCount
-- Patients         12352
-- Encounters       321528
-- Medications      431262
-- Observations     1659750
-- Conditions       114544
```

### 2. **Kiểm tra File Log**

```bash
# Xem log file
logs/etl_20260415_143022.log

# Xem file CSV execution summary
logs/etl_execution_20260415_143022.csv
```

### 3. **Kiểm tra LoadLog Table**

```sql
SELECT * FROM DW_Synthea_Landing.dbo.LoadLog
ORDER BY CreatedDate DESC;
```

---

## 🔧 Cấu hình Chi tiết

### Sửa Config trong `02_ETL_Synthea_Extract.py`

```python
class Config:
    # Đường dẫn
    PROJECT_ROOT = Path(__file__).parent
    CSV_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "synthea" / "csv"
    
    # SQL Server
    SQL_SERVER = "localhost"      # Thay 'localhost' by hostname/IP
    SQL_USER = ""                 # Để trống nếu Windows Auth
    SQL_PASSWORD = ""             # Để trống nếu Windows Auth
    USE_WINDOWS_AUTH = True      # True = Windows, False = SQL Auth
    
    # Database names
    LANDING_DB = "DW_Synthea_Landing"
    STAGING_DB = "DW_Synthea_Staging"
    
    # Performance tuning
    CHUNK_SIZE = 100000           # Rows per chunk (càng lớn → memory nhiều)
    BATCH_SIZE = 5000             # Rows per batch insert
```

### Tuning Performance

| Tình huống | CHUNK_SIZE | BATCH_SIZE | Ghi chú |
|-----------|-----------|-----------|--------|
| Memory ít (~4GB) | 50,000 | 2,500 | Chậm hơn nhưng ổn định |
| Memory vừa (~8GB) | 100,000 | 5,000 | Cân bằng |
| Memory lớn (~16GB+) | 200,000 | 10,000 | Nhanh nhất |

---

## ⚙️ Xử lý Lỗi Thường gặp

### Lỗi 1: "No package metadata found"

**Nguyên nhân**: pyodbc chưa cài đặt đúng

**Giải pháp**:
```bash
pip uninstall pyodbc -y
pip install pyodbc --upgrade
```

### Lỗi 2: "Connection error to localhost"

**Nguyên nhân**: SQL Server chưa chạy hoặc hostname sai

**Kiểm tra**:
```bash
# Mở SQL Server Configuration Manager
# Kiểm tra: SQL Server (MSSQLSERVER) có running không?

# Hoặc dùng PowerShell
Get-Service MSSQLSERVER
```

**Giải pháp**: Chỉnh `SQL_SERVER`:
```python
# Dùng hostname thực
SQL_SERVER = "DESKTOP-ABC123"

# Hoặc dùng IP
SQL_SERVER = "192.168.1.100"

# Với port custom
SQL_SERVER = "localhost,1433"
```

### Lỗi 3: "File not found CSV"

**Nguyên nhân**: Đường dẫn CSV sai

**Kiểm tra**:
```bash
# Command Prompt
dir "d:\DWH_Synthea_Project\data\raw\synthea\csv\*.csv"
```

**Giải pháp**: Sửa `CSV_DATA_PATH` trong Config

### Lỗi 4: "Memory Error" khi load OBSERVATIONS

**Nguyên nhân**: File quá lớn (~1.65M rows), CHUNK_SIZE quá lớn

**Giải pháp**:
```python
CHUNK_SIZE = 50000        # Giảm từ 100K xuống 50K
BATCH_SIZE = 2500         # Giảm từ 5K xuống 2.5K
```

---

## 📈 Performance Benchmark

**Thời gian Load dự kiến** (trên máy tính bình thường):

| File | Rows | Thời gian |
|------|------|----------|
| Patients | 12K | ~1 giây |
| Encounters | 321K | ~8 giây |
| Conditions | 114K | ~3 giây |
| Medications | 431K | ~12 giây |
| **Observations** | **1.66M** | **~45 giây** |
| **Tổng cộng** | **~2.8M** | **~120 giây** |

---

## 📝 Danh sách Cột Landing vs Staging

### Ví dụ: PATIENTS Table

**Landing** (Tất cả = VARCHAR):
```sql
CREATE TABLE Landing_Patients (
    Id VARCHAR(36),
    BIRTHDATE VARCHAR(10),
    DEATHDATE VARCHAR(10),
    HEALTHCARE_EXPENSES VARCHAR(20),
    ...
)
```

**Staging** (Kiểu dữ liệu chính xác):
```sql
CREATE TABLE Staging_Patients (
    Id VARCHAR(36) PRIMARY KEY,
    BIRTHDATE DATE,
    DEATHDATE DATE,
    HEALTHCARE_EXPENSES DECIMAL(10,2),
    ...
)
```

---

## 🔐 Bảo mật

### Best Practices

1. **Không store credential trong code**
   ```python
   # ❌ Sai
   SQL_PASSWORD = "MyPassword123"
   
   # ✅ Đúng
   import os
   SQL_PASSWORD = os.getenv("SQL_PASSWORD")
   ```

2. **Dùng Windows Authentication** khi có thể
   ```python
   USE_WINDOWS_AUTH = True
   ```

3. **Backup dữ liệu trước khi load**
   ```bash
   # SQL script backup
   BACKUP DATABASE DW_Synthea_Landing TO DISK='C:\backup\landing_backup.bak'
   ```

---

## 🛠️ Tìm hiểu Thêm

### Xem Log Chi tiết

```bash
# Xem log file chính
type logs/etl_20260415_143022.log | more

# Grep lỗi
findstr "ERROR" logs/etl_20260415_143022.log

# Xem CSV summary
start logs/etl_execution_20260415_143022.csv
```

### Query Check Data Quality

```sql
-- Kiểm tra NULL values
SELECT 
    'Patients' AS TableName,
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN Id IS NULL THEN 1 ELSE 0 END) AS NullIds
FROM DW_Synthea_Landing.dbo.Landing_Patients
UNION ALL
SELECT 'Encounters', COUNT(*), SUM(CASE WHEN Id IS NULL THEN 1 ELSE 0 END)
FROM DW_Synthea_Landing.dbo.Landing_Encounters;

-- Kiểm tra LoadDate
SELECT MIN(LoadDate) AS EarliestLoad, MAX(LoadDate) AS LatestLoad
FROM DW_Synthea_Landing.dbo.Landing_Patients;
```

---

## 📞 Support

**Vấn đề thường gặp:**
- Xem log file trong `logs/` folder
- Kiểm tra error log trong `logs/errors/`
- Validate CSV file format

**Tiếp theo:**
1. Chạy script `02_ETL_Synthea_Extract.py`
2. Tạo schema Staging (`03_Schema_Staging.sql`)
3. Build transformation logic từ Landing → Staging

---

**Version**: 1.0  
**Last Updated**: 2026-04-15
