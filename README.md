# Synthea ETL Project - Landing to Staging

Dự án ETL đơn giản để load dữ liệu Synthea từ CSV vào SQL Server:
- **Landing Layer**: Dữ liệu thô (VARCHAR) từ CSV
- **Staging Layer**: Dữ liệu đã clean và type đúng (DATE, DECIMAL, INT)

## Cài đặt và Chạy

### 1. Clone Repository
```bash
git clone <your-repo-url>
cd DWH
```

### 2. Cài đặt Dependencies
```bash
python -m pip install -r requirements.txt
```

### 3. Cấu hình SQL Server
- Copy file `.env.example` thành `.env`
- Sửa các giá trị trong `.env` cho SQL Server của bạn:

```env
SYNTHEA_SQL_SERVER=localhost          # Thay bằng server name của bạn
SYNTHEA_ODBC_DRIVER=ODBC Driver 17 for SQL Server
```

**Lưu ý**: Nếu dùng SQL Server local, thường là `localhost` hoặc `.\SQLEXPRESS`

### 4. Chuẩn bị Dữ liệu
- Đặt 16 file CSV Synthea vào thư mục `data/raw/synthea/csv/`
- File cần có: allergies.csv, careplans.csv, conditions.csv, devices.csv, encounters.csv, imaging_studies.csv, immunizations.csv, medications.csv, observations.csv, organizations.csv, patients.csv, payer_transitions.csv, payers.csv, procedures.csv, providers.csv, supplies.csv

### 5. Tạo Database Schema
Chạy 2 file SQL trong SQL Server Management Studio (SSMS):
- `01_Schema_Landing.sql` - Tạo database DW_Synthea_Landing
- `03_Schema_Staging.sql` - Tạo database DW_Synthea_Staging

### 6. Chạy ETL
```bash
# Load CSV vào Landing
python 02_ETL_Synthea_Extract.py

# Transform Landing thành Staging
python 05_Transform_Landing_to_Staging.py
```

### 7. Validation
```bash
# Kiểm tra setup
python 04_Validation.py
```

## Flow Chi Tiết

```
CSV Files → Landing Tables (VARCHAR) → Staging Tables (Typed)
     ↓              ↓                        ↓
  Raw Data    + Audit Columns          + Data Quality
              (create_at, update_at)     (NULL handling,
                                         type conversion)
```

## Troubleshooting

### Lỗi "Login failed for user"
- Kiểm tra SQL Server đang chạy
- Đảm bảo user Windows có quyền access database
- Hoặc set SQL authentication trong `.env`

### Lỗi "Cannot open database"
- Chạy lại 2 file SQL schema trong SSMS
- Kiểm tra tên database trong code

### Lỗi ODBC Driver
- Cài đặt Microsoft ODBC Driver for SQL Server
- Thay đổi `SYNTHEA_ODBC_DRIVER` trong `.env`

## Tech Stack
- Python 3.9+
- SQL Server 2019+
- pandas, pyodbc, tqdm
