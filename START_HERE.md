# 🚀 SYNTHEA ETL - BẮT ĐẦU SỬ DỤNG

## ⚡ Bắt Đầu Nhanh (5 Phút)

**Chọn bước đầu tiên của bạn:**

### 🪟 Windows Users
```bash
Double-click → 00_Quick_Start.bat
```

### 🐧 macOS/Linux Users
```bash
python 04_Validation.py
python 02_ETL_Synthea_Extract.py
python 05_Transform_Landing_to_Staging.py
```

---

## 📊 Data Flow Architecture

```
CSV Files (505 MB, 2.7M rows)
    ↓ [Extract]
Landing DB (Raw Data - VARCHAR)
├─ create_at, update_at (audit)
├─ No transformation
└─ Exact copy from CSV
    ↓ [Transform & Clean]
Staging DB (Typed Data - DATE, DECIMAL, INT)
├─ Data cleaned & validated
├─ create_at, update_at (audit)
└─ Ready for DW
    ↓ [Your Business Logic]
Kimball Star Schema
└─ Your responsibility
```

---

## 📋 Danh Sách File ETL (8 File)

| File | Loại | Mục Đích |
|-----|------|---------|
| **00_Quick_Start.bat** | Script | ⚡ Cài đặt (Windows) |
| **01_Schema_Landing.sql** | SQL | 💾 Landing DB (raw) |
| **02_ETL_Synthea_Extract.py** | Python | 🔥 CSV → Landing |
| **03_Schema_Staging.sql** | SQL | 💾 Staging DB (typed) |
| **04_Validation.py** | Python | ✅ Kiểm tra setup |
| **05_Transform_Landing_to_Staging.py** | Python | ✨ Landing → Staging |
| **requirements.txt** | Config | 📦 Packages |
| **FLOW_DOCUMENTATION.md** | Docs | 📖 Chi tiết flow |

---

## 🎯 Quy Trình 7 Bước

### 📌 [Bước 1] Chuẩn Bị Máy (5 phút)

Kiểm tra:
- ✅ Python 3.8+ 
- ✅ SQL Server running
- ✅ ODBC Driver 17
- ✅ CSV files ở `data/raw/synthea/csv/`

### 📌 [Bước 2] Cài Python Packages (2 phút)

```bash
pip install -r requirements.txt
```

### 📌 [Bước 3] Kiểm Tra Setup (1 phút)

```bash
python 04_Validation.py
# Output: ✓ all checks passed
```

### 📌 [Bước 4] Tạo Landing Database (1 phút)

```sql
-- SSMS → Open 01_Schema_Landing.sql → F5
```

### 📌 [Bước 5] Extract: CSV → Landing (2-3 phút) 🔥

```bash
python 02_ETL_Synthea_Extract.py
# ~2.7M rows → Landing_* tables
# Auto audit columns: create_at, update_at
```

### 📌 [Bước 6] Tạo Staging Database (1 phút)

```sql
-- SSMS → Open 03_Schema_Staging.sql → F5
```

### 📌 [Bước 7] Transform: Landing → Staging (5-10 phút) ✨

```bash
python 05_Transform_Landing_to_Staging.py
# Data clean, type conversion
# VARCHAR → DATE, DECIMAL, INT
# Insert into Staging_* tables
```

**✅ XONG! Data sẵn sàng cho DW**

---

## 🔄 Audit Columns

Mỗi row có 2 cột để tracking:

```sql
create_at  => Khi extract từ CSV
update_at  => Khi transform gần nhất
```

---

## 📖 Documentation

📍 **Bắt đầu:** [FLOW_DOCUMENTATION.md](FLOW_DOCUMENTATION.md)  
📍 **Chi tiết:** [FILE_MANIFEST.md](FILE_MANIFEST.md)  
📍 **Hướng dẫn:** [README_ETL.md](README_ETL.md)  

---

## 🚀 Start Now!

### Windows
```
→ Double-click: 00_Quick_Start.bat
```

### macOS/Linux
```bash
python 04_Validation.py
python 02_ETL_Synthea_Extract.py
python 05_Transform_Landing_to_Staging.py
```

---

**Version**: 2.0 | **Status**: ✅ Ready
