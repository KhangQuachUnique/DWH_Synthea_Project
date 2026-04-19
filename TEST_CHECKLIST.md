# Synthea DWH Incremental ETL - Test Checklist

## 1. Chuẩn bị môi trường

- [ ] Đảm bảo đã tạo đủ 3 database: DW_Synthea_Landing, DW_Synthea_Staging, DW_Synthea_DWH
- [ ] Chạy các script schema: 01_Schema_Landing.sql, 03_Schema_Staging.sql, 06_Schema_DWH_v2.sql
- [ ] Đảm bảo bảng ETL_Control (Landing) và ETL_Run_Log (Landing, DWH) đã tồn tại
- [ ] Copy đủ 16 file CSV vào data/raw/synthea/csv/
- [ ] Cài Python packages: pandas, pyodbc, tqdm, python-dotenv
- [ ] Cấu hình .env với SYNTHEA_SQL_SERVER đúng

## 2. Test ETL Extract (Landing)

- [ ] Chạy: `python 02_ETL_Synthea_Extract.py`
- [ ] Kiểm tra log extract trong logs/
- [ ] Kiểm tra bảng Landing\_\* đã có batch_id, create_at, update_at
- [ ] Kiểm tra ETL_Control cập nhật LastBatchId, LastLoadedAt, RowsLoaded
- [ ] Kiểm tra ETL_Run_Log ghi nhận extract_pipeline (SUCCESS/FAILED)

## 3. Test Transform Landing -> Staging (Incremental)

- [ ] Chạy: `python 05_Transform_Landing_to_Staging.py`
- [ ] Kiểm tra Staging\_\* đã upsert đúng (không mất dữ liệu cũ, chỉ update/insert mới)
- [ ] Kiểm tra log transform (nếu có)

## 4. Test Load DWH (Incremental, Logging)

- [ ] Chạy proc: `EXEC DW_Synthea_DWH.dbo.usp_run_dwh_load @FromDate, @ToDate, @AsOfDate, @batch_id`
- [ ] Kiểm tra các bảng dim, fact đã cập nhật đúng, không trùng lặp
- [ ] Kiểm tra ETL_Run_Log (DWH) có log từng proc, batch_id, trạng thái
- [ ] Test chạy lại cùng batch_id: không load lại (idempotent)

## 5. Test lại incremental

- [ ] Thêm mới 1 dòng vào 1 file CSV, chạy lại extract + transform + DWH load
- [ ] Kiểm tra chỉ có dữ liệu mới được thêm vào các layer, không bị duplicate

## 6. Kiểm tra lỗi

- [ ] Xóa 1 file CSV, chạy lại extract: phải báo lỗi, log FAILED
- [ ] Sửa sai cấu trúc 1 file CSV, chạy lại extract: phải báo lỗi, log FAILED

## 7. Tổng kết

- [ ] Đảm bảo mọi bước đều log trạng thái, batch_id, thời gian, rows
- [ ] Đảm bảo pipeline chạy lại không bị duplicate, không mất dữ liệu cũ

---

Nếu có lỗi, kiểm tra logs/ và bảng ETL_Run_Log để truy vết nguyên nhân.
