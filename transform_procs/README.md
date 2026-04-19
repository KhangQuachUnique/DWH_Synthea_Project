# Transform Landing to Staging Procedures

- Chứa các stored procedure chuẩn hóa, làm sạch dữ liệu từ Landing sang Staging.
- Mỗi file .sql là 1 proc cho 1 bảng (Patients, Encounters, Conditions, ...).
- Chạy file run_all_transform_procs.sql để thực thi toàn bộ pipeline transform.

## Thêm mới proc

- Copy mẫu, sửa tên bảng/cột/kiểu dữ liệu phù hợp.
- Thêm EXEC vào run_all_transform_procs.sql để tự động hóa.
