"""
================================================================================
SYNTHEA ETL SCRIPT - Loading CSV to SQL Server Landing
================================================================================
Mục đích: Load 16 file CSV Synthea vào Landing Database (dữ liệu thô)

Yêu cầu: 
  - SQL Server đã có database DW_Synthea_Landing (chạy 01_Schema_Landing.sql trước)
  - Python packages: pip install pandas pyodbc tqdm

Chạy: python 02_ETL_Synthea_Extract.py
================================================================================
"""

import os
import sys
import pandas as pd
import pyodbc
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import traceback
from tqdm import tqdm

# Tắt warnings
pd.options.mode.copy_on_write = True
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# CONFIG
# ============================================================================

class Config:
    """Cấu hình cho ETL"""
    
    # Đường dẫn
    PROJECT_ROOT = Path(__file__).parent
    LOG_PATH = PROJECT_ROOT / "logs"
    
    # SQL Server - Windows Authentication ONLY
    SQL_SERVER = "localhost"
    
    LANDING_DB = "DW_Synthea_Landing"
    
    # Batch processing
    CHUNK_SIZE = 200000
    BATCH_SIZE = 30000   
    # CSV Files mapping (table_key -> csv_filename)
    CSV_FILES = {
        'Patients': 'patients.csv',
        'Encounters': 'encounters.csv',
        'Conditions': 'conditions.csv',
        'Medications': 'medications.csv',
        'Organizations': 'organizations.csv',
        'Providers': 'providers.csv',
        'Payers': 'payers.csv',
        'Payer_Transitions': 'payer_transitions.csv'
    }

    # Ưu tiên path mới, vẫn giữ fallback path cũ để tương thích ngược
    CSV_PATH_CANDIDATES = [
        PROJECT_ROOT / "data",
        PROJECT_ROOT / "data" / "raw" / "synthea" / "csv",
        PROJECT_ROOT / "data" / "raw" / "synthea",
        PROJECT_ROOT / "data" / "csv"
    ]
    CSV_DATA_PATH = None


def get_csv_search_paths() -> List[Path]:
    """Danh sách path dùng để dò CSV (hỗ trợ override qua ENV)."""
    paths: List[Path] = []

    env_csv_path = os.getenv("SYNTHEA_CSV_PATH", "").strip()
    if env_csv_path:
        paths.append(Path(env_csv_path).expanduser())

    paths.extend(Config.CSV_PATH_CANDIDATES)
    return paths


def resolve_csv_data_path() -> Path:
    """Tự động chọn thư mục CSV hợp lệ theo danh sách ưu tiên."""
    required_csv = {name.lower() for name in Config.CSV_FILES.values()}
    search_paths = get_csv_search_paths()

    # Ưu tiên thư mục có chứa ít nhất 1 file CSV cần dùng của pipeline.
    for candidate in search_paths:
        if candidate.exists() and candidate.is_dir():
            available_csv = {p.name.lower() for p in candidate.glob("*.csv")}
            if required_csv.intersection(available_csv):
                return candidate

    # Fallback: thư mục nào tồn tại thì dùng thư mục đó.
    for candidate in search_paths:
        if candidate.exists() and candidate.is_dir():
            return candidate

    # Nếu chưa có thư mục nào, giữ path đầu tiên để hiển thị thông báo lỗi rõ ràng.
    return search_paths[0]


Config.CSV_DATA_PATH = resolve_csv_data_path()


# ============================================================================
# LOGGING
# ============================================================================

def setup_logging():
    """Thiết lập logging"""
    Config.LOG_PATH.mkdir(parents=True, exist_ok=True)
    
    log_format = '%(asctime)s | %(levelname)-8s | %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    log_file = Config.LOG_PATH / f"extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("="*80)
    logger.info("SYNTHEA ETL - EXTRACT PROCESS")
    logger.info(f"CSV Path: {Config.CSV_DATA_PATH}")
    logger.info(f"Target DB: {Config.LANDING_DB}")
    logger.info("="*80)
    
    return logger


logger = setup_logging()


# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_odbc_driver() -> str:
    """Tự động chọn ODBC Driver tốt nhất"""
    installed = pyodbc.drivers()
    
    # Ưu tiên driver mới nhất
    for preferred in ['ODBC Driver 18 for SQL Server', 'ODBC Driver 17 for SQL Server']:
        if preferred in installed:
            return preferred
    
    # Fallback: driver nào có "SQL Server"
    for driver in reversed(installed):
        if 'SQL Server' in driver:
            return driver
    
    raise Exception(f"Không tìm thấy ODBC Driver. Installed: {installed}")


def create_connection(database: str = None):
    """Tạo connection với Windows Authentication"""
    try:
        driver = get_odbc_driver()
        
        conn_str = f"DRIVER={{{driver}}};SERVER={Config.SQL_SERVER};"
        
        if database:
            conn_str += f"DATABASE={database};"
        
        conn_str += "Trusted_Connection=yes;"
        
        # Thêm encryption settings cho ODBC 18
        if 'Driver 18' in driver:
            conn_str += "Encrypt=no;TrustServerCertificate=yes;"
        
        connection = pyodbc.connect(conn_str, timeout=30)
        connection.autocommit = True  # Manual commit
        
        return connection
        
    except Exception as e:
        logger.error(f"Lỗi kết nối database {database}: {str(e)}")
        raise


# ============================================================================
# RESET LANDING TABLES
# ============================================================================


# ============================================================================
# WATERMARK & ETL CONTROL
# ============================================================================

import uuid

def get_watermark(table_key: str):
    """Đọc LastLoadedAt từ ETL_Control"""
    conn = create_connection(Config.LANDING_DB)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT ISNULL(LastLoadedAt, '1900-01-01') FROM dbo.ETL_Control WHERE TableName = ?",
        [f'Landing_{table_key}']
    )
    row = cursor.fetchone()
    cursor.close(); conn.close()
    return row[0] if row else datetime(1900, 1, 1)

def update_watermark(table_key: str, batch_id: str, rows: int):
    conn = create_connection(Config.LANDING_DB)
    cursor = conn.cursor()
    cursor.execute(
        """MERGE dbo.ETL_Control AS t
           USING (SELECT ? AS TableName, ? AS LastBatchId,
                         GETDATE() AS LastLoadedAt, ? AS RowsLoaded) AS s
           ON t.TableName = s.TableName
           WHEN MATCHED THEN
             UPDATE SET LastBatchId=s.LastBatchId, LastLoadedAt=s.LastLoadedAt,
                        RowsLoaded=s.RowsLoaded, Status='SUCCESS'
           WHEN NOT MATCHED THEN
             INSERT (TableName,LastBatchId,LastLoadedAt,RowsLoaded)
             VALUES (s.TableName,s.LastBatchId,s.LastLoadedAt,s.RowsLoaded);""",
        [f'Landing_{table_key}', batch_id, rows]
    )
    conn.commit(); cursor.close(); conn.close()

def get_existing_count(table_name: str):
    conn = create_connection(Config.LANDING_DB)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
    count = cursor.fetchone()[0]
    cursor.close(); conn.close()
    return count


# ============================================================================
# LOAD CSV TO LANDING
# ============================================================================


def load_to_landing(table_key: str, csv_filename: str) -> Dict:
    result = {
        'table': f'Landing_{table_key}',
        'status': 'FAILED',
        'rows_loaded': 0,
        'error': None,
        'start_time': datetime.now(),
        'end_time': None
    }
    csv_path = Config.CSV_DATA_PATH / csv_filename
    table_name = f'Landing_{table_key}'
    try:
        if not csv_path.exists():
            raise FileNotFoundError(f"File không tồn tại: {csv_path}")

        logger.info(f"\n[Loading] {table_name}")
        logger.info(f"  File: {csv_filename}")

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        df = df.where(pd.notna(df), None)

        df['create_at'] = datetime.now()
        df['update_at'] = datetime.now()
        batch_id = str(uuid.uuid4())
        df['batch_id'] = batch_id

        logger.info(f"  Rows in CSV: {len(df):,}")
        logger.info(f"  Columns: {len(df.columns)}")

        # So sánh số lượng dòng để xác định có cần load không
        existing_count = get_existing_count(table_name)
        if len(df) == existing_count:
            logger.info(f"  [SKIP] {table_name}: no new rows (rowcount matches)")
            result['status'] = 'SKIP'
            result['rows_loaded'] = 0
            update_watermark(table_key, batch_id, 0)
            result['end_time'] = datetime.now()
            return result

        conn = create_connection(Config.LANDING_DB)
        cursor = conn.cursor()
        cursor.fast_executemany = True

        columns = list(df.columns)
        col_str = ", ".join(f"[{col}]" for col in columns)
        placeholders = ", ".join("?" * len(columns))
        insert_sql = f"INSERT INTO dbo.{table_name} ({col_str}) VALUES ({placeholders})"

        total_rows = len(df)
        rows_inserted = 0

        with tqdm(total=total_rows, desc="  Inserting", unit="rows") as pbar:
            for batch_start in range(0, total_rows, Config.BATCH_SIZE):
                batch_end = min(batch_start + Config.BATCH_SIZE, total_rows)
                batch_df = df.iloc[batch_start:batch_end]
                rows = [tuple(row) for row in batch_df.values]
                cursor.executemany(insert_sql, rows)
                rows_inserted += len(rows)
                pbar.update(len(rows))

        cursor.close()
        conn.close()

        result['status'] = 'SUCCESS'
        result['rows_loaded'] = rows_inserted
        logger.info(f"  [OK] Loaded {rows_inserted:,} rows (batch_id={batch_id})")
        update_watermark(table_key, batch_id, rows_inserted)

    except Exception as e:
        result['error'] = str(e)
        logger.error(f"  [ERROR] {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        result['end_time'] = datetime.now()
    return result



# ===================== MAIN PIPELINE + ETL_Run_Log =====================
import uuid
def log_etl_run(proc_name, batch_id, status, rows_affected, error_msg=None, params=None, db='DW_Synthea_Landing'):
    try:
        conn = create_connection(db)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO dbo.ETL_Run_Log(proc_name, batch_id, start_time, end_time, status, rows_affected, error_msg, params)
            VALUES (?, ?, SYSUTCDATETIME(), SYSUTCDATETIME(), ?, ?, ?, ?)
            """,
            [proc_name, str(batch_id), status, rows_affected, error_msg, params]
        )
        conn.commit()
        cursor.close(); conn.close()
    except Exception as e:
        logger.error(f"[ETL_Run_Log] {str(e)}")

def run_extract_pipeline():
    """Chạy toàn bộ Extract pipeline (incremental, không truncate) + log ETL_Run_Log"""
    batch_id = uuid.uuid4()
    logger.info("\n" + "="*80)
    logger.info(f"STARTING EXTRACT PIPELINE (INCREMENTAL) | batch_id={batch_id}")
    logger.info("="*80)

    # Step 0: Check connection
    logger.info("\n[STEP 0] Checking database connection...")
    try:
        conn = create_connection(Config.LANDING_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME()")
        db_name = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        logger.info(f"[OK] Connected to '{db_name}' on '{Config.SQL_SERVER}'")
    except Exception as e:
        logger.error(f"[ERROR] Cannot connect to database: {str(e)}")
        log_etl_run('extract_pipeline', batch_id, 'FAILED', 0, str(e), 'Check connection')
        sys.exit(1)

    # Step 1: Load CSV files (incremental)
    logger.info("\n[STEP 1] Loading CSV files to Landing (incremental)...")
    results = []
    for table_key, csv_file in Config.CSV_FILES.items():
        result = load_to_landing(table_key, csv_file)
        results.append(result)

    # Step 2: Summary
    logger.info("\n" + "="*80)
    logger.info("EXTRACT SUMMARY")
    logger.info("="*80)

    success_count = len([r for r in results if r['status'] == 'SUCCESS'])
    skipped_count = len([r for r in results if r['status'] == 'SKIP'])
    failed_count = len([r for r in results if r['status'] == 'FAILED'])
    total_rows = sum(r['rows_loaded'] for r in results if r['status'] == 'SUCCESS')

    logger.info(f"\nSuccessful: {success_count}/{len(results)}")
    logger.info(f"Skipped:    {skipped_count}/{len(results)}")
    logger.info(f"Failed:     {failed_count}/{len(results)}")
    logger.info(f"Total rows: {total_rows:,}\n")

    logger.info(f"{'Table':<30} {'Rows':>15} {'Duration':>10} {'Status':>10}")
    logger.info("-" * 70)

    for result in results:
        duration = (result['end_time'] - result['start_time']).total_seconds()
        logger.info(
            f"{result['table']:<30} {result['rows_loaded']:>15,} "
            f"{duration:>9.2f}s {result['status']:>10}"
        )

    logger.info("\n" + "="*80)

    if failed_count > 0:
        logger.warning("MỘT SỐ TABLE LOAD LỖI - Kiểm tra log!")
        logger.info("\nNext steps:")
        logger.info(f"  1. Kiểm tra file CSV trong thư mục {Config.CSV_DATA_PATH}")
        logger.info("  2. Xem chi tiết lỗi trong log file")
        logger.info("  3. Sửa lỗi và chạy lại script")
        log_etl_run('extract_pipeline', batch_id, 'FAILED', total_rows, 'Some tables failed', f'Success={success_count};Failed={failed_count}')
    else:
        logger.info("EXTRACT COMPLETED SUCCESSFULLY!")
        logger.info("\nNext step:")
        logger.info("  Run: python 05_Transform_Landing_to_Staging.py")
        log_etl_run('extract_pipeline', batch_id, 'SUCCESS', total_rows, None, f'Success={success_count};Skipped={skipped_count}')
    logger.info("="*80)

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    print(f"--- PYTHON EXECUTABLE: {sys.executable} ---")
    try:
        search_paths = get_csv_search_paths()

        # Kiểm tra CSV path
        if not Config.CSV_DATA_PATH.exists():
            logger.error(f"CSV folder không tồn tại: {Config.CSV_DATA_PATH}")
            logger.error("Các path đã thử:")
            for p in search_paths:
                logger.error(f"  - {p}")
            sys.exit(1)
        
        # Kiểm tra có file CSV
        csv_files = list(Config.CSV_DATA_PATH.glob("*.csv"))
        if not csv_files:
            logger.error(f"Không tìm thấy file CSV trong: {Config.CSV_DATA_PATH}")
            sys.exit(1)
        
        logger.info(f"Found {len(csv_files)} CSV files")
        
        # Run pipeline
        run_extract_pipeline()
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"\n[FATAL ERROR] {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)