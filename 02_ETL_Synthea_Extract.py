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
from typing import Dict
import traceback
from tqdm import tqdm

# Tắt warnings
pd.options.mode.copy_on_write = True
import warnings
warnings.filterwarnings('ignore')

# Load environment variables from .env (optional)
from dotenv import load_dotenv
load_dotenv()


# ============================================================================
# CONFIG
# ============================================================================

class Config:
    """Cấu hình cho ETL"""
    
    # Đường dẫn
    PROJECT_ROOT = Path(__file__).parent
    CSV_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "synthea" / "csv"
    LOG_PATH = PROJECT_ROOT / "logs"
    
    # SQL Server - Windows Authentication ONLY
    SQL_SERVER = os.environ.get('SYNTHEA_SQL_SERVER')
    if not SQL_SERVER:
        raise RuntimeError("SYNTHEA_SQL_SERVER is not set. Copy .env.example -> .env and edit.")
    
    LANDING_DB = "DW_Synthea_Landing"
    
    # Batch processing
    CHUNK_SIZE = 200000  # 200K rows per chunk
    BATCH_SIZE = 20000   # 20K rows per batch insert
    
    # CSV Files mapping (table_key -> csv_filename)
    CSV_FILES = {
        'Patients': 'patients.csv',
        'Encounters': 'encounters.csv',
        'Conditions': 'conditions.csv',
        'Medications': 'medications.csv',
        'Observations': 'observations.csv',
        'Procedures': 'procedures.csv',
        'Immunizations': 'immunizations.csv',
        'Allergies': 'allergies.csv',
        'Careplans': 'careplans.csv',
        'Devices': 'devices.csv',
        'Imaging_Studies': 'imaging_studies.csv',
        'Supplies': 'supplies.csv',
        'Organizations': 'organizations.csv',
        'Providers': 'providers.csv',
        'Payers': 'payers.csv',
        'Payer_Transitions': 'payer_transitions.csv'
    }


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

def reset_landing_tables():
    """TRUNCATE tất cả Landing tables"""
    
    logger.info("\nResetting Landing tables (TRUNCATE)...")
    
    try:
        conn = create_connection(Config.LANDING_DB)
        cursor = conn.cursor()

        cursor.execute("SELECT @@SERVERNAME, DB_NAME()")
        server, db = cursor.fetchone()
        logger.info(f"  [DEBUG] Connected to server={server}, db={db}")
        
        tables = [f"Landing_{key}" for key in Config.CSV_FILES.keys()]
        
        for table in tables:
            try:
                cursor.execute(f"TRUNCATE TABLE dbo.{table}")
                logger.info(f"  [OK] Truncated {table}")
            except Exception as e:
                logger.warning(f"  [SKIP] {table}: {str(e)}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Reset completed.")
        
    except Exception as e:
        logger.error(f"Error resetting tables: {str(e)}")
        raise


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
        
        logger.info(f"  Rows in CSV: {len(df):,}")
        logger.info(f"  Columns: {len(df.columns)}")
        
        conn = create_connection(Config.LANDING_DB)
        cursor = conn.cursor()

        # 🔥 FIX: đảm bảo context đúng
        cursor.execute("USE DW_Synthea_Landing")

        # 🔥 DEBUG HARD CHECK
        cursor.execute(f"SELECT TOP 1 * FROM dbo.{table_name}")
        logger.info("  [DEBUG] Table accessible OK")

        # 🔥 FIX: tăng tốc executemany
        cursor.fast_executemany = True
        
        columns = list(df.columns)
        col_str = ", ".join(f"[{col}]" for col in columns)
        placeholders = ", ".join("?" * len(columns))
        
        # 🔥 FIX QUAN TRỌNG NHẤT
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
        
        logger.info(f"  [OK] Loaded {rows_inserted:,} rows")
        
    except Exception as e:
        result['error'] = str(e)
        logger.error(f"  [ERROR] {str(e)}")
        logger.error(traceback.format_exc())
    
    finally:
        result['end_time'] = datetime.now()
    
    return result


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def run_extract_pipeline():
    """Chạy toàn bộ Extract pipeline"""
    
    logger.info("\n" + "="*80)
    logger.info("STARTING EXTRACT PIPELINE")
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
        logger.error("Vui lòng kiểm tra:")
        logger.error("  1. SQL Server đang chạy")
        logger.error("  2. Database DW_Synthea_Landing đã được tạo")
        logger.error("  3. Windows Authentication có quyền truy cập")
        sys.exit(1)
    
    # Step 1: Reset tables
    logger.info("\n[STEP 1] Resetting Landing tables...")
    reset_landing_tables()
    
    # Step 2: Load CSV files
    logger.info("\n[STEP 2] Loading CSV files to Landing...")
    
    results = []
    
    for table_key, csv_file in Config.CSV_FILES.items():
        result = load_to_landing(table_key, csv_file)
        results.append(result)
    
    # Step 3: Summary
    logger.info("\n" + "="*80)
    logger.info("EXTRACT SUMMARY")
    logger.info("="*80)
    
    success_count = len([r for r in results if r['status'] == 'SUCCESS'])
    failed_count = len([r for r in results if r['status'] == 'FAILED'])
    total_rows = sum(r['rows_loaded'] for r in results if r['status'] == 'SUCCESS')
    
    logger.info(f"\nSuccessful: {success_count}/{len(results)}")
    logger.info(f"Failed:     {failed_count}/{len(results)}")
    logger.info(f"Total rows: {total_rows:,}\n")
    
    logger.info(f"{'Table':<30} {'Rows':>15} {'Duration':>10} {'Status':>10}")
    logger.info("-" * 70)
    
    for result in results:
        duration = (result['end_time'] - result['start_time']).total_seconds()
        
        if result['status'] == 'SUCCESS':
            logger.info(
                f"{result['table']:<30} {result['rows_loaded']:>15,} "
                f"{duration:>9.2f}s {result['status']:>10}"
            )
        else:
            logger.info(
                f"{result['table']:<30} {'ERROR':>15} "
                f"{duration:>9.2f}s {result['status']:>10}"
            )
    
    logger.info("\n" + "="*80)
    
    if failed_count > 0:
        logger.warning("MỘT SỐ TABLE LOAD LỖI - Kiểm tra log!")
        logger.info("\nNext steps:")
        logger.info("  1. Kiểm tra file CSV trong thư mục data/raw/synthea/csv/")
        logger.info("  2. Xem chi tiết lỗi trong log file")
        logger.info("  3. Sửa lỗi và chạy lại script")
    else:
        logger.info("EXTRACT COMPLETED SUCCESSFULLY!")
        logger.info("\nNext step:")
        logger.info("  Run: python 05_Transform_Landing_to_Staging.py")
    
    logger.info("="*80)


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        # Kiểm tra CSV path
        if not Config.CSV_DATA_PATH.exists():
            logger.error(f"CSV folder không tồn tại: {Config.CSV_DATA_PATH}")
            logger.error("Vui lòng tạo folder và copy CSV files vào đó")
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