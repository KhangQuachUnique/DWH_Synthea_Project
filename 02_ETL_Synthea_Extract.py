"""
================================================================================
SYNTHEA ETL SCRIPT - Loading CSV to SQL Server (Landing & Staging)
================================================================================
Mục đích: Load 16 file CSV Synthea vào Landing (dữ liệu thô)
         và Staging (dữ liệu transformation)

Tác giả: Data Engineering Team
Ngày tạo: 2026-04-15
Version: 1.0

Yêu cầu: pip install pandas pyodbc sqlalchemy tqdm openpyxl
================================================================================
"""

import os
import sys
import pandas as pd
import pyodbc
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import traceback
from tqdm import tqdm

# Tắt warnings
pd.options.mode.copy_on_write = True
import warnings
warnings.filterwarnings('ignore')


# ============================================================================
# CONFIG SECTION - CẤU HÌNH ĐƯỜNG DẪN VÀ KẾT NỐI
# ============================================================================

class Config:
    """Class cấu hình chung cho ETL"""

    # Đường dẫn
    PROJECT_ROOT = Path(__file__).parent
    CSV_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "synthea" / "csv"
    LOG_PATH = PROJECT_ROOT / "logs"
    ERROR_LOG_PATH = PROJECT_ROOT / "logs" / "errors"

    # SQL Server Configuration
    SQL_SERVER = "AKUMA"       # Your local SQL Server name
    SQL_USER = ""              # Để trống nếu dùng Windows Authentication
    SQL_PASSWORD = ""          # Để trống nếu dùng Windows Authentication
    USE_WINDOWS_AUTH = True    # True = Windows Auth, False = SQL Auth

    # Database names
    LANDING_DB = "DW_Synthea_Landing"
    STAGING_DB = "DW_Synthea_Staging"

    # Chunking parameters - Optimized for 23GB RAM + 12 cores
    CHUNK_SIZE = 200000  # 200K rows per chunk (you have plenty of RAM)
    BATCH_SIZE = 10000   # 10K rows per batch (fast insert)

    # File list - Danh sách 16 file cần load
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
# LOGGING SETUP - CẤU HÌNH LOG
# ============================================================================

def setup_logging() -> None:
    """Thiết lập logging cho toàn bộ ETL process"""

    # Tạo folder log nếu chưa tồn tại
    Config.LOG_PATH.mkdir(parents=True, exist_ok=True)
    Config.ERROR_LOG_PATH.mkdir(parents=True, exist_ok=True)

    # Định dạng log
    log_format = '%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # File log chính
    log_file = Config.LOG_PATH / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # Cấu hình logging
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info("="*80)
    logger.info("SYNTHEA ETL PROCESS STARTED")
    logger.info(f"Log file: {log_file}")
    logger.info(f"CSV data path: {Config.CSV_DATA_PATH}")
    logger.info("="*80)

    return logger


logger = setup_logging()


# ============================================================================
# DATABASE CONNECTION - KẾT NỐI CSDL
# ============================================================================

def get_connection_string(database_name: str = None, use_windows_auth: bool = True) -> str:
    """
    Tạo connection string cho SQL Server

    Args:
        database_name: Tên database (None = master)
        use_windows_auth: True = Windows Auth, False = SQL Auth

    Returns:
        Connection string
    """

    if use_windows_auth:
        # Windows Authentication
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={Config.SQL_SERVER};"
        if database_name:
            conn_str += f"DATABASE={database_name};"
        conn_str += "Trusted_Connection=yes;"
    else:
        # SQL Server Authentication
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={Config.SQL_SERVER};"
        conn_str += f"UID={Config.SQL_USER};PWD={Config.SQL_PASSWORD};"
        if database_name:
            conn_str += f"DATABASE={database_name};"

    return conn_str


def create_connection(database_name: str = None):
    """Tạo connection object"""
    try:
        conn_str = get_connection_string(database_name, Config.USE_WINDOWS_AUTH)
        connection = pyodbc.connect(conn_str, timeout=30, autocommit=True)
        return connection
    except Exception as e:
        logger.error(f"Connection failed to {database_name or 'master'}: {str(e)}")
        raise


def execute_sql(sql_query: str, database_name: str = None, fetch: bool = False):
    """
    Thực thi SQL query

    Args:
        sql_query: SQL command
        database_name: Database name
        fetch: True = fetch results

    Returns:
        Results (nếu fetch=True)
    """
    conn = None
    try:
        conn = create_connection(database_name)
        cursor = conn.cursor()
        cursor.execute(sql_query)

        if fetch:
            result = cursor.fetchall()
            cursor.close()
            return result

        conn.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"SQL Execution Error: {str(e)}\nQuery: {sql_query[:200]}")
        raise
    finally:
        if conn:
            conn.close()


# ============================================================================
# DATABASE SETUP - TẠO DATABASE NẾU CHƯA CÓ
# ============================================================================

def create_database(database_name: str) -> bool:
    """Tạo database nếu chưa tồn tại"""

    try:
        # Kiểm tra database đã tồn tại chưa
        check_sql = f"""
        SELECT COUNT(*) AS cnt FROM sys.databases WHERE name = '{database_name}'
        """
        result = execute_sql(check_sql, "master", fetch=True)

        if result[0][0] > 0:
            logger.info(f"[OK] Database '{database_name}' already exists")
            return True

        # Tạo database mới
        create_sql = f"""
        CREATE DATABASE [{database_name}]
        ON PRIMARY (
            NAME = '{database_name}_Data',
            FILENAME = 'C:\\SQLServerData\\{database_name}.mdf',
            SIZE = 1024MB,
            FILEGROWTH = 512MB
        )
        LOG ON (
            NAME = '{database_name}_Log',
            FILENAME = 'C:\\SQLServerData\\{database_name}.ldf',
            SIZE = 256MB,
            FILEGROWTH = 256MB
        );
        """

        execute_sql(create_sql, "master")
        logger.info(f"[OK] Database '{database_name}' created successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to create database '{database_name}': {str(e)}")
        return False


def reset_landing_tables() -> bool:
    """Xóa dữ liệu cũ từ Landing tables (TRUNCATE)"""
    try:
        conn = create_connection(Config.LANDING_DB)
        cursor = conn.cursor()

        for table_key, csv_file in Config.CSV_FILES.items():
            table_name = f"Landing_{table_key}"
            try:
                cursor.execute(f"TRUNCATE TABLE [{table_name}];")
                logger.info(f"[OK] Truncated {table_name}")
            except:
                pass  # Table might not exist yet

        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error truncating landing tables: {str(e)}")
        return False


def ensure_min_column_length(
    database_name: str,
    table_name: str,
    column_name: str,
    min_length: int,
    data_type: str = 'VARCHAR'
) -> bool:
    """Đảm bảo độ dài tối thiểu cho cột text để tránh truncate khi load."""

    try:
        check_sql = f"""
        SELECT CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM [{database_name}].INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo'
          AND TABLE_NAME = '{table_name}'
          AND COLUMN_NAME = '{column_name}'
        """
        result = execute_sql(check_sql, database_name, fetch=True)

        if not result:
            logger.warning(f"[WARN] Column not found: {table_name}.{column_name}")
            return False

        current_length = result[0][0]
        is_nullable = result[0][1]

        if current_length is None or current_length >= min_length:
            logger.info(f"[OK] {table_name}.{column_name} length = {current_length}, no change needed")
            return True

        nullable_sql = "NULL" if str(is_nullable).upper() == "YES" else "NOT NULL"
        alter_sql = (
            f"ALTER TABLE [dbo].[{table_name}] "
            f"ALTER COLUMN [{column_name}] {data_type}({min_length}) {nullable_sql};"
        )
        execute_sql(alter_sql, database_name)

        logger.info(
            f"[OK] Altered {table_name}.{column_name} from {current_length} to {min_length}"
        )
        return True

    except Exception as e:
        logger.error(
            f"[ERROR] Failed to ensure length for {table_name}.{column_name}: {str(e)}"
        )
        return False


def ensure_landing_schema_compatibility() -> bool:
    """Đồng bộ nhanh các cột dễ bị truncate trong Landing trước khi load."""

    checks = [
        ("Landing_Observations", "VALUE", 255),
        ("Landing_Imaging_Studies", "SOP_CODE", 64),
    ]

    all_ok = True
    for table_name, column_name, min_length in checks:
        ok = ensure_min_column_length(
            database_name=Config.LANDING_DB,
            table_name=table_name,
            column_name=column_name,
            min_length=min_length,
            data_type='VARCHAR'
        )
        all_ok = all_ok and ok

    return all_ok


# ============================================================================
# LANDING LAYER - LOAD RAW DATA (VỚI KIỂU VARCHAR CHO TẤT CẢ CỘT)
# ============================================================================

def load_to_landing(table_key: str, csv_file: str) -> Dict:
    """
    Load CSV file vào Landing table (dữ liệu thô)

    Args:
        table_key: Tên key (e.g., 'Patients')
        csv_file: Tên file CSV (e.g., 'patients.csv')

    Returns:
        Dict với thông tin load
    """

    result = {
        'table': f"Landing_{table_key}",
        'status': 'FAILED',
        'rows_loaded': 0,
        'error': None,
        'start_time': datetime.now(),
        'end_time': None
    }

    csv_path = Config.CSV_DATA_PATH / csv_file

    if not csv_path.exists():
        result['error'] = f"File not found: {csv_path}"
        logger.warning(f"[WARN] {result['error']}")
        return result

    try:
        # Lấy kích thước file
        file_size_mb = csv_path.stat().st_size / (1024 * 1024)
        logger.info(f"\n{'='*80}")
        logger.info(f"Loading {table_key:20} | File: {csv_file:25} | Size: {file_size_mb:.2f} MB")
        logger.info(f"{'='*80}")

        # Read CSV file
        df = pd.read_csv(csv_path, dtype=str, na_values=['', 'NA', 'NULL'])

        # Xử lý tên cột - loại bỏ khoảng trắng
        df.columns = df.columns.str.strip()

        # Convert empty strings to None (NULL in SQL)
        df = df.where(pd.notna(df), None)

        # Thêm cột audit - create_at & update_at
        now = datetime.now()
        df['create_at'] = now
        df['update_at'] = now

        logger.info(f"Total rows to load: {len(df):,}")

        # Load vào database với chunking (sử dụng pyodbc trực tiếp)
        total_rows = 0

        # Tạo connection
        conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={Config.SQL_SERVER};Database={Config.LANDING_DB};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()

        # Xây dựng INSERT statement
        cols = list(df.columns)
        col_str = ", ".join(f"[{col}]" for col in cols)
        placeholders = ", ".join("?" * len(cols))
        insert_sql = f"INSERT INTO [{result['table']}] ({col_str}) VALUES ({placeholders})"

        with tqdm(total=len(df), desc=f"Loading {table_key}", unit=" rows") as pbar:
            for batch_start in range(0, len(df), Config.BATCH_SIZE):
                batch_end = min(batch_start + Config.BATCH_SIZE, len(df))
                batch_df = df[batch_start:batch_end].copy()

                try:
                    # Convert to tuples và insert
                    rows = [tuple(row) for row in batch_df.values]
                    cursor.executemany(insert_sql, rows)
                    conn.commit()

                    rows_in_batch = len(batch_df)
                    total_rows += rows_in_batch
                    pbar.update(rows_in_batch)

                except Exception as e:
                    logger.error(f"Error loading batch {batch_start}-{batch_end}: {str(e)}")
                    conn.close()
                    raise

        cursor.close()
        conn.close()

        result['status'] = 'SUCCESS'
        result['rows_loaded'] = total_rows
        logger.info(f"[OK] Successfully loaded {total_rows:,} rows to {result['table']}")

    except Exception as e:
        result['error'] = str(e)
        logger.error(f"[ERROR] Failed to load {table_key}: {str(e)}")
        logger.error(traceback.format_exc())

    finally:
        result['end_time'] = datetime.now()

    return result


# ============================================================================
# STAGING LAYER - TRANSFORM DỮ LIỆU & KIỂU DATA TYPE ĐỐI
# ============================================================================

def transform_and_load_to_staging(table_key: str) -> Dict:
    """
    Transform dữ liệu từ Landing và load vào Staging với kiểu dữ liệu đúng

    Args:
        table_key: Tên table key (e.g., 'Patients')

    Returns:
        Dict với kết quả transformation
    """

    result = {
        'table': f"Staging_{table_key}",
        'status': 'SKIPPED',  # Staging load mặc định skip (sẽ implement chi tiết sau)
        'rows_inserted': 0,
        'error': None,
        'start_time': datetime.now(),
        'end_time': None
    }

    landing_table = f"Landing_{table_key}"

    try:
        # Placeholder: Staging tables sẽ được transform từ Landing
        # Phần này có thể mở rộng với transformation logic cụ thể

        logger.info(f"[INFO] Staging transformation for {table_key} - pending implementation")
        result['status'] = 'PENDING'

    except Exception as e:
        result['error'] = str(e)
        logger.error(f"[ERROR] Failed to transform {table_key}: {str(e)}")

    finally:
        result['end_time'] = datetime.now()

    return result


# ============================================================================
# MAIN ETL ORCHESTRATION - ĐIỀU PHỐI CHÍNH
# ============================================================================

def run_etl_pipeline() -> None:
    """
    Chạy toàn bộ ETL pipeline

    Flow:
    1. Tạo databases
    2. Reset Landing tables
    3. Load CSV to Landing (16 files)
    4. Transform và load to Staging
    5. Log results
    """

    logger.info("\n" + "="*80)
    logger.info("STARTING ETL PIPELINE")
    logger.info("="*80)

    # Step 1: Tạo databases
    logger.info("\n[STEP 1] Creating databases...")
    create_database(Config.LANDING_DB)
    create_database(Config.STAGING_DB)

    # Step 1.5: Ensure Landing schema is compatible with incoming CSV
    logger.info("\n[STEP 1.5] Validating Landing schema compatibility...")
    ensure_landing_schema_compatibility()

    # Step 2: Reset Landing tables
    logger.info("\n[STEP 2] Resetting landing tables...")
    reset_landing_tables()

    # Step 3: Load CSV files to Landing
    logger.info("\n[STEP 3] Loading CSV files to Landing layer...")
    landing_results = []

    for table_key, csv_file in Config.CSV_FILES.items():
        result = load_to_landing(table_key, csv_file)
        landing_results.append(result)

        # Log kết quả
        if result['status'] == 'SUCCESS':
            duration = (result['end_time'] - result['start_time']).total_seconds()
            logger.info(f"[OK] {result['table']:30} | {result['rows_loaded']:>10,} rows | {duration:>6.2f}s")
        else:
            logger.warning(f"[ERROR] {result['table']:30} | ERROR: {result['error']}")

    # Step 4: Transform to Staging (optional - placeholder)
    logger.info("\n[STEP 4] Transforming to Staging layer...")
    staging_results = []

    for table_key in Config.CSV_FILES.keys():
        result = transform_and_load_to_staging(table_key)
        staging_results.append(result)

    # Step 5: Summary Report
    logger.info("\n" + "="*80)
    logger.info("ETL SUMMARY REPORT")
    logger.info("="*80)

    total_rows_landing = sum(r['rows_loaded'] for r in landing_results if r['status'] == 'SUCCESS')
    success_count = len([r for r in landing_results if r['status'] == 'SUCCESS'])
    failed_count = len([r for r in landing_results if r['status'] == 'FAILED'])

    logger.info(f"\nLanding Layer:")
    logger.info(f"  [OK] Successful loads: {success_count}/{len(landing_results)}")
    logger.info(f"  [ERROR] Failed loads:     {failed_count}/{len(landing_results)}")
    logger.info(f"  Total rows loaded:  {total_rows_landing:,}")

    # Detailed summary
    logger.info(f"\nDetailed Summary:")
    logger.info(f"{'Table':<30} {'Rows':>15} {'Status':>10} {'Duration':>10}")
    logger.info("-" * 65)

    for result in landing_results:
        if result['status'] == 'SUCCESS':
            duration = (result['end_time'] - result['start_time']).total_seconds()
            logger.info(
                f"{result['table']:<30} {result['rows_loaded']:>15,} {result['status']:>10} {duration:>9.2f}s"
            )
        else:
            logger.info(
                f"{result['table']:<30} {'N/A':>15} {result['status']:>10} {'N/A':>10}"
            )

    logger.info("\n" + "="*80)
    logger.info("ETL PROCESS COMPLETED")
    logger.info("="*80)

    # Save detailed log to file
    save_execution_log(landing_results, staging_results)


# ============================================================================
# LOGGING UTILITIES - LƯU LOG CHI TIẾT
# ============================================================================

def save_execution_log(landing_results: List[Dict], staging_results: List[Dict]) -> None:
    """Lưu chi tiết execution log vào CSV file"""

    try:
        log_data = []

        for result in landing_results:
            log_data.append({
                'LayerType': 'Landing',
                'TableName': result['table'],
                'RowsProcessed': result['rows_loaded'],
                'Status': result['status'],
                'Duration_Seconds': (result['end_time'] - result['start_time']).total_seconds()
                    if result['end_time'] else None,
                'ErrorMessage': result['error'],
                'ExecutedAt': result['start_time'].isoformat()
            })

        # Create DataFrame and save
        log_df = pd.DataFrame(log_data)
        log_file = Config.LOG_PATH / f"etl_execution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        log_df.to_csv(log_file, index=False)
        logger.info(f"\n[OK] Execution log saved to: {log_file}")

    except Exception as e:
        logger.error(f"Error saving execution log: {str(e)}")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        # Kiểm tra folder CSV tồn tại
        if not Config.CSV_DATA_PATH.exists():
            logger.error(f"CSV data path does not exist: {Config.CSV_DATA_PATH}")
            sys.exit(1)

        # Kiểm tra có file CSV nào không
        csv_files_found = list(Config.CSV_DATA_PATH.glob("*.csv"))
        if not csv_files_found:
            logger.error(f"No CSV files found in: {Config.CSV_DATA_PATH}")
            sys.exit(1)

        logger.info(f"Found {len(csv_files_found)} CSV files")

        # Chạy ETL pipeline
        run_etl_pipeline()

        logger.info("\n[OK] ETL Pipeline completed successfully!")
        sys.exit(0)

    except Exception as e:
        logger.error(f"\n[ERROR] ETL Pipeline failed: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)
