"""
================================================================================
SYNTHEA TRANSFORMATION - Landing to Staging
================================================================================
Mục đích: Transform dữ liệu từ Landing (VARCHAR raw) sang Staging (typed data)

Yêu cầu:
  - Đã chạy 01_Schema_Landing.sql và 03_Schema_Staging.sql
  - Đã chạy 02_ETL_Synthea_Extract.py thành công
  
Chạy: python 05_Transform_Landing_to_Staging.py
================================================================================
"""

import os
import sys
import csv
import uuid
import pandas as pd
import numpy as np
import pyodbc
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict
import traceback
from tqdm import tqdm

import warnings
warnings.filterwarnings('ignore')

# Load environment variables from .env (optional)
from dotenv import load_dotenv
load_dotenv()


# ============================================================================
# CONFIG
# ============================================================================

class Config:
    """Cấu hình Transform"""
    
    PROJECT_ROOT = Path(__file__).parent
    LOG_PATH = PROJECT_ROOT / "logs"
    
    # SQL Server - Windows Authentication ONLY
    SQL_SERVER = os.environ.get('SYNTHEA_SQL_SERVER', 'LAPTOP-GE6ISH50')
    LANDING_DB = "DW_Synthea_Landing"
    STAGING_DB = "DW_Synthea_Staging"
    
    # Chunk / Insert processing
    CHUNK_SIZE = int(os.environ.get('SYNTHEA_CHUNK_SIZE', '50000'))
    BATCH_SIZE = int(os.environ.get('SYNTHEA_BATCH_SIZE', '10000'))

    # Insert method: executemany | bulk_insert
    INSERT_METHOD = os.environ.get('SYNTHEA_INSERT_METHOD', 'executemany').strip().lower()

    # Bulk insert temp files (SQL Server service must be able to read this path)
    BULK_DIR = PROJECT_ROOT / "data" / "bulk"
    BULK_FIELD_TERMINATOR = "\x1f"  # Unit Separator (rare in text)
    BULK_ROW_TERMINATOR = "\n"


# ============================================================================
# LOGGING
# ============================================================================

def setup_logging():
    """Thiết lập logging"""
    Config.LOG_PATH.mkdir(parents=True, exist_ok=True)
    
    log_format = '%(asctime)s | %(levelname)-8s | %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    log_file = Config.LOG_PATH / f"transform_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
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
    logger.info("SYNTHEA ETL - TRANSFORM PROCESS")
    logger.info(f"Source: {Config.LANDING_DB}")
    logger.info(f"Target: {Config.STAGING_DB}")
    logger.info("="*80)
    
    return logger


logger = setup_logging()


# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_odbc_driver() -> str:
    """Tự động chọn ODBC Driver tốt nhất"""
    installed = pyodbc.drivers()
    
    for preferred in ['ODBC Driver 18 for SQL Server', 'ODBC Driver 17 for SQL Server']:
        if preferred in installed:
            return preferred
    
    for driver in reversed(installed):
        if 'SQL Server' in driver:
            return driver
    
    raise Exception(f"Không tìm thấy ODBC Driver. Installed: {installed}")


def create_connection(database: str):
    """Tạo connection với Windows Authentication"""
    try:
        driver = get_odbc_driver()
        
        conn_str = f"DRIVER={{{driver}}};SERVER={Config.SQL_SERVER};"
        conn_str += f"DATABASE={database};"
        conn_str += "Trusted_Connection=yes;"
        
        if 'Driver 18' in driver:
            conn_str += "Encrypt=no;TrustServerCertificate=yes;"
        
        connection = pyodbc.connect(conn_str, timeout=30)
        connection.autocommit = False
        
        return connection
        
    except Exception as e:
        logger.error(f"Lỗi kết nối {database}: {str(e)}")
        raise


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def safe_to_date(value):
    """Convert string to date, return None if invalid"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return pd.to_datetime(value).date()
    except:
        return None


def safe_to_datetime(value):
    """Convert string to datetime, return None if invalid"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return pd.to_datetime(value)
    except:
        return None


def safe_to_decimal(value, decimal_places=2):
    """Convert string to decimal, return None if invalid"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return round(float(value), decimal_places)
    except:
        return None


def safe_to_int(value):
    """Convert string to int, return None if invalid"""
    if pd.isna(value) or value == '' or value is None:
        return None
    try:
        return int(float(value))
    except:
        return None


def trim_string(value, max_length):
    """Trim string to max length"""
    if pd.isna(value) or value is None or value == '':
        return None
    s = str(value).strip()
    return s[:max_length] if s else None


# ============================================================================
# TRANSFORM FUNCTIONS
# ============================================================================

def transform_patients(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Patients data"""
    logger.info("  Transforming Patients...")
    
    result = pd.DataFrame()
    result['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    result['BIRTHDATE'] = df['BIRTHDATE'].apply(safe_to_date)
    result['DEATHDATE'] = df['DEATHDATE'].apply(safe_to_date)
    result['SSN'] = df['SSN'].apply(lambda x: trim_string(x, 11))
    result['DRIVERS'] = df['DRIVERS'].apply(lambda x: trim_string(x, 50))
    result['PASSPORT'] = df['PASSPORT'].apply(lambda x: trim_string(x, 50))
    result['PREFIX'] = df['PREFIX'].apply(lambda x: trim_string(x, 10))
    result['FIRST'] = df['FIRST'].apply(lambda x: trim_string(x, 100))
    result['LAST'] = df['LAST'].apply(lambda x: trim_string(x, 100))
    result['SUFFIX'] = df['SUFFIX'].apply(lambda x: trim_string(x, 10))
    result['MAIDEN'] = df['MAIDEN'].apply(lambda x: trim_string(x, 100))
    result['MARITAL'] = df['MARITAL'].apply(lambda x: trim_string(x, 20))
    result['RACE'] = df['RACE'].apply(lambda x: trim_string(x, 50))
    result['ETHNICITY'] = df['ETHNICITY'].apply(lambda x: trim_string(x, 50))
    result['GENDER'] = df['GENDER'].apply(lambda x: trim_string(x, 1))
    result['BIRTHPLACE'] = df['BIRTHPLACE'].apply(lambda x: trim_string(x, 255))
    result['ADDRESS'] = df['ADDRESS'].apply(lambda x: trim_string(x, 255))
    result['CITY'] = df['CITY'].apply(lambda x: trim_string(x, 100))
    result['STATE'] = df['STATE'].apply(lambda x: trim_string(x, 50))
    result['COUNTY'] = df['COUNTY'].apply(lambda x: trim_string(x, 100))
    result['ZIP'] = df['ZIP'].apply(lambda x: trim_string(x, 10))
    result['LAT'] = pd.to_numeric(df['LAT'], errors='coerce')
    result['LON'] = pd.to_numeric(df['LON'], errors='coerce')
    result['HEALTHCARE_EXPENSES'] = df['HEALTHCARE_EXPENSES'].apply(lambda x: safe_to_decimal(x, 2))
    result['HEALTHCARE_COVERAGE'] = df['HEALTHCARE_COVERAGE'].apply(lambda x: safe_to_decimal(x, 2))
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_encounters(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Encounters data"""
    logger.info("  Transforming Encounters...")
    
    result = pd.DataFrame()
    result['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    result['START'] = pd.to_datetime(df['START'], errors='coerce')
    result['STOP'] = pd.to_datetime(df['STOP'], errors='coerce')
    result['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    result['ORGANIZATION'] = df['ORGANIZATION'].apply(lambda x: trim_string(x, 36))
    result['PROVIDER'] = df['PROVIDER'].apply(lambda x: trim_string(x, 36))
    result['PAYER'] = df['PAYER'].apply(lambda x: trim_string(x, 36))
    result['ENCOUNTERCLASS'] = df['ENCOUNTERCLASS'].apply(lambda x: trim_string(x, 50))
    result['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    result['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['BASE_ENCOUNTER_COST'] = df['BASE_ENCOUNTER_COST'].apply(lambda x: safe_to_decimal(x, 2))
    result['TOTAL_CLAIM_COST'] = df['TOTAL_CLAIM_COST'].apply(lambda x: safe_to_decimal(x, 2))
    result['PAYER_COVERAGE'] = df['PAYER_COVERAGE'].apply(lambda x: safe_to_decimal(x, 2))
    result['REASONCODE'] = df['REASONCODE'].apply(lambda x: trim_string(x, 20))
    result['REASONDESCRIPTION'] = df['REASONDESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_conditions(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Conditions data"""
    logger.info("  Transforming Conditions...")
    
    result = pd.DataFrame()
    result['START'] = pd.to_datetime(df['START'], errors='coerce')
    result['STOP'] = pd.to_datetime(df['STOP'], errors='coerce')
    result['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    result['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    result['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    result['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_medications(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Medications data"""
    logger.info("  Transforming Medications...")
    
    result = pd.DataFrame()
    result['START'] = pd.to_datetime(df['START'], errors='coerce')
    result['STOP'] = pd.to_datetime(df['STOP'], errors='coerce')
    result['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    result['PAYER'] = df['PAYER'].apply(lambda x: trim_string(x, 36))
    result['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    result['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    result['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['BASE_COST'] = df['BASE_COST'].apply(lambda x: safe_to_decimal(x, 2))
    result['PAYER_COVERAGE'] = df['PAYER_COVERAGE'].apply(lambda x: safe_to_decimal(x, 2))
    result['DISPENSES'] = df['DISPENSES'].apply(safe_to_int)
    result['TOTALCOST'] = df['TOTALCOST'].apply(lambda x: safe_to_decimal(x, 2))
    result['REASONCODE'] = df['REASONCODE'].apply(lambda x: trim_string(x, 20))
    result['REASONDESCRIPTION'] = df['REASONDESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_observations(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Observations data"""
    logger.info("  Transforming Observations...")
    
    result = pd.DataFrame()
    result['DATE'] = df['DATE'].apply(safe_to_date)
    result['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    result['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    result['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    result['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['VALUE'] = df['VALUE'].apply(lambda x: trim_string(x, 255))
    result['UNITS'] = df['UNITS'].apply(lambda x: trim_string(x, 20))
    result['TYPE'] = df['TYPE'].apply(lambda x: trim_string(x, 50))
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_procedures(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Procedures data"""
    logger.info("  Transforming Procedures...")
    
    result = pd.DataFrame()
    result['DATE'] = df['DATE'].apply(safe_to_date)
    result['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    result['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    result['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    result['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['BASE_COST'] = df['BASE_COST'].apply(lambda x: safe_to_decimal(x, 2))
    result['REASONCODE'] = df['REASONCODE'].apply(lambda x: trim_string(x, 20))
    result['REASONDESCRIPTION'] = df['REASONDESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_immunizations(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Immunizations data"""
    logger.info("  Transforming Immunizations...")
    
    result = pd.DataFrame()
    result['DATE'] = df['DATE'].apply(safe_to_date)
    result['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    result['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    result['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    result['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['BASE_COST'] = df['BASE_COST'].apply(lambda x: safe_to_decimal(x, 2))
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_allergies(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Allergies data"""
    logger.info("  Transforming Allergies...")
    
    result = pd.DataFrame()
    result['START'] = pd.to_datetime(df['START'], errors='coerce')
    result['STOP'] = pd.to_datetime(df['STOP'], errors='coerce')
    result['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    result['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    result['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    result['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_careplans(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Careplans data"""
    logger.info("  Transforming Careplans...")
    
    result = pd.DataFrame()
    result['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    result['START'] = pd.to_datetime(df['START'], errors='coerce')
    result['STOP'] = pd.to_datetime(df['STOP'], errors='coerce')
    result['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    result['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    result['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    result['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['REASONCODE'] = df['REASONCODE'].apply(lambda x: trim_string(x, 20))
    result['REASONDESCRIPTION'] = df['REASONDESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_devices(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Devices data"""
    logger.info("  Transforming Devices...")
    
    result = pd.DataFrame()
    result['START'] = pd.to_datetime(df['START'], errors='coerce')
    result['STOP'] = pd.to_datetime(df['STOP'], errors='coerce')
    result['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    result['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    result['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    result['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['UDI'] = df['UDI'].apply(lambda x: trim_string(x, 500))
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_imaging_studies(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Imaging Studies data"""
    logger.info("  Transforming Imaging Studies...")
    
    result = pd.DataFrame()
    result['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    result['DATE'] = df['DATE'].apply(safe_to_date)
    result['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    result['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    result['BODYSITE_CODE'] = df['BODYSITE_CODE'].apply(lambda x: trim_string(x, 20))
    result['BODYSITE_DESCRIPTION'] = df['BODYSITE_DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['MODALITY_CODE'] = df['MODALITY_CODE'].apply(lambda x: trim_string(x, 5))
    result['MODALITY_DESCRIPTION'] = df['MODALITY_DESCRIPTION'].apply(lambda x: trim_string(x, 50))
    result['SOP_CODE'] = df['SOP_CODE'].apply(lambda x: trim_string(x, 64))
    result['SOP_DESCRIPTION'] = df['SOP_DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_supplies(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Supplies data"""
    logger.info("  Transforming Supplies...")
    
    result = pd.DataFrame()
    result['DATE'] = df['DATE'].apply(safe_to_date)
    result['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    result['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    result['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    result['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    result['QUANTITY'] = df['QUANTITY'].apply(safe_to_int)
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_organizations(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Organizations data"""
    logger.info("  Transforming Organizations...")
    
    result = pd.DataFrame()
    result['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    result['NAME'] = df['NAME'].apply(lambda x: trim_string(x, 255))
    result['ADDRESS'] = df['ADDRESS'].apply(lambda x: trim_string(x, 255))
    result['CITY'] = df['CITY'].apply(lambda x: trim_string(x, 100))
    result['STATE'] = df['STATE'].apply(lambda x: trim_string(x, 50))
    result['ZIP'] = df['ZIP'].apply(lambda x: trim_string(x, 10))
    result['LAT'] = pd.to_numeric(df['LAT'], errors='coerce')
    result['LON'] = pd.to_numeric(df['LON'], errors='coerce')
    result['PHONE'] = df['PHONE'].apply(lambda x: trim_string(x, 20))
    result['REVENUE'] = df['REVENUE'].apply(lambda x: safe_to_decimal(x, 2))
    result['UTILIZATION'] = df['UTILIZATION'].apply(safe_to_int)
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_providers(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Providers data"""
    logger.info("  Transforming Providers...")
    
    result = pd.DataFrame()
    result['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    result['ORGANIZATION'] = df['ORGANIZATION'].apply(lambda x: trim_string(x, 36))
    result['NAME'] = df['NAME'].apply(lambda x: trim_string(x, 255))
    result['GENDER'] = df['GENDER'].apply(lambda x: trim_string(x, 1))
    result['SPECIALITY'] = df['SPECIALITY'].apply(lambda x: trim_string(x, 100))
    result['ADDRESS'] = df['ADDRESS'].apply(lambda x: trim_string(x, 255))
    result['CITY'] = df['CITY'].apply(lambda x: trim_string(x, 100))
    result['STATE'] = df['STATE'].apply(lambda x: trim_string(x, 50))
    result['ZIP'] = df['ZIP'].apply(lambda x: trim_string(x, 10))
    result['LAT'] = pd.to_numeric(df['LAT'], errors='coerce')
    result['LON'] = pd.to_numeric(df['LON'], errors='coerce')
    result['UTILIZATION'] = df['UTILIZATION'].apply(safe_to_int)
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_payers(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Payers data"""
    logger.info("  Transforming Payers...")
    
    result = pd.DataFrame()
    result['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    result['NAME'] = df['NAME'].apply(lambda x: trim_string(x, 255))
    result['ADDRESS'] = df['ADDRESS'].apply(lambda x: trim_string(x, 255))
    result['CITY'] = df['CITY'].apply(lambda x: trim_string(x, 100))
    result['STATE_HEADQUARTERED'] = df['STATE_HEADQUARTERED'].apply(lambda x: trim_string(x, 50))
    result['ZIP'] = df['ZIP'].apply(lambda x: trim_string(x, 10))
    result['PHONE'] = df['PHONE'].apply(lambda x: trim_string(x, 20))
    result['AMOUNT_COVERED'] = df['AMOUNT_COVERED'].apply(lambda x: safe_to_decimal(x, 2))
    result['AMOUNT_UNCOVERED'] = df['AMOUNT_UNCOVERED'].apply(lambda x: safe_to_decimal(x, 2))
    result['REVENUE'] = df['REVENUE'].apply(lambda x: safe_to_decimal(x, 2))
    result['COVERED_ENCOUNTERS'] = df['COVERED_ENCOUNTERS'].apply(safe_to_int)
    result['UNCOVERED_ENCOUNTERS'] = df['UNCOVERED_ENCOUNTERS'].apply(safe_to_int)
    result['COVERED_MEDICATIONS'] = df['COVERED_MEDICATIONS'].apply(safe_to_int)
    result['UNCOVERED_MEDICATIONS'] = df['UNCOVERED_MEDICATIONS'].apply(safe_to_int)
    result['COVERED_PROCEDURES'] = df['COVERED_PROCEDURES'].apply(safe_to_int)
    result['UNCOVERED_PROCEDURES'] = df['UNCOVERED_PROCEDURES'].apply(safe_to_int)
    result['COVERED_IMMUNIZATIONS'] = df['COVERED_IMMUNIZATIONS'].apply(safe_to_int)
    result['UNCOVERED_IMMUNIZATIONS'] = df['UNCOVERED_IMMUNIZATIONS'].apply(safe_to_int)
    result['UNIQUE_CUSTOMERS'] = df['UNIQUE_CUSTOMERS'].apply(safe_to_int)
    result['QOLS_AVG'] = df['QOLS_AVG'].apply(lambda x: safe_to_decimal(x, 4))
    result['MEMBER_MONTHS'] = df['MEMBER_MONTHS'].apply(safe_to_int)
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


def transform_payer_transitions(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Payer Transitions data"""
    logger.info("  Transforming Payer Transitions...")
    
    result = pd.DataFrame()
    result['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    result['START_YEAR'] = df['START_YEAR'].apply(safe_to_int)
    result['END_YEAR'] = df['END_YEAR'].apply(safe_to_int)
    result['PAYER'] = df['PAYER'].apply(lambda x: trim_string(x, 36))
    result['OWNERSHIP'] = df['OWNERSHIP'].apply(lambda x: trim_string(x, 50))
    result['create_at'] = datetime.now()
    result['update_at'] = datetime.now()
    
    return result


# ============================================================================
# MAIN TRANSFORM PIPELINE
# ============================================================================

def _build_insert_sql(table: str, columns):
    col_str = ", ".join(f"[{col}]" for col in columns)
    placeholders = ", ".join("?" * len(columns))
    return f"INSERT INTO [{table}] ({col_str}) VALUES ({placeholders})"


def _bulk_insert_dataframe(staging_conn, staging_table: str, df_staging: pd.DataFrame) -> int:
    """Bulk insert bằng BULK INSERT (nhanh, nhưng SQL Server service phải đọc được file path)."""
    Config.BULK_DIR.mkdir(parents=True, exist_ok=True)

    df_dump = df_staging.copy()
    obj_cols = df_dump.select_dtypes(include=["object", "string"]).columns
    if len(obj_cols) > 0:
        df_dump[obj_cols] = df_dump[obj_cols].astype("string")
        for col in obj_cols:
            df_dump[col] = (
                df_dump[col]
                .str.replace(Config.BULK_FIELD_TERMINATOR, " ", regex=False)
                .str.replace("\r", " ", regex=False)
                .str.replace("\n", " ", regex=False)
                .str.replace("\t", " ", regex=False)
            )

    bulk_file = Config.BULK_DIR / f"{staging_table}_{uuid.uuid4().hex}.dat"

    try:
        df_dump.to_csv(
            bulk_file,
            sep=Config.BULK_FIELD_TERMINATOR,
            index=False,
            header=False,
            encoding="utf-8",
            lineterminator=Config.BULK_ROW_TERMINATOR,
            na_rep="",
            quoting=csv.QUOTE_NONE,
        )

        sql_path = str(bulk_file).replace("'", "''")

        cursor = staging_conn.cursor()
        cursor.execute(
            f"""
BULK INSERT [{staging_table}]
FROM '{sql_path}'
WITH (
    DATAFILETYPE = 'char',
    CODEPAGE = '65001',
    FIELDTERMINATOR = '0x1f',
    ROWTERMINATOR = '0x0a',
    TABLOCK
)
"""
        )
        staging_conn.commit()
        cursor.close()

        return len(df_dump)

    finally:
        if bulk_file.exists():
            bulk_file.unlink()


def transform_table(landing_table: str, staging_table: str, transform_func):
    """Transform single table từ Landing sang Staging (chunk + batch insert)"""

    start_time = datetime.now()

    try:
        logger.info(f"\n[Transform] {landing_table} -> {staging_table}")

        landing_conn = None
        staging_conn = None
        cursor = None

        try:
            # Open connections
            landing_conn = create_connection(Config.LANDING_DB)
            staging_conn = create_connection(Config.STAGING_DB)
            cursor = staging_conn.cursor()
            cursor.fast_executemany = True

            # Truncate target once
            cursor.execute(f"TRUNCATE TABLE [{staging_table}]")
            staging_conn.commit()

            query = f"SELECT * FROM [{landing_table}]"
            chunk_iter = pd.read_sql(query, landing_conn, chunksize=Config.CHUNK_SIZE)

            insert_sql = None
            columns = None
            rows_inserted = 0

            logger.info(
                f"  Chunk processing: chunksize={Config.CHUNK_SIZE:,} | insert_method={Config.INSERT_METHOD}"
            )

            with tqdm(desc="  Processing", unit="rows") as pbar:
                for df_chunk in chunk_iter:
                    if df_chunk is None or len(df_chunk) == 0:
                        continue

                    df_staging = transform_func(df_chunk)

                    if columns is None:
                        columns = list(df_staging.columns)

                    df_staging = df_staging[columns]

                    if Config.INSERT_METHOD == 'bulk_insert':
                        rows_inserted += _bulk_insert_dataframe(staging_conn, staging_table, df_staging)
                        pbar.update(len(df_staging))
                        continue

                    # Default: executemany (fast_executemany)
                    if insert_sql is None:
                        insert_sql = _build_insert_sql(staging_table, columns)

                    total_rows = len(df_staging)
                    for batch_start in range(0, total_rows, Config.BATCH_SIZE):
                        batch_end = min(batch_start + Config.BATCH_SIZE, total_rows)
                        batch = df_staging.iloc[batch_start:batch_end]

                        rows = list(batch.itertuples(index=False, name=None))
                        cursor.executemany(insert_sql, rows)
                        staging_conn.commit()

                        rows_inserted += len(rows)
                        pbar.update(len(rows))

        finally:
            if cursor is not None:
                cursor.close()
            if staging_conn is not None:
                staging_conn.close()
            if landing_conn is not None:
                landing_conn.close()

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"  [OK] Inserted {rows_inserted:,} rows ({duration:.2f}s)")

        return {
            'table': staging_table,
            'status': 'SUCCESS',
            'rows': rows_inserted,
            'duration': duration
        }

    except Exception as e:
        logger.error(f"  [ERROR] {str(e)}")
        logger.error(traceback.format_exc())

        return {
            'table': staging_table,
            'status': 'FAILED',
            'rows': 0,
            'error': str(e)
        }


def run_transform_pipeline():
    """Chạy toàn bộ Transform pipeline"""
    
    logger.info("\n" + "="*80)
    logger.info("STARTING TRANSFORM PIPELINE")
    logger.info("="*80)
    
    # Check connection
    logger.info("\n[STEP 0] Checking database connections...")
    try:
        # Check Landing
        conn_landing = create_connection(Config.LANDING_DB)
        cursor = conn_landing.cursor()
        cursor.execute("SELECT COUNT(*) FROM Landing_Patients")
        count = cursor.fetchone()[0]
        cursor.close()
        conn_landing.close()
        logger.info(f"[OK] Landing DB connected ({count:,} patients)")
        
        # Check Staging
        conn_staging = create_connection(Config.STAGING_DB)
        cursor = conn_staging.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn_staging.close()
        logger.info(f"[OK] Staging DB connected")
        
    except Exception as e:
        logger.error(f"[ERROR] Cannot connect: {str(e)}")
        sys.exit(1)
    
    # Transform all tables
    logger.info("\n[STEP 1] Transforming tables...")
    
    tables_to_transform = [
        ('Landing_Patients', 'Staging_Patients', transform_patients),
        ('Landing_Encounters', 'Staging_Encounters', transform_encounters),
        ('Landing_Conditions', 'Staging_Conditions', transform_conditions),
        ('Landing_Medications', 'Staging_Medications', transform_medications),
        ('Landing_Observations', 'Staging_Observations', transform_observations),
        ('Landing_Procedures', 'Staging_Procedures', transform_procedures),
        ('Landing_Immunizations', 'Staging_Immunizations', transform_immunizations),
        ('Landing_Allergies', 'Staging_Allergies', transform_allergies),
        ('Landing_Careplans', 'Staging_Careplans', transform_careplans),
        ('Landing_Devices', 'Staging_Devices', transform_devices),
        ('Landing_Imaging_Studies', 'Staging_Imaging_Studies', transform_imaging_studies),
        ('Landing_Supplies', 'Staging_Supplies', transform_supplies),
        ('Landing_Organizations', 'Staging_Organizations', transform_organizations),
        ('Landing_Providers', 'Staging_Providers', transform_providers),
        ('Landing_Payers', 'Staging_Payers', transform_payers),
        ('Landing_Payer_Transitions', 'Staging_Payer_Transitions', transform_payer_transitions),
    ]
    
    results = []
    
    for landing_table, staging_table, transform_func in tables_to_transform:
        result = transform_table(landing_table, staging_table, transform_func)
        results.append(result)
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("TRANSFORM SUMMARY")
    logger.info("="*80)
    
    success_count = len([r for r in results if r['status'] == 'SUCCESS'])
    failed_count = len([r for r in results if r['status'] == 'FAILED'])
    total_rows = sum(r['rows'] for r in results if r['status'] == 'SUCCESS')
    
    logger.info(f"\nSuccessful: {success_count}/{len(results)}")
    logger.info(f"Failed:     {failed_count}/{len(results)}")
    logger.info(f"Total rows: {total_rows:,}\n")
    
    logger.info(f"{'Table':<35} {'Rows':>15} {'Duration':>10} {'Status':>10}")
    logger.info("-" * 75)
    
    for result in results:
        if result['status'] == 'SUCCESS':
            logger.info(
                f"{result['table']:<35} {result['rows']:>15,} "
                f"{result['duration']:>9.2f}s {result['status']:>10}"
            )
        else:
            logger.info(
                f"{result['table']:<35} {'ERROR':>15} "
                f"{'N/A':>10} {result['status']:>10}"
            )
    
    logger.info("\n" + "="*80)
    
    if failed_count > 0:
        logger.warning("MỘT SỐ TABLE TRANSFORM LỖI!")
        logger.info("Kiểm tra log để xem chi tiết lỗi")
    else:
        logger.info("TRANSFORM COMPLETED SUCCESSFULLY!")
        logger.info("\nDữ liệu đã sẵn sàng trong Staging database")
        logger.info("Có thể tiếp tục build Data Warehouse/Data Mart")
    
    logger.info("="*80)


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        run_transform_pipeline()
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"\n[FATAL ERROR] {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)