"""
================================================================================
SYNTHEA TRANSFORMATION SCRIPT - Landing to Staging
================================================================================
Mục đích: Transform dữ liệu từ Landing (raw) thành Staging (cleaned + typed)

Flow:
  Landing (raw VARCHAR) -> Staging (cleaned + proper types)

Data Quality:
  - NULL handling
  - Type conversion (DATE, DECIMAL, INT)
  - Trim whitespace
  - Handle empty/invalid values

Chạy: python 05_Transform_Landing_to_Staging.py
================================================================================
"""

import os
import sys
import pandas as pd
import numpy as np
import pyodbc
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import traceback
from tqdm import tqdm

import warnings
warnings.filterwarnings('ignore')


# ============================================================================
# CONFIG
# ============================================================================

class Config:
    """Cấu hình Transform"""

    PROJECT_ROOT = Path(__file__).parent
    LOG_PATH = PROJECT_ROOT / "logs"
    ERROR_LOG_PATH = PROJECT_ROOT / "logs" / "errors"

    SQL_SERVER = "Akuma"       # Your local SQL Server
    SQL_USER = ""
    SQL_PASSWORD = ""
    USE_WINDOWS_AUTH = True

    LANDING_DB = "DW_Synthea_Landing"
    STAGING_DB = "DW_Synthea_Staging"

    CHUNK_SIZE = 200000        # Optimized for 23GB RAM
    BATCH_SIZE = 10000         # Batch size for pyodbc inserts


# ============================================================================
# LOGGING
# ============================================================================

def setup_logging() -> None:
    """Thiết lập logging"""
    Config.LOG_PATH.mkdir(parents=True, exist_ok=True)
    Config.ERROR_LOG_PATH.mkdir(parents=True, exist_ok=True)

    log_format = '%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    log_file = Config.LOG_PATH / f"transform_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
    logger.info("SYNTHEA TRANSFORMATION PROCESS STARTED")
    logger.info(f"Landing DB: {Config.LANDING_DB}")
    logger.info(f"Staging DB: {Config.STAGING_DB}")
    logger.info("="*80)

    return logger


logger = setup_logging()


# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_connection_string(database_name: str = None) -> str:
    """Tạo connection string"""
    if Config.USE_WINDOWS_AUTH:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={Config.SQL_SERVER};"
        if database_name:
            conn_str += f"DATABASE={database_name};"
        conn_str += "Trusted_Connection=yes;"
    else:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={Config.SQL_SERVER};"
        conn_str += f"UID={Config.SQL_USER};PWD={Config.SQL_PASSWORD};"
        if database_name:
            conn_str += f"DATABASE={database_name};"

    return conn_str


def create_connection(database_name: str = None):
    """Tạo connection"""
    try:
        conn_str = get_connection_string(database_name)
        connection = pyodbc.connect(conn_str, timeout=30, autocommit=True)
        return connection
    except Exception as e:
        logger.error(f"Connection failed to {database_name}: {str(e)}")
        raise


def ensure_min_staging_column_length(
    table_name: str,
    column_name: str,
    min_length: int,
    data_type: str = 'VARCHAR'
) -> bool:
    """Đảm bảo độ dài tối thiểu cho cột text trong Staging để tránh truncate."""

    conn = None
    try:
        conn = create_connection(Config.STAGING_DB)
        cursor = conn.cursor()

        check_sql = """
        SELECT CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dbo'
          AND TABLE_NAME = ?
          AND COLUMN_NAME = ?
        """
        row = cursor.execute(check_sql, (table_name, column_name)).fetchone()

        if not row:
            logger.warning(f"[WARN] Column not found: {table_name}.{column_name}")
            return False

        current_length = row[0]
        is_nullable = row[1]

        if current_length is None or current_length >= min_length:
            logger.info(f"[OK] {table_name}.{column_name} length = {current_length}, no change needed")
            return True

        nullable_sql = "NULL" if str(is_nullable).upper() == "YES" else "NOT NULL"
        alter_sql = (
            f"ALTER TABLE [dbo].[{table_name}] "
            f"ALTER COLUMN [{column_name}] {data_type}({min_length}) {nullable_sql};"
        )
        cursor.execute(alter_sql)
        conn.commit()

        logger.info(
            f"[OK] Altered {table_name}.{column_name} from {current_length} to {min_length}"
        )
        return True

    except Exception as e:
        logger.error(
            f"[ERROR] Failed to ensure length for {table_name}.{column_name}: {str(e)}"
        )
        return False
    finally:
        if conn:
            conn.close()


def ensure_staging_schema_compatibility() -> bool:
    """Đồng bộ nhanh các cột Staging dễ bị truncate trước khi transform/load."""

    checks = [
        ("Staging_Observations", "VALUE", 255),
        ("Staging_Imaging_Studies", "SOP_CODE", 64),
    ]

    all_ok = True
    for table_name, column_name, min_length in checks:
        ok = ensure_min_staging_column_length(
            table_name=table_name,
            column_name=column_name,
            min_length=min_length,
            data_type='VARCHAR'
        )
        all_ok = all_ok and ok

    return all_ok


# ============================================================================
# DATA UTILITIES
# ============================================================================

def safe_to_date(value):
    """Convert to DATE safely"""
    if pd.isna(value) or value == '' or value == 'NULL':
        return None
    try:
        return pd.to_datetime(str(value).strip()).date()
    except:
        return None


def safe_to_datetime(value):
    """Convert to DATETIME safely"""
    if pd.isna(value) or value == '' or value == 'NULL':
        return None
    try:
        return pd.to_datetime(str(value).strip())
    except:
        return None


def safe_to_decimal(value, places=2):
    """Convert to DECIMAL safely"""
    if pd.isna(value) or value == '' or value == 'NULL':
        return None
    try:
        return round(float(str(value).strip()), places)
    except:
        return None


def safe_to_int(value):
    """Convert to INT safely"""
    if pd.isna(value) or value == '' or value == 'NULL':
        return None
    try:
        return int(float(str(value).strip()))
    except:
        return None


def trim_string(value, max_len=None):
    """Trim string safely"""
    if pd.isna(value):
        return None
    s = str(value).strip()
    if s == '' or s == 'NULL':
        return None
    if max_len and len(s) > max_len:
        return s[:max_len]
    return s


# ============================================================================
# TRANSFORMATION LOGIC
# ============================================================================

def transform_patients(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Patients data"""

    logger.info("Transforming Patients...")

    df_transform = pd.DataFrame()
    df_transform['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    df_transform['BIRTHDATE'] = df['BIRTHDATE'].apply(safe_to_date)
    df_transform['DEATHDATE'] = df['DEATHDATE'].apply(safe_to_date)
    df_transform['SSN'] = df['SSN'].apply(lambda x: trim_string(x, 11))
    df_transform['DRIVERS'] = df['DRIVERS'].apply(lambda x: trim_string(x, 50))
    df_transform['PASSPORT'] = df['PASSPORT'].apply(lambda x: trim_string(x, 50))
    df_transform['PREFIX'] = df['PREFIX'].apply(lambda x: trim_string(x, 10))
    df_transform['FIRST'] = df['FIRST'].apply(lambda x: trim_string(x, 100))
    df_transform['LAST'] = df['LAST'].apply(lambda x: trim_string(x, 100))
    df_transform['SUFFIX'] = df['SUFFIX'].apply(lambda x: trim_string(x, 10))
    df_transform['MAIDEN'] = df['MAIDEN'].apply(lambda x: trim_string(x, 100))
    df_transform['MARITAL'] = df['MARITAL'].apply(lambda x: trim_string(x, 20))
    df_transform['RACE'] = df['RACE'].apply(lambda x: trim_string(x, 50))
    df_transform['ETHNICITY'] = df['ETHNICITY'].apply(lambda x: trim_string(x, 50))
    df_transform['GENDER'] = df['GENDER'].apply(lambda x: trim_string(x, 1))
    df_transform['BIRTHPLACE'] = df['BIRTHPLACE'].apply(lambda x: trim_string(x, 255))
    df_transform['ADDRESS'] = df['ADDRESS'].apply(lambda x: trim_string(x, 255))
    df_transform['CITY'] = df['CITY'].apply(lambda x: trim_string(x, 100))
    df_transform['STATE'] = df['STATE'].apply(lambda x: trim_string(x, 50))
    df_transform['COUNTY'] = df['COUNTY'].apply(lambda x: trim_string(x, 100))
    df_transform['ZIP'] = df['ZIP'].apply(lambda x: trim_string(x, 10))
    df_transform['LAT'] = df['LAT'].apply(safe_to_decimal)
    df_transform['LON'] = df['LON'].apply(safe_to_decimal)
    df_transform['HEALTHCARE_EXPENSES'] = df['HEALTHCARE_EXPENSES'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['HEALTHCARE_COVERAGE'] = df['HEALTHCARE_COVERAGE'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()

    logger.info(f"[OK] Transformed {len(df_transform)} Patients rows")
    return df_transform


def transform_encounters(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Encounters data"""

    logger.info("Transforming Encounters...")

    df_transform = pd.DataFrame()
    df_transform['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    df_transform['START'] = df['START'].apply(safe_to_datetime)
    df_transform['STOP'] = df['STOP'].apply(safe_to_datetime)
    df_transform['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    df_transform['ORGANIZATION'] = df['ORGANIZATION'].apply(lambda x: trim_string(x, 36))
    df_transform['PROVIDER'] = df['PROVIDER'].apply(lambda x: trim_string(x, 36))
    df_transform['PAYER'] = df['PAYER'].apply(lambda x: trim_string(x, 36))
    df_transform['ENCOUNTERCLASS'] = df['ENCOUNTERCLASS'].apply(lambda x: trim_string(x, 50))
    df_transform['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    df_transform['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['BASE_ENCOUNTER_COST'] = df['BASE_ENCOUNTER_COST'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['TOTAL_CLAIM_COST'] = df['TOTAL_CLAIM_COST'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['PAYER_COVERAGE'] = df['PAYER_COVERAGE'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['REASONCODE'] = df['REASONCODE'].apply(lambda x: trim_string(x, 20))
    df_transform['REASONDESCRIPTION'] = df['REASONDESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()

    logger.info(f"[OK] Transformed {len(df_transform)} Encounters rows")
    return df_transform


def transform_observations(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Observations data (lớn nhất)"""

    logger.info("Transforming Observations...")

    df_transform = pd.DataFrame()
    df_transform['DATE'] = df['DATE'].apply(safe_to_date)
    df_transform['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    df_transform['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    df_transform['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    df_transform['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['VALUE'] = df['VALUE'].apply(lambda x: trim_string(x, 255))
    df_transform['UNITS'] = df['UNITS'].apply(lambda x: trim_string(x, 20))
    df_transform['TYPE'] = df['TYPE'].apply(lambda x: trim_string(x, 50))
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()

    logger.info(f"[OK] Transformed {len(df_transform)} Observations rows")
    return df_transform


def transform_medications(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Medications data"""

    logger.info("Transforming Medications...")

    df_transform = pd.DataFrame()
    df_transform['START'] = df['START'].apply(safe_to_date)
    df_transform['STOP'] = df['STOP'].apply(safe_to_date)
    df_transform['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    df_transform['PAYER'] = df['PAYER'].apply(lambda x: trim_string(x, 36))
    df_transform['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    df_transform['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    df_transform['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['BASE_COST'] = df['BASE_COST'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['PAYER_COVERAGE'] = df['PAYER_COVERAGE'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['DISPENSES'] = df['DISPENSES'].apply(safe_to_int)
    df_transform['TOTALCOST'] = df['TOTALCOST'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['REASONCODE'] = df['REASONCODE'].apply(lambda x: trim_string(x, 20))
    df_transform['REASONDESCRIPTION'] = df['REASONDESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()

    logger.info(f"[OK] Transformed {len(df_transform)} Medications rows")
    return df_transform


def transform_conditions(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Conditions data"""
    logger.info("Transforming Conditions...")
    df_transform = pd.DataFrame()
    df_transform['START'] = df['START'].apply(safe_to_date)
    df_transform['STOP'] = df['STOP'].apply(safe_to_date)
    df_transform['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    df_transform['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    df_transform['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    df_transform['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()
    logger.info(f"[OK] Transformed {len(df_transform)} Conditions rows")
    return df_transform


def transform_procedures(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Procedures data"""
    logger.info("Transforming Procedures...")
    df_transform = pd.DataFrame()
    df_transform['DATE'] = df['DATE'].apply(safe_to_date)
    df_transform['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    df_transform['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    df_transform['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    df_transform['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['BASE_COST'] = df['BASE_COST'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['REASONCODE'] = df['REASONCODE'].apply(lambda x: trim_string(x, 20))
    df_transform['REASONDESCRIPTION'] = df['REASONDESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()
    logger.info(f"[OK] Transformed {len(df_transform)} Procedures rows")
    return df_transform


def transform_immunizations(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Immunizations data"""
    logger.info("Transforming Immunizations...")
    df_transform = pd.DataFrame()
    df_transform['DATE'] = df['DATE'].apply(safe_to_date)
    df_transform['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    df_transform['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    df_transform['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    df_transform['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['BASE_COST'] = df['BASE_COST'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()
    logger.info(f"[OK] Transformed {len(df_transform)} Immunizations rows")
    return df_transform


def transform_allergies(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Allergies data"""
    logger.info("Transforming Allergies...")
    df_transform = pd.DataFrame()
    df_transform['START'] = df['START'].apply(safe_to_date)
    df_transform['STOP'] = df['STOP'].apply(safe_to_date)
    df_transform['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    df_transform['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    df_transform['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    df_transform['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()
    logger.info(f"[OK] Transformed {len(df_transform)} Allergies rows")
    return df_transform


def transform_careplans(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Careplans data"""
    logger.info("Transforming Careplans...")
    df_transform = pd.DataFrame()
    df_transform['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    df_transform['START'] = df['START'].apply(safe_to_date)
    df_transform['STOP'] = df['STOP'].apply(safe_to_date)
    df_transform['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    df_transform['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    df_transform['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    df_transform['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['REASONCODE'] = df['REASONCODE'].apply(lambda x: trim_string(x, 20))
    df_transform['REASONDESCRIPTION'] = df['REASONDESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()
    logger.info(f"[OK] Transformed {len(df_transform)} Careplans rows")
    return df_transform


def transform_devices(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Devices data"""
    logger.info("Transforming Devices...")
    df_transform = pd.DataFrame()
    df_transform['START'] = df['START'].apply(safe_to_date)
    df_transform['STOP'] = df['STOP'].apply(safe_to_date)
    df_transform['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    df_transform['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    df_transform['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    df_transform['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['UDI'] = df['UDI'].apply(lambda x: trim_string(x, 500))
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()
    logger.info(f"[OK] Transformed {len(df_transform)} Devices rows")
    return df_transform


def transform_imaging_studies(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Imaging Studies data"""
    logger.info("Transforming Imaging Studies...")
    df_transform = pd.DataFrame()
    df_transform['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    df_transform['DATE'] = df['DATE'].apply(safe_to_date)
    df_transform['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    df_transform['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    df_transform['BODYSITE_CODE'] = df['BODYSITE_CODE'].apply(lambda x: trim_string(x, 20))
    df_transform['BODYSITE_DESCRIPTION'] = df['BODYSITE_DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['MODALITY_CODE'] = df['MODALITY_CODE'].apply(lambda x: trim_string(x, 5))
    df_transform['MODALITY_DESCRIPTION'] = df['MODALITY_DESCRIPTION'].apply(lambda x: trim_string(x, 50))
    df_transform['SOP_CODE'] = df['SOP_CODE'].apply(lambda x: trim_string(x, 64))
    df_transform['SOP_DESCRIPTION'] = df['SOP_DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()
    logger.info(f"[OK] Transformed {len(df_transform)} Imaging Studies rows")
    return df_transform


def transform_supplies(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Supplies data"""
    logger.info("Transforming Supplies...")
    df_transform = pd.DataFrame()
    df_transform['DATE'] = df['DATE'].apply(safe_to_date)
    df_transform['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    df_transform['ENCOUNTER'] = df['ENCOUNTER'].apply(lambda x: trim_string(x, 36))
    df_transform['CODE'] = df['CODE'].apply(lambda x: trim_string(x, 20))
    df_transform['DESCRIPTION'] = df['DESCRIPTION'].apply(lambda x: trim_string(x, 255))
    df_transform['QUANTITY'] = df['QUANTITY'].apply(safe_to_int)
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()
    logger.info(f"[OK] Transformed {len(df_transform)} Supplies rows")
    return df_transform


def transform_organizations(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Organizations data"""
    logger.info("Transforming Organizations...")
    df_transform = pd.DataFrame()
    df_transform['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    df_transform['NAME'] = df['NAME'].apply(lambda x: trim_string(x, 255))
    df_transform['ADDRESS'] = df['ADDRESS'].apply(lambda x: trim_string(x, 255))
    df_transform['CITY'] = df['CITY'].apply(lambda x: trim_string(x, 100))
    df_transform['STATE'] = df['STATE'].apply(lambda x: trim_string(x, 50))
    df_transform['ZIP'] = df['ZIP'].apply(lambda x: trim_string(x, 10))
    df_transform['LAT'] = df['LAT'].apply(lambda x: safe_to_decimal(x, 10))
    df_transform['LON'] = df['LON'].apply(lambda x: safe_to_decimal(x, 10))
    df_transform['PHONE'] = df['PHONE'].apply(lambda x: trim_string(x, 20))
    df_transform['REVENUE'] = df['REVENUE'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['UTILIZATION'] = df['UTILIZATION'].apply(safe_to_int)
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()
    logger.info(f"[OK] Transformed {len(df_transform)} Organizations rows")
    return df_transform


def transform_providers(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Providers data"""
    logger.info("Transforming Providers...")
    df_transform = pd.DataFrame()
    df_transform['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    df_transform['ORGANIZATION'] = df['ORGANIZATION'].apply(lambda x: trim_string(x, 36))
    df_transform['NAME'] = df['NAME'].apply(lambda x: trim_string(x, 255))
    df_transform['GENDER'] = df['GENDER'].apply(lambda x: trim_string(x, 1))
    df_transform['SPECIALITY'] = df['SPECIALITY'].apply(lambda x: trim_string(x, 100))
    df_transform['ADDRESS'] = df['ADDRESS'].apply(lambda x: trim_string(x, 255))
    df_transform['CITY'] = df['CITY'].apply(lambda x: trim_string(x, 100))
    df_transform['STATE'] = df['STATE'].apply(lambda x: trim_string(x, 50))
    df_transform['ZIP'] = df['ZIP'].apply(lambda x: trim_string(x, 10))
    df_transform['LAT'] = df['LAT'].apply(lambda x: safe_to_decimal(x, 10))
    df_transform['LON'] = df['LON'].apply(lambda x: safe_to_decimal(x, 10))
    df_transform['UTILIZATION'] = df['UTILIZATION'].apply(safe_to_int)
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()
    logger.info(f"[OK] Transformed {len(df_transform)} Providers rows")
    return df_transform


def transform_payers(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Payers data"""
    logger.info("Transforming Payers...")
    df_transform = pd.DataFrame()
    df_transform['Id'] = df['Id'].apply(lambda x: trim_string(x, 36))
    df_transform['NAME'] = df['NAME'].apply(lambda x: trim_string(x, 255))
    df_transform['ADDRESS'] = df['ADDRESS'].apply(lambda x: trim_string(x, 255))
    df_transform['CITY'] = df['CITY'].apply(lambda x: trim_string(x, 100))
    df_transform['STATE_HEADQUARTERED'] = df['STATE_HEADQUARTERED'].apply(lambda x: trim_string(x, 50))
    df_transform['ZIP'] = df['ZIP'].apply(lambda x: trim_string(x, 10))
    df_transform['PHONE'] = df['PHONE'].apply(lambda x: trim_string(x, 20))
    df_transform['AMOUNT_COVERED'] = df['AMOUNT_COVERED'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['AMOUNT_UNCOVERED'] = df['AMOUNT_UNCOVERED'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['REVENUE'] = df['REVENUE'].apply(lambda x: safe_to_decimal(x, 2))
    df_transform['COVERED_ENCOUNTERS'] = df['COVERED_ENCOUNTERS'].apply(safe_to_int)
    df_transform['UNCOVERED_ENCOUNTERS'] = df['UNCOVERED_ENCOUNTERS'].apply(safe_to_int)
    df_transform['COVERED_MEDICATIONS'] = df['COVERED_MEDICATIONS'].apply(safe_to_int)
    df_transform['UNCOVERED_MEDICATIONS'] = df['UNCOVERED_MEDICATIONS'].apply(safe_to_int)
    df_transform['COVERED_PROCEDURES'] = df['COVERED_PROCEDURES'].apply(safe_to_int)
    df_transform['UNCOVERED_PROCEDURES'] = df['UNCOVERED_PROCEDURES'].apply(safe_to_int)
    df_transform['COVERED_IMMUNIZATIONS'] = df['COVERED_IMMUNIZATIONS'].apply(safe_to_int)
    df_transform['UNCOVERED_IMMUNIZATIONS'] = df['UNCOVERED_IMMUNIZATIONS'].apply(safe_to_int)
    df_transform['UNIQUE_CUSTOMERS'] = df['UNIQUE_CUSTOMERS'].apply(safe_to_int)
    df_transform['QOLS_AVG'] = df['QOLS_AVG'].apply(lambda x: safe_to_decimal(x, 4))
    df_transform['MEMBER_MONTHS'] = df['MEMBER_MONTHS'].apply(safe_to_int)
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()
    logger.info(f"[OK] Transformed {len(df_transform)} Payers rows")
    return df_transform


def transform_payer_transitions(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Payer Transitions data"""
    logger.info("Transforming Payer Transitions...")
    df_transform = pd.DataFrame()
    df_transform['PATIENT'] = df['PATIENT'].apply(lambda x: trim_string(x, 36))
    df_transform['START_YEAR'] = df['START_YEAR'].apply(safe_to_int)
    df_transform['END_YEAR'] = df['END_YEAR'].apply(safe_to_int)
    df_transform['PAYER'] = df['PAYER'].apply(lambda x: trim_string(x, 36))
    df_transform['OWNERSHIP'] = df['OWNERSHIP'].apply(lambda x: trim_string(x, 50))
    df_transform['create_at'] = datetime.now()
    df_transform['update_at'] = datetime.now()
    logger.info(f"[OK] Transformed {len(df_transform)} Payer Transitions rows")
    return df_transform


# ============================================================================
# MAIN TRANSFORMATION PIPELINE
# ============================================================================

def transform_landing_to_staging():
    """
    Main transformation pipeline

    Flow:
    1. Read from Landing_* tables
    2. Apply transformation logic
    3. Write to Staging_* tables
    """

    logger.info("\n" + "="*80)
    logger.info("STARTING TRANSFORMATION: Landing -> Staging")
    logger.info("="*80 + "\n")

    try:
        # Ensure key Staging columns are wide enough before loading
        logger.info("[STEP] Validating Staging schema compatibility...")
        ensure_staging_schema_compatibility()

        # Transformation tables mapping
        tables_to_transform = [
            ('Landing_Patients', 'Staging_Patients', transform_patients),
            ('Landing_Encounters', 'Staging_Encounters', transform_encounters),
            ('Landing_Observations', 'Staging_Observations', transform_observations),
            ('Landing_Medications', 'Staging_Medications', transform_medications),
            ('Landing_Conditions', 'Staging_Conditions', transform_conditions),
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
            start_time = datetime.now()

            try:
                logger.info(f"\n[Transform] {landing_table} -> {staging_table}")

                # Read from Landing
                conn_str = get_connection_string(Config.LANDING_DB)
                query = f"SELECT * FROM [{landing_table}]"
                df_landing = pd.read_sql(query, f'mssql+pyodbc:///?odbc_connect={conn_str}')

                logger.info(f"  Loaded {len(df_landing):,} rows from {landing_table}")

                # Transform
                df_staging = transform_func(df_landing)

                # Write to Staging using pyodbc (faster than SQLAlchemy)
                staging_conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={Config.SQL_SERVER};Database={Config.STAGING_DB};Trusted_Connection=yes;"
                staging_conn = pyodbc.connect(staging_conn_str, timeout=30)
                staging_cursor = staging_conn.cursor()

                # Clear existing data
                staging_cursor.execute(f"TRUNCATE TABLE [{staging_table}]")
                staging_conn.commit()

                # Build INSERT statement
                cols = list(df_staging.columns)
                col_str = ", ".join(f"[{col}]" for col in cols)
                placeholders = ", ".join("?" * len(cols))
                insert_sql = f"INSERT INTO [{staging_table}] ({col_str}) VALUES ({placeholders})"

                # Load in batches (faster than chunking through SQLAlchemy)
                total_inserted = 0
                for batch_start in range(0, len(df_staging), Config.BATCH_SIZE):
                    batch_end = min(batch_start + Config.BATCH_SIZE, len(df_staging))
                    batch = df_staging[batch_start:batch_end].copy()

                    rows = [tuple(row) for row in batch.values]
                    staging_cursor.executemany(insert_sql, rows)
                    staging_conn.commit()
                    total_inserted += len(batch)

                staging_cursor.close()
                staging_conn.close()

                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"[OK] Inserted {total_inserted:,} rows ({duration:.2f}s)")

                results.append({
                    'table': staging_table,
                    'status': 'SUCCESS',
                    'rows': total_inserted,
                    'duration': duration
                })

            except Exception as e:
                logger.error(f"[ERROR] Failed to transform {landing_table}: {str(e)}")
                logger.error(traceback.format_exc())
                results.append({
                    'table': staging_table,
                    'status': 'FAILED',
                    'rows': 0,
                    'error': str(e)
                })

        # Summary
        logger.info("\n" + "="*80)
        logger.info("TRANSFORMATION SUMMARY")
        logger.info("="*80)

        success_count = len([r for r in results if r['status'] == 'SUCCESS'])
        total_rows = sum(r['rows'] for r in results if r['status'] == 'SUCCESS')

        logger.info(f"\nSuccessful: {success_count}/{len(results)}")
        logger.info(f"Total rows transformed: {total_rows:,}\n")

        for result in results:
            if result['status'] == 'SUCCESS':
                logger.info(f"[OK] {result['table']:30} {result['rows']:>10,} rows  {result['duration']:>7.2f}s")
            else:
                logger.info(f"[ERROR] {result['table']:30} FAILED")

        logger.info("\n" + "="*80)
        logger.info("TRANSFORMATION COMPLETED")
        logger.info("="*80)

    except Exception as e:
        logger.error(f"\n[ERROR] Transformation pipeline failed: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        transform_landing_to_staging()
        logger.info("\n[OK] All transformations completed successfully!")
        sys.exit(0)
    except Exception as e:
        logger.error(f"\n[ERROR] ETL failed: {str(e)}")
        sys.exit(1)
