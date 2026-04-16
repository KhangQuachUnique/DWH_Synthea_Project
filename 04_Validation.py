"""
================================================================================
SYNTHEA DATA VALIDATION SCRIPT
================================================================================
Kiểm tra chuẩn bị trước khi chạy ETL:
- Kiểm tra CSV files có đủ không
- Kiểm tra SQL Server connection
- Kiểm tra Python dependencies
- Báo cáo dung lượng dữ liệu

Chạy: python 04_Validation.py
================================================================================
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import importlib.util


def check_module(module_name: str) -> bool:
    """Kiểm tra module Python đã cài đặt chưa"""
    spec = importlib.util.find_spec(module_name)
    return spec is not None


def get_file_size_mb(file_path: Path) -> float:
    """Lấy kích thước file (MB)"""
    return file_path.stat().st_size / (1024 * 1024)


def main():
    """Main validation"""

    print("\n" + "="*80)
    print("SYNTHEA ETL - PRE-FLIGHT VALIDATION")
    print("="*80)

    # Step 1: Check Python dependencies
    print("\n[1] Checking Python Dependencies")
    print("-" * 80)

    dependencies = ['pandas', 'pyodbc', 'tqdm']
    all_installed = True

    for dep in dependencies:
        if check_module(dep):
            print(f"  [OK] {dep:15} installed")
        else:
            print(f"  [ERR] {dep:15} NOT installed")
            all_installed = False

    if not all_installed:
        print("\n[ERROR] Missing dependencies!")
        print("Run: pip install pandas pyodbc tqdm")
        return False

    # Step 2: Check CSV files
    print("\n[2] Checking CSV Files")
    print("-" * 80)

    project_root = Path(__file__).parent
    csv_path = project_root / "data" / "raw" / "synthea" / "csv"

    expected_files = {
        'allergies.csv': 5417,
        'careplans.csv': 37715,
        'conditions.csv': 114544,
        'devices.csv': 2360,
        'encounters.csv': 321528,
        'imaging_studies.csv': 4504,
        'immunizations.csv': 16481,
        'medications.csv': 431262,
        'observations.csv': 1659750,
        'organizations.csv': 5499,
        'patients.csv': 12352,
        'payer_transitions.csv': 41392,
        'payers.csv': 10,
        'procedures.csv': 100427,
        'providers.csv': 31764,
        'supplies.csv': 143110,
    }

    if not csv_path.exists():
        print(f"  [ERR] CSV directory not found: {csv_path}")
        print(f"\n  Please create folder and copy CSV files:")
        print(f"  {csv_path}")
        return False

    print(f"  CSV Path: {csv_path}\n")

    found_files = 0
    total_size_mb = 0
    total_expected_rows = 0

    for filename, expected_rows in expected_files.items():
        file_path = csv_path / filename

        if file_path.exists():
            file_size = get_file_size_mb(file_path)
            total_size_mb += file_size
            total_expected_rows += expected_rows
            print(f"  [OK] {filename:25} {file_size:>8.2f} MB  (~{expected_rows:>10,} rows)")
            found_files += 1
        else:
            print(f"  [ERR] {filename:25} NOT FOUND")

    print(f"\n  Files found: {found_files}/{len(expected_files)}")
    print(f"  Total size: {total_size_mb:.2f} MB")
    print(f"  Total rows expected: {total_expected_rows:,}")

    if found_files == 0:
        print("\n  [ERROR] No CSV files found! Please copy data to:")
        print(f"  {csv_path}")
        return False

    if found_files < len(expected_files):
        missing = len(expected_files) - found_files
        print(f"\n  [WARNING] {missing} files missing")

    # Step 3: Check SQL Server connectivity
    print("\n[3] Checking SQL Server Connection")
    print("-" * 80)

    try:
        import pyodbc

        print("  Attempting connection to SQL Server...")
        
        # Get SQL Server name
        sql_server = os.environ.get('SYNTHEA_SQL_SERVER', 'localhost')
        
        # Auto-detect ODBC Driver
        installed_drivers = pyodbc.drivers()
        odbc_driver = None
        
        for preferred in ['ODBC Driver 18 for SQL Server', 'ODBC Driver 17 for SQL Server']:
            if preferred in installed_drivers:
                odbc_driver = preferred
                break
        
        if not odbc_driver:
            for driver in reversed(installed_drivers):
                if 'SQL Server' in driver:
                    odbc_driver = driver
                    break
        
        if not odbc_driver:
            print(f"  [ERR] No ODBC Driver found!")
            print(f"  Installed drivers: {installed_drivers}")
            print(f"\n  Please install ODBC Driver 17 or 18 for SQL Server")
            return False
        
        print(f"  Using ODBC Driver: {odbc_driver}")
        
        # Build connection string with Windows Authentication
        conn_str = f"DRIVER={{{odbc_driver}}};SERVER={sql_server};Trusted_Connection=yes;"
        
        # Add encryption settings for ODBC 18
        if 'Driver 18' in odbc_driver:
            conn_str += "Encrypt=no;TrustServerCertificate=yes;"
        
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()

        # Get SQL Server version
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print(f"  [OK] Connected successfully!")
        print(f"  [OK] SQL Server: {version[:60]}...")

        # Check if databases exist
        cursor.execute("""
            SELECT name FROM sys.databases
            WHERE name IN ('DW_Synthea_Landing', 'DW_Synthea_Staging')
        """)

        existing_dbs = [row[0] for row in cursor.fetchall()]

        print(f"\n  Database Status:")
        for db in ['DW_Synthea_Landing', 'DW_Synthea_Staging']:
            if db in existing_dbs:
                print(f"    [OK] {db:30} exists")
            else:
                print(f"    [~] {db:30} needs to be created")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"  [ERR] Connection failed: {str(e)}")
        print(f"\n  [ERROR] Cannot connect to SQL Server!")
        print("  Troubleshooting:")
        print("    1. Is SQL Server running?")
        print("    2. Is ODBC Driver installed?")
        print("    3. Correct server name?")
        print(f"    4. Current server: {sql_server}")
        print("    5. Windows Authentication có quyền truy cập?")
        return False

    # Step 4: Check disk space
    print("\n[4] Checking Disk Space")
    print("-" * 80)

    try:
        import shutil
        disk_usage = shutil.disk_usage(str(project_root))
        free_gb = disk_usage.free / (1024**3)
        total_gb = disk_usage.total / (1024**3)

        print(f"  Total disk: {total_gb:.2f} GB")
        print(f"  Free space: {free_gb:.2f} GB")

        # Estimate needed space (raw data + 2x for staging/indexes)
        needed_gb = (total_size_mb / 1024) * 3
        print(f"  Estimated needed: ~{needed_gb:.2f} GB")

        if free_gb < needed_gb:
            print(f"\n  [WARNING] Not enough free space!")
        else:
            print(f"  [OK] Sufficient free space")

    except Exception as e:
        print(f"  [~] Could not check disk space: {str(e)}")

    # Step 5: Show system info
    print("\n[5] System Information")
    print("-" * 80)

    print(f"  Python version: {sys.version.split()[0]}")
    print(f"  Platform: {sys.platform}")
    print(f"  Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Final summary
    print("\n" + "="*80)
    print("VALIDATION SUMMARY")
    print("="*80)

    if all_installed and found_files > 0:
        print("\n[OK] Validation checks passed!")
        print("\nNext steps:")
        print("  1. Run SQL scripts in SQL Server Management Studio:")
        print("     - 01_Schema_Landing.sql")
        print("     - 03_Schema_Staging.sql")
        print("  2. Extract:   python 02_ETL_Synthea_Extract.py")
        print("  3. Transform: python 05_Transform_Landing_to_Staging.py")
        return True
    else:
        print("\n[WARNING] Some checks failed. Fix issues before running ETL.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)