"""
Script to upload pincode master Excel file to pincode_mapping table.

Usage:
    python scripts/upload_pincode_master.py <path_to_excel_file>

Example:
    python scripts/upload_pincode_master.py pincode_master.xlsx
"""

import sys
import os

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.adhoc import upload_pincode_master


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/upload_pincode_master.py <path_to_excel_file>")
        print("\nExample:")
        print("    python scripts/upload_pincode_master.py pincode_master.xlsx")
        sys.exit(1)

    excel_file_path = sys.argv[1]

    # Optional: Set update_existing to False if you want to skip duplicates instead of updating
    update_existing = True
    if len(sys.argv) > 2 and sys.argv[2].lower() == "false":
        update_existing = False

    try:
        print(f"Reading Excel file: {excel_file_path}")
        print(f"Update existing records: {update_existing}")
        print("-" * 50)

        result = upload_pincode_master(excel_file_path, update_existing=update_existing)

        print("\n" + "=" * 50)
        print("UPLOAD SUMMARY")
        print("=" * 50)
        print(f"Total rows in file: {result['total_rows_in_file']}")
        print(f"Inserted: {result['inserted']}")
        print(f"Updated: {result['updated']}")
        print(f"Skipped: {result['skipped']}")
        print(f"Errors: {result['errors']}")

        if result["error_details"]:
            print("\nFirst few errors:")
            for error in result["error_details"]:
                print(f"  Pincode {error['pincode']}: {error['error']}")

        print("\n" + "=" * 50)
        print("Upload completed successfully!")

    except Exception as e:
        print(f"\nERROR: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
