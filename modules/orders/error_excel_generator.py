"""
Excel Error File Generator for Last Miles Service
This module provides functionality to generate standardized Excel error files
for bulk order import operations with proper formatting and error categorization.
"""

import pandas as pd
from io import BytesIO
from datetime import datetime
from typing import List, Dict, Any, Optional
from modules.orders.order_schema import BulkImportValidationError


class ErrorExcelGenerator:
    """
    Generates Excel files for bulk import errors with standardized formatting
    """

    # Error category mapping for user-friendly display
    ERROR_CATEGORY_MAP = {
        "schema_validation": "Data Validation Error",
        "duplicate_order": "Duplicate Order ID",
        "invalid_pickup_location": "Invalid Pickup Location",
        "zone_calculation": "Pincode/Zone Error",
        "processing_error": "Order Processing Error",
        "model_creation": "Database Model Error",
        "insert_error": "Database Insert Error",
        "courier_validation": "Courier Validation Error",
        "product_validation": "Product Information Error",
        "address_validation": "Address Validation Error",
        "payment_validation": "Payment Mode Error",
        "dimension_validation": "Package Dimension Error",
        "weight_validation": "Weight Validation Error",
        "phone_validation": "Phone Number Error",
        "email_validation": "Email Validation Error",
        "pincode_validation": "Pincode Validation Error",
        "amount_validation": "Amount Validation Error",
    }

    # Standard Excel columns for error reporting
    ERROR_COLUMNS = [
        "order_id",
        "error_category",
        "error_description",
        "problematic_field",
        "error_type",
        "suggested_fix",
    ]

    # Standard order columns to include in error file
    ORDER_COLUMNS = [
        "consignee_full_name",
        "consignee_phone",
        "consignee_email",
        "consignee_address",
        "consignee_pincode",
        "consignee_city",
        "consignee_state",
        "payment_mode",
        "total_amount",
        "order_value",
        "shipping_charges",
        "cod_charges",
        "weight",
        "length",
        "breadth",
        "height",
        "pickup_location_code",
        "order_date",
        "channel",
    ]

    @classmethod
    def generate_error_excel(
        cls,
        validation_errors: List[BulkImportValidationError],
        original_orders: List[Dict[str, Any]],
        client_id: int,
        include_suggestions: bool = True,
    ) -> BytesIO:
        """
        Generate a formatted Excel file containing error details and original order data

        Args:
            validation_errors: List of validation errors
            original_orders: List of original order dictionaries
            client_id: Client ID for file naming
            include_suggestions: Whether to include suggested fixes

        Returns:
            BytesIO: Excel file buffer
        """
        error_data = []

        for error in validation_errors:
            # Find corresponding original order data
            original_order = next(
                (
                    order
                    for order in original_orders
                    if order.get("order_id") == error.order_id
                ),
                {},
            )

            # Get user-friendly error category
            error_category = cls.ERROR_CATEGORY_MAP.get(
                error.error_type, error.error_type
            )

            # Generate suggested fix
            suggested_fix = (
                cls._generate_suggested_fix(error) if include_suggestions else "N/A"
            )

            # Create error row with reordered columns (error info first)
            error_row = {
                "order_id": error.order_id,
                "error_category": error_category,
                "error_description": error.error_message,
                "problematic_field": error.field_name or "N/A",
                "error_type": error.error_type,
                "suggested_fix": suggested_fix,
            }

            # Add original order data (only relevant columns)
            for column in cls.ORDER_COLUMNS:
                error_row[column] = original_order.get(column, "")

            # Handle products array specially
            if "products" in original_order and isinstance(
                original_order["products"], list
            ):
                for idx, product in enumerate(original_order["products"], 1):
                    error_row[f"product_{idx}_name"] = product.get("name", "")
                    error_row[f"product_{idx}_sku"] = product.get("sku_code", "")
                    error_row[f"product_{idx}_quantity"] = product.get("quantity", "")
                    error_row[f"product_{idx}_price"] = product.get("unit_price", "")

            error_data.append(error_row)

        # Create DataFrame
        df = pd.DataFrame(error_data)

        # Create Excel file with formatting
        return cls._create_formatted_excel(df, client_id)

    @classmethod
    def _create_formatted_excel(cls, df: pd.DataFrame, client_id: int) -> BytesIO:
        """
        Create formatted Excel file with styling and validation
        """
        excel_buffer = BytesIO()

        with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
            # Write data to Excel
            df.to_excel(writer, index=False, sheet_name="Error_Orders")

            # Get workbook and worksheet objects for formatting
            workbook = writer.book
            worksheet = writer.sheets["Error_Orders"]

            # Define formats
            header_format = workbook.add_format(
                {
                    "bold": True,
                    "text_wrap": True,
                    "valign": "top",
                    "bg_color": "#D7E4BC",
                    "border": 1,
                    "font_size": 11,
                }
            )

            error_format = workbook.add_format(
                {"text_wrap": True, "valign": "top", "bg_color": "#FFE6E6", "border": 1}
            )

            critical_error_format = workbook.add_format(
                {
                    "text_wrap": True,
                    "valign": "top",
                    "bg_color": "#FF9999",
                    "border": 1,
                    "bold": True,
                }
            )

            warning_format = workbook.add_format(
                {"text_wrap": True, "valign": "top", "bg_color": "#FFF2CC", "border": 1}
            )

            # Apply header formatting
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

            # Set column widths
            column_widths = {
                "order_id": 15,
                "error_category": 25,
                "error_description": 50,
                "problematic_field": 20,
                "error_type": 20,
                "suggested_fix": 40,
                "consignee_full_name": 25,
                "consignee_phone": 15,
                "consignee_email": 25,
                "consignee_address": 40,
                "consignee_pincode": 12,
                "consignee_city": 15,
                "consignee_state": 15,
                "payment_mode": 12,
                "total_amount": 12,
                "weight": 10,
                "length": 8,
                "breadth": 8,
                "height": 8,
            }

            # Apply column widths and formatting
            for col_num, column in enumerate(df.columns):
                width = column_widths.get(column, 15)
                worksheet.set_column(col_num, col_num, width)

                # Apply conditional formatting based on error severity
                if column == "error_description":
                    worksheet.set_column(col_num, col_num, width, error_format)
                elif column == "error_category":
                    # Color code based on error type
                    for row_num in range(1, len(df) + 1):
                        cell_value = (
                            df.iloc[row_num - 1]["error_type"]
                            if row_num - 1 < len(df)
                            else ""
                        )
                        if cell_value in [
                            "duplicate_order",
                            "schema_validation",
                            "insert_error",
                        ]:
                            worksheet.write(
                                row_num,
                                col_num,
                                df.iloc[row_num - 1][column],
                                critical_error_format,
                            )
                        elif cell_value in [
                            "zone_calculation",
                            "invalid_pickup_location",
                        ]:
                            worksheet.write(
                                row_num,
                                col_num,
                                df.iloc[row_num - 1][column],
                                warning_format,
                            )
                        else:
                            worksheet.write(
                                row_num,
                                col_num,
                                df.iloc[row_num - 1][column],
                                error_format,
                            )

            # Add summary at the top
            worksheet.write("A1", "ERROR SUMMARY", header_format)
            summary_row = len(df) + 3
            worksheet.write(f"A{summary_row}", "Total Errors:", header_format)
            worksheet.write(f"B{summary_row}", len(df))

            # Error type breakdown
            error_types = df["error_category"].value_counts()
            for idx, (error_type, count) in enumerate(error_types.items()):
                worksheet.write(
                    f"A{summary_row + idx + 1}", f"{error_type}:", header_format
                )
                worksheet.write(f"B{summary_row + idx + 1}", count)

        excel_buffer.seek(0)
        return excel_buffer

    @classmethod
    def _generate_suggested_fix(cls, error: BulkImportValidationError) -> str:
        """
        Generate suggested fix based on error type and field
        """
        suggestions = {
            "schema_validation": "Check data format and ensure all required fields are provided",
            "duplicate_order": "Use a unique order ID that doesn't already exist in the system",
            "invalid_pickup_location": "Use a valid pickup location code from your configured locations",
            "zone_calculation": "Verify the pincode is valid and serviceable",
            "processing_error": "Review order data for any formatting issues or missing information",
            "model_creation": "Contact support - this appears to be a system error",
            "insert_error": "Contact support - this appears to be a database error",
            "courier_validation": "Select a valid courier partner from available options",
            "product_validation": "Ensure product information is complete with valid SKU, price, and quantity",
            "address_validation": "Provide complete address with all required fields",
            "payment_validation": "Use 'COD' or 'prepaid' as payment mode",
            "dimension_validation": "Provide valid dimensions (length, breadth, height) in cm",
            "weight_validation": "Provide valid weight in kg (greater than 0)",
            "phone_validation": "Use a valid 10-digit phone number",
            "email_validation": "Provide a valid email address format",
            "pincode_validation": "Use a valid 6-digit pincode",
            "amount_validation": "Ensure amounts are positive numbers",
        }

        base_suggestion = suggestions.get(
            error.error_type, "Review the error message and correct the data"
        )

        # Add field-specific suggestions
        if error.field_name:
            field_suggestions = {
                "consignee_phone": "Format: 10 digits without country code (e.g., 9876543210)",
                "consignee_pincode": "Format: 6-digit Indian pincode (e.g., 110001)",
                "payment_mode": "Use either 'COD' or 'prepaid'",
                "weight": "Weight should be in kg and greater than 0",
                "total_amount": "Amount should be a positive number",
                "order_date": "Format: YYYY-MM-DD HH:MM:SS",
                "consignee_email": "Format: example@domain.com",
            }

            if error.field_name in field_suggestions:
                base_suggestion += f". {field_suggestions[error.field_name]}"

        return base_suggestion

    @classmethod
    def create_sample_error_excel(cls, client_id: int = 1) -> BytesIO:
        """
        Create a sample error Excel file for demonstration purposes
        """
        sample_errors = [
            BulkImportValidationError(
                order_id="ORD001",
                error_type="schema_validation",
                error_message="Missing required field: consignee_phone",
                field_name="consignee_phone",
            ),
            BulkImportValidationError(
                order_id="ORD002",
                error_type="duplicate_order",
                error_message="Order ID already exists in system",
                field_name="order_id",
            ),
            BulkImportValidationError(
                order_id="ORD003",
                error_type="pincode_validation",
                error_message="Invalid pincode format",
                field_name="consignee_pincode",
            ),
        ]

        sample_orders = [
            {
                "order_id": "ORD001",
                "consignee_full_name": "John Doe",
                "consignee_phone": "",  # Missing phone
                "consignee_email": "john@example.com",
                "consignee_address": "123 Main St",
                "consignee_pincode": "110001",
                "consignee_city": "Delhi",
                "consignee_state": "Delhi",
                "payment_mode": "COD",
                "total_amount": 500.0,
                "weight": 1.0,
            },
            {
                "order_id": "ORD002",  # Duplicate
                "consignee_full_name": "Jane Smith",
                "consignee_phone": "9876543210",
                "consignee_email": "jane@example.com",
                "consignee_address": "456 Oak Ave",
                "consignee_pincode": "110002",
                "consignee_city": "Delhi",
                "consignee_state": "Delhi",
                "payment_mode": "prepaid",
                "total_amount": 750.0,
                "weight": 2.0,
            },
            {
                "order_id": "ORD003",
                "consignee_full_name": "Bob Wilson",
                "consignee_phone": "9876543211",
                "consignee_email": "bob@example.com",
                "consignee_address": "789 Pine Rd",
                "consignee_pincode": "INVALID",  # Invalid pincode
                "consignee_city": "Mumbai",
                "consignee_state": "Maharashtra",
                "payment_mode": "COD",
                "total_amount": 300.0,
                "weight": 0.5,
            },
        ]

        return cls.generate_error_excel(sample_errors, sample_orders, client_id)


# Example usage function
def generate_bulk_import_error_file(
    validation_errors: List[BulkImportValidationError],
    original_orders: List[Dict[str, Any]],
    client_id: int,
) -> str:
    """
    Generate error Excel file and return the file path or S3 URL

    Args:
        validation_errors: List of validation errors
        original_orders: Original order data
        client_id: Client ID

    Returns:
        str: File path or S3 URL of the generated error file
    """
    # Generate Excel file
    excel_buffer = ErrorExcelGenerator.generate_error_excel(
        validation_errors, original_orders, client_id
    )

    # Upload to S3 (using existing upload function)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{client_id}/bulk_upload_errors/bulk_upload_error_{timestamp}.xlsx"

    # Use your existing S3 upload function
    from utils.aws_s3 import upload_file_to_s3

    upload_result = upload_file_to_s3(
        file_obj=excel_buffer,
        s3_key=s3_key,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    if upload_result.get("success"):
        return upload_result.get("url")
    else:
        raise Exception("Failed to upload error file to S3")
