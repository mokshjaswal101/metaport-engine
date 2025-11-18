import pandas as pd
import xlsxwriter
from io import BytesIO
import base64
from typing import List, Dict, Any
import json


class ErrorExcelGenerator:
    """
    Generates error Excel files that match the exact format of the bulk upload template
    including colors and styling for easy user correction and re-upload.
    """

    # Define the exact column headers as per the upload template
    UPLOAD_TEMPLATE_HEADERS = [
        # Order Details
        "Order ID",
        "Order Date",
        "Channel",
        # Consignee Information
        "Consignee Full Name",
        "Consignee Phone Number",
        "Consignee Email",
        "Consignee Alternate Phone",
        "Consignee Company",
        "Consignee GSTIN",
        "Shipping Address",
        "Shipping Landmark",
        "Shipping Pincode",
        "Shipping City",
        "Shipping State",
        "Shipping Country",
        # Billing Details
        "Is Billing Same as Shipping",
        "Billing Full Name",
        "Billing Phone Number",
        "Billing Email",
        "Billing Address",
        "Billing Landmark",
        "Billing Pincode",
        "Billing City",
        "Billing State",
        "Billing Country",
        # Pickup Details
        "Pickup Location Code",
        # Product Information
        "Product Name",
        "Product Unit Price",
        "Product Quantity",
        "Product SKU Code",
        # Package Info
        "Package Length",
        "Package Breadth",
        "Package Height",
        "Package Weight",
        # Payment Information
        "Payment Mode",
        "Shipping Charges",
        "COD Charges",
        "Discount",
        "Gift Wrap Charges",
        "Other Charges",
        "Tax Percentage",
        # Error Information (additional columns for error reporting)
        "Error Description",
        "Error Field",
        "Suggested Fix",
    ]

    # Map frontend keys to Excel headers
    FRONTEND_TO_EXCEL_MAPPING = {
        "order_id": "Order ID",
        "order_date": "Order Date",
        "channel": "Channel",
        "consignee_full_name": "Consignee Full Name",
        "consignee_phone": "Consignee Phone Number",
        "consignee_email": "Consignee Email",
        "consignee_alternate_phone": "Consignee Alternate Phone",
        "consignee_company": "Consignee Company",
        "consignee_gstin": "Consignee GSTIN",
        "consignee_address": "Shipping Address",
        "consignee_landmark": "Shipping Landmark",
        "consignee_pincode": "Shipping Pincode",
        "consignee_city": "Shipping City",
        "consignee_state": "Shipping State",
        "consignee_country": "Shipping Country",
        "billing_is_same_as_consignee": "Is Billing Same as Shipping",
        "billing_full_name": "Billing Full Name",
        "billing_phone": "Billing Phone Number",
        "billing_email": "Billing Email",
        "billing_address": "Billing Address",
        "billing_landmark": "Billing Landmark",
        "billing_pincode": "Billing Pincode",
        "billing_city": "Billing City",
        "billing_state": "Billing State",
        "billing_country": "Billing Country",
        "pickup_location_code": "Pickup Location Code",
        "name": "Product Name",
        "unit_price": "Product Unit Price",
        "quantity": "Product Quantity",
        "sku_code": "Product SKU Code",
        "length": "Package Length",
        "breadth": "Package Breadth",
        "height": "Package Height",
        "weight": "Package Weight",
        "payment_mode": "Payment Mode",
        "shipping_charges": "Shipping Charges",
        "cod_charges": "COD Charges",
        "discount": "Discount",
        "gift_wrap_charges": "Gift Wrap Charges",
        "other_charges": "Other Charges",
        "tax_amount": "Tax Percentage",
    }

    # Required fields as per frontend validation
    REQUIRED_FIELDS = [
        "Order ID",
        "Order Date",
        "Consignee Full Name",
        "Consignee Phone Number",
        "Consignee Email",
        "Shipping Address",
        "Shipping Pincode",
        "Shipping City",
        "Shipping State",
        "Shipping Country",
        "Is Billing Same as Shipping",
        "Pickup Location Code",
        "Product Name",
        "Product Unit Price",
        "Product Quantity",
        "Package Length",
        "Package Breadth",
        "Package Height",
        "Package Weight",
        "Payment Mode",
    ]

    @classmethod
    def generate_error_excel(
        cls, error_data: List[Dict[str, Any]], filename: str = "error_orders.xlsx"
    ) -> str:
        """
        Generate an error Excel file with the same format as upload template

        Args:
            error_data: List of dictionaries containing order data with errors
            filename: Name of the output file

        Returns:
            Base64 encoded Excel file content
        """
        # Create an in-memory bytes buffer
        output = BytesIO()

        # Create workbook and worksheet
        workbook = xlsxwriter.Workbook(output, {"in_memory": True})
        worksheet = workbook.add_worksheet("Bulk Orders")

        # Define clean formats without complex colors
        # First header row (categories)
        category_header_format = workbook.add_format(
            {
                "bold": True,
                "border": 1,
                "align": "center",
                "valign": "vcenter",
                "font_size": 11,
            }
        )

        # Second header row (field names)
        field_header_format = workbook.add_format(
            {
                "bold": True,
                "border": 1,
                "align": "center",
                "valign": "vcenter",
                "font_size": 10,
            }
        )

        # Data cell formats
        error_cell_format = workbook.add_format(
            {
                "bg_color": "#FFCCCB",  # Light red background for error cells
                "border": 1,
                "valign": "top",
            }
        )

        normal_cell_format = workbook.add_format({"border": 1, "valign": "top"})

        error_info_cell_format = workbook.add_format(
            {
                "bg_color": "#FFF2CC",  # Light yellow background for error info
                "border": 1,
                "valign": "top",
                "font_size": 9,
            }
        )

        # Define category headers and their column spans
        categories = [
            ("Order Details", 3),  # Order ID, Order Date, Channel
            ("Consignee Information", 12),  # All consignee fields
            ("Billing Details", 10),  # All billing fields
            ("Pickup Details", 1),  # Pickup Location Code
            ("Product Information", 4),  # Product fields
            ("Package Info", 4),  # Package dimensions
            ("Payment Information", 7),  # Payment fields
            ("Error Information", 3),  # Error fields
        ]

        # Write first header row (categories)
        col_index = 0
        for category_name, span in categories:
            if span > 1:
                worksheet.merge_range(
                    0,
                    col_index,
                    0,
                    col_index + span - 1,
                    category_name,
                    category_header_format,
                )
            else:
                worksheet.write(0, col_index, category_name, category_header_format)
            col_index += span

        # Write second header row (field names) with simple formatting
        for col, header in enumerate(cls.UPLOAD_TEMPLATE_HEADERS):
            if header in cls.REQUIRED_FIELDS:
                worksheet.write(
                    1, col, header + "*", field_header_format
                )  # Add * for required
            else:
                worksheet.write(1, col, header, field_header_format)

        # Calculate and set proper column widths to fit headers without wrapping
        column_widths = []
        for header in cls.UPLOAD_TEMPLATE_HEADERS:
            # Calculate width based on header length, minimum 10, maximum 25
            header_length = len(header) + 2  # Add padding
            width = max(10, min(25, header_length))
            column_widths.append(width)

        # Set column widths
        for col, width in enumerate(column_widths):
            worksheet.set_column(col, col, width)

        # Process and write error data starting from row 2 (0-indexed)
        for row_idx, order_data in enumerate(error_data, start=2):
            error_fields = order_data.get("error_fields", [])
            error_description = order_data.get("error_description", "")
            error_field = order_data.get("error_field", "")
            suggested_fix = order_data.get("suggested_fix", "")

            # Write order data
            for col, header in enumerate(
                cls.UPLOAD_TEMPLATE_HEADERS[:-3]
            ):  # Exclude error columns
                # Map Excel header to data key
                data_key = None
                for frontend_key, excel_header in cls.FRONTEND_TO_EXCEL_MAPPING.items():
                    if excel_header == header:
                        data_key = frontend_key
                        break

                # Get the value from order data
                if data_key:
                    value = order_data.get(data_key, "")
                else:
                    # Handle special cases
                    if header == "Order ID":
                        value = order_data.get("order_id", "")
                    elif header == "Order Date":
                        value = order_data.get("order_date", "")
                    else:
                        value = ""

                # Apply formatting based on whether field has error
                excel_header_lower = header.lower().replace(" ", "_").replace("*", "")
                field_has_error = (
                    excel_header_lower in error_fields
                    or header in error_fields
                    or data_key in error_fields
                    if data_key
                    else False
                )

                if field_has_error:
                    worksheet.write(row_idx, col, value, error_cell_format)
                else:
                    worksheet.write(row_idx, col, value, normal_cell_format)

            # Write error information columns
            error_col_start = len(cls.UPLOAD_TEMPLATE_HEADERS) - 3
            worksheet.write(
                row_idx, error_col_start, error_description, error_info_cell_format
            )
            worksheet.write(
                row_idx, error_col_start + 1, error_field, error_info_cell_format
            )
            worksheet.write(
                row_idx, error_col_start + 2, suggested_fix, error_info_cell_format
            )

        # Add freeze panes to keep headers visible
        worksheet.freeze_panes(2, 0)  # Freeze first two rows

        # Add instructions worksheet
        instructions_worksheet = workbook.add_worksheet("Instructions")

        instruction_header_format = workbook.add_format(
            {"bold": True, "border": 1, "align": "center", "font_size": 14}
        )

        instruction_text_format = workbook.add_format(
            {"border": 1, "font_size": 11, "valign": "top"}
        )

        instructions = [
            "BULK UPLOAD ERROR CORRECTION GUIDE",
            "",
            "FORMATTING:",
            "• Bold Headers: Field names with proper spacing",
            "• Headers with (*): Required fields that must be filled",
            "• Light Red Cells: Fields with errors that need correction",
            "• Light Yellow Cells: Error descriptions and suggested fixes",
            "",
            "HOW TO FIX ERRORS:",
            "1. Look at the Error Description column to understand what's wrong",
            "2. Fix the errors in the Light Red cells in the same row",
            "3. Remove the last 3 columns (Error Description, Error Field, Suggested Fix)",
            "4. Save the file and re-upload",
            "",
            "VALIDATION RULES:",
            "• Order ID: Must be unique",
            "• Phone Numbers: Must be exactly 10 digits",
            "• Email: Must be valid email format (user@domain.com)",
            "• Pincode: Must be exactly 6 digits",
            "• Payment Mode: Only 'COD' or 'Prepaid' allowed",
            "• Order Date: Must be in YYYY-MM-DD format",
            "• Package Dimensions: Must be numeric values greater than 0",
            "• Prices: Must be numeric values only (no currency symbols)",
            "",
            "REQUIRED FIELDS (marked with *):",
            "• Order ID, Order Date, Consignee Full Name, Phone, Email",
            "• Shipping Address, Pincode, City, State, Country",
            "• Pickup Location Code, Product Name, Unit Price, Quantity",
            "• Package Length, Breadth, Height, Weight",
            "• Payment Mode",
            "",
            "NOTE: After fixing errors, make sure to delete the error information",
            "columns before re-uploading the file.",
        ]

        instructions_worksheet.write(0, 0, instructions[0], instruction_header_format)

        for i, instruction in enumerate(instructions[1:], start=1):
            if instruction.startswith("•"):
                bullet_format = workbook.add_format({"font_size": 10, "indent": 1})
                instructions_worksheet.write(i, 0, instruction, bullet_format)
            elif instruction.endswith(":"):
                section_format = workbook.add_format({"bold": True, "font_size": 11})
                instructions_worksheet.write(i, 0, instruction, section_format)
            else:
                instructions_worksheet.write(i, 0, instruction, instruction_text_format)

        instructions_worksheet.set_column("A:A", 80)

        # Close workbook
        workbook.close()

        # Return the file as base64 encoded string
        output.seek(0)
        return base64.b64encode(output.getvalue()).decode("utf-8")

    @classmethod
    def create_sample_error_data(cls) -> List[Dict[str, Any]]:
        """
        Create sample error data for testing
        """
        return [
            {
                "order_id": "ORD001",
                "order_date": "2025-08-28",
                "channel": "Website",
                "consignee_full_name": "",  # Error: Missing required field
                "consignee_phone": "12345",  # Error: Invalid phone number
                "consignee_email": "invalid-email",  # Error: Invalid email format
                "consignee_address": "123 Main St",
                "consignee_pincode": "12345",  # Error: Invalid pincode
                "consignee_city": "Mumbai",
                "consignee_state": "Maharashtra",
                "consignee_country": "India",
                "billing_is_same_as_consignee": "Yes",
                "pickup_location_code": "PUP001",
                "name": "Product 1",
                "unit_price": "abc",  # Error: Invalid price format
                "quantity": "2",
                "length": "10",
                "breadth": "10",
                "height": "10",
                "weight": "0.5",
                "payment_mode": "Invalid",  # Error: Invalid payment mode
                "error_fields": [
                    "consignee_full_name",
                    "consignee_phone",
                    "consignee_email",
                    "consignee_pincode",
                    "unit_price",
                    "payment_mode",
                ],
                "error_description": "Multiple validation errors found",
                "error_field": "consignee_full_name, consignee_phone, consignee_email, consignee_pincode, unit_price, payment_mode",
                "suggested_fix": "Fill name, use 10-digit phone, valid email, 6-digit pincode, numeric price, use COD/Prepaid",
            },
            {
                "order_id": "ORD002",
                "order_date": "",  # Error: Missing order date
                "channel": "App",
                "consignee_full_name": "John Doe",
                "consignee_phone": "9876543210",
                "consignee_email": "john@example.com",
                "consignee_address": "",  # Error: Missing address
                "consignee_pincode": "400001",
                "consignee_city": "Mumbai",
                "consignee_state": "Maharashtra",
                "consignee_country": "India",
                "billing_is_same_as_consignee": "Yes",
                "pickup_location_code": "",  # Error: Missing pickup location
                "name": "Product 2",
                "unit_price": "999",
                "quantity": "1",
                "length": "15",
                "breadth": "15",
                "height": "15",
                "weight": "1.0",
                "payment_mode": "COD",
                "error_fields": [
                    "order_date",
                    "consignee_address",
                    "pickup_location_code",
                ],
                "error_description": "Missing required fields",
                "error_field": "order_date, consignee_address, pickup_location_code",
                "suggested_fix": "Add order date (YYYY-MM-DD), complete address, valid pickup location code",
            },
        ]


def generate_error_excel_file(
    error_orders: List[Dict[str, Any]], filename: str = "bulk_upload_errors.xlsx"
) -> str:
    """
    Utility function to generate error Excel file

    Args:
        error_orders: List of order dictionaries with error information
        filename: Output filename

    Returns:
        Base64 encoded Excel file content
    """
    return ErrorExcelGenerator.generate_error_excel(error_orders, filename)


# Example usage and testing
if __name__ == "__main__":
    # Create sample error data
    sample_errors = ErrorExcelGenerator.create_sample_error_data()

    # Generate error Excel file
    excel_content = ErrorExcelGenerator.generate_error_excel(
        sample_errors, "test_error_file.xlsx"
    )

    # Save to file for testing
    with open("test_error_file.xlsx", "wb") as f:
        f.write(base64.b64decode(excel_content))

    print("Error Excel file generated successfully!")
    print("File contains sample error data with proper formatting and styling.")
