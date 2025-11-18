"""
Thermal shipping label template
"""

from datetime import datetime
from pytz import timezone
from context_manager.context import get_db_session
from models import Courier_Routing_Code
from logger import logger
from modules.orders.order_schema import Order_Model
from modules.documents.shipping_label.shipping_label_schema import (
    LabelSettingResponseModel,
)

from .common.utils import (
    generate_barcode_image,
    convert_image_to_base64,
    get_base64_from_s3_url,
    sanitize_product_name,
)

from utils.string import truncate_text


def generate_thermal_label(
    order: Order_Model,
    settings: LabelSettingResponseModel,
    client_name: str,
    client_id: int,
) -> str:
    """Generate thermal shipping label HTML"""
    try:
        db = get_db_session()
        # Generate barcodes
        awb_barcode = convert_image_to_base64(generate_barcode_image(order.awb_number))
        order_id_barcode = (
            convert_image_to_base64(generate_barcode_image(order.order_id))
            if settings.order_id_barcode_enabled
            else ""
        )

        # Handle company logo
        base64_logo = ""
        if settings.logo_url and settings.logo_shown:
            base64_logo = get_base64_from_s3_url(settings.logo_url)

        # Process products (limit to 5 for thermal format)
        product_rows = ""
        total_quantity = 0
        total_amount = 0.0

        # Determine column display based purely on settings
        show_sku_column = settings.SKU
        show_product_name = settings.product_name

        # Dynamic width calculation based on column configuration
        if show_sku_column and show_product_name:
            product_col_width = 50
            sku_col_width = 30
            qty_col_width = 10
            amount_col_width = 15
            product_name_length = 30  # Shorter when both columns shown
            product_sku_length = 15

        elif show_sku_column and not show_product_name:
            product_col_width = 50  # SKU only (no product name)
            sku_col_width = 0  # Not used in this case
            qty_col_width = 25
            amount_col_width = 25
            product_name_length = 0
            product_sku_length = 40

        else:
            product_col_width = 60  # Product name only
            sku_col_width = 0
            qty_col_width = 20
            amount_col_width = 20
            product_name_length = 40
            product_sku_length = 0

        max_product_rows = 3

        # Loop through the products and process up to max_product_rows
        for i, product in enumerate(order.products[:max_product_rows]):
            # Determine what to show in the main product column based on settings
            if show_product_name:
                main_content = sanitize_product_name(
                    product["name"], product_name_length
                )
            else:
                main_content = truncate_text(
                    product.get("sku_code") or "-", product_sku_length
                )

            product_quantity = float(product["quantity"])
            product_unit_price = float(product["unit_price"])
            product_total = product_quantity * product_unit_price

            # Accumulate totals
            total_quantity += product_quantity
            total_amount += product_total

            # SKU column: render separate SKU cell only when both product name and SKU
            # are enabled. If product name is disabled, SKU is shown in the main column.
            sku_column = (
                f"<td>{truncate_text(product.get('sku_code') or '-', product_sku_length)}</td>"
                if (show_sku_column and show_product_name)
                else ""
            )

            # Add the product row
            product_rows += f"""
                <tr style="border:0; font-size: 9px; padding-bottom:1px; padding-top:1px">
                    <td>{main_content}</td>
                    {sku_column}
                    <td style="text-align:center">{product_quantity}</td>
                    <td style="text-align:center">{product_total:.2f}</td>
                </tr>
                """

        # Add "..." row if there are more than 5 products
        if len(order.products) > max_product_rows:
            product_rows += f"""
                <tr style="border:0; font-size: 9px; padding-bottom:1px; padding-top:1px">
                    <td> +  {order.product_quantity - total_quantity} Items ...</td>
                    {'<td>&nbsp;</td>' if (show_sku_column and show_product_name) else ''}
                    <td>&nbsp;</td>
                    <td>&nbsp;</td>
                </tr>
                """

        # Calculate the number of rows to fill up total product rows
        num_rows = len(order.products[:max_product_rows]) + (
            1 if len(order.products) > max_product_rows else 0
        )  # 1 for "..." row
        empty_rows_needed = max(0, max_product_rows - num_rows)

        # Add empty rows to make up to 4 total rows
        for _ in range(empty_rows_needed):
            product_rows += f"""
                <tr style="border:0; font-size: 9px; padding-bottom:1px; padding-top:1px; color:white">
                    <td>&nbsp;</td>
                    {'<td>&nbsp;</td>' if (show_sku_column and show_product_name) else ''}
                    <td>&nbsp;</td>
                    <td>&nbsp;</td>
                </tr>
                """

        # Add the total row at the end
        product_rows += f"""
            <tr style="border:0; font-size: 9px; font-weight:bold; padding-bottom:1px; padding-top:1px">
                <td>Total</td>
                {'<td>&nbsp;</td>' if (show_sku_column and show_product_name) else ''}
                <td style="text-align:center">{order.product_quantity}</td>
                <td style="text-align:center">{order.order_value:.2f}</td>
            </tr>
            """

        # Column header logic - determine what to show in main column header
        if show_product_name:
            main_column_header = "Product"
        else:
            main_column_header = "SKU"

        # Create the SKU column header conditionally
        sku_header_column = (
            f'<td style="width:{sku_col_width}%"><b>SKU</b></td>'
            if (show_sku_column and show_product_name)
            else ""
        )

        # Get current time
        ist = timezone("Asia/Kolkata")
        order_date = (
            order.booking_date.astimezone(ist).strftime("%Y-%m-%d, %H:%M:%S")
            if order.booking_date
            else order.order_date.astimezone(ist).strftime("%Y-%m-%d, %H:%M:%S")
        )

        # Get routing  and cluster code in case of Bluedart
        routing_code = None
        cluster_code = None

        if order.courier_partner and "bluedart" in order.courier_partner.lower():
            codes = (
                db.query(Courier_Routing_Code)
                .filter(
                    Courier_Routing_Code.pincode == order.consignee_pincode,
                )
                .first()
            )

            routing_code = codes.bluedart_routing_code if codes else ""
            cluster_code = codes.bluedart_cluster_code if codes else ""

        branding = (
            f"""<tr><td style="width:100%; font-size:9px; padding:3px; text-align:center ">Powered by<b> Last Miles @ Warehousity</b></td></td></tr>"""
            if settings.branding
            else ""
        )

        message = (
            f""" <tr>
                        <td style="width:100%;">
                            All disputes will be resolved under {order.pickup_location.state} jurisdiction. Sold goods are eligible for return or exchange according to the store's policy.
                        </td>
                    </tr>"""
            if settings.message
            else ""
        )

        gst = ""

        # Defining the HTML for the label structure
        # Combine and truncate consignee and pickup addresses for display
        consignee_combined = f"{order.consignee_address}{' ' + order.consignee_landmark if getattr(order, 'consignee_landmark', None) else ''}"
        consignee_display = truncate_text(consignee_combined, 120)

        pickup_combined = f"{order.pickup_location.address}{' ' + getattr(order.pickup_location, 'landmark', '') if getattr(order.pickup_location, 'landmark', None) else ''}"
        pickup_display = truncate_text(pickup_combined, 120)

        html_content = f"""
            <html>
            <head>
                <style>
                    @page {{
                        size: 4in 6in; /* Set page size to 6x4 inches */
                        margin: 5px; /* Optional: remove default margin */
                    }}
                    body {{
                        font-family: Arial, sans-serif;
                        margin: 0;
                        padding: 0;
                        width: 100%;
                        font-size: 10px;
                        border: 1px solid black
                    }}
                    
                    .header {{
                        padding: 5px;
                        
                    }}
                    
                    .section {{
                        margin-bottom: 10px;
                        padding-bottom: 10px;
                    }}
                    .table {{
                        width: 100%;
                        border-collapse: collapse;
                        border: 0;
                    }}
                    .table td, .table th {{
                        padding: 5px;
                    }}
                </style>
            </head>
            <body>
                <table style="border: 1px solid black;">
                <tr>
                <td style="">
                
                    <div>
                        <table class="header" style="border-bottom: 1px solid black;" >
                        <tr>
                <td style="width:60%; font-size:10px ; ">
                <span style="font-weight:bold">Ship To :</span><br />
                    <span>{order.consignee_full_name}</span><br />
                    <span>{consignee_display}</span><br />
                    <span>{order.consignee_pincode}, {order.consignee_city}, {order.consignee_state}</span><br /> 
                    {f'<span><b>Mobile Number</b> - {order.consignee_phone}</span>' if settings.consignee_phone else ""}
                </td>
    <td style="width:40%">
                        {f'<img style="width:100px;" src="{base64_logo}" alt="Logo"/>' if settings.logo_shown and base64_logo else ""}
                    </td>                    </tr >
                        </table>
                    </div>
                    <div ">
                        <table class="header" style="font-size:10px;border-bottom: 1px solid black;">
                        <tbody>
                            <tr>
                                <td style="width:40%; margin:0">
                                    {f'<span><b>Dim</b>: {int(order.length) if order.length == int(order.length) else order.length} x {int(order.breadth) if order.breadth == int(order.breadth) else order.breadth} x {int(order.height) if order.height == int(order.height) else order.height} cm </span>  <br /> ' if settings.package_dimensions else "" }
                                                                
                                    {f'<span><b>Weight</b>: {order.weight} kg </span>  <br />' if settings.weight else ''}
                                
                                    {f'<span> <b>Date</b>: {order_date if settings.order_date else ""}'}
                                    
                                    {f'<span> <b>Payment mode</b>: {order.payment_mode.upper()}</span>' if settings.payment_type else ""}
                                </td>
                                <td style="width:60%;  margin:0">
                                    <span>
                                        <b>Courier</b> - {order.courier_partner}
                                    </span> <br />
        
                                    <span style="width:70%;">                           
                                        <img  style="width:400px; margin: 10px;  height: 100px;" src="data:image/png;base64,{awb_barcode}" alt="Awb Barcode" />
                                    </span> <br />
                                    
                                    <span>
                                        <b>AWB</b> - {order.awb_number}
                                    </span>
                                    
                                    <br />
                                    
                                    
                                    {
                                        f'<span><b>Routing Code</b> - {routing_code}</span>' if routing_code else ""
                                    }
                                    <br/>
                                    {
                                        f'<span><b>Cluster Code</b> - {cluster_code}</span>' if cluster_code else ""
                                    }
                                </td>
                            </tr>
                        </tbody>
                        </table>
                    </span>
                    
                    <table class="header table" style="border-bottom: 1px solid black">
                            <tr>                                        
                                <td style="width:60%">
                                    {f'<span className=""><b>Shipped By</b>: ( if undelivered, return to )</span><br />' if settings.pickup_address else ""}
                                    {f'<span>{client_name}</span><br />' if settings.company_name else ""} 
                                    {f'<span>{pickup_display}</span><br /><span>{order.pickup_location.pincode}, {order.pickup_location.city}, {order.pickup_location.state}</span><br />' if settings.pickup_address else ""}
                                    
                                    {f'<span style="margin: 5px 0;"><b>Mobile Number</b> - {order.pickup_location.contact_person_phone}</span><span style="margin: 5px 0;"><br /><b>Email</b> - {order.pickup_location.contact_person_email}</span>' if (settings.pickup_address and client_id != 402 and client_id != 424) else ""}
                                    {gst}
                                </td>  
                                <td style="width:40%; text-align:center;">
                                    <span>
                                        <b>Order ID</b>: {truncate_text(order.order_id, 15)}
                                    </span><br />
                                    
                                    {f'<span><img style="width:400px; margin: 10px; height: 100px;" src="data:image/png;base64,{order_id_barcode}" alt="Order id Barcode" /></span><br />' if settings.order_id_barcode_enabled else ""}
                                    <br />
                                    {f'<span style="font-weight:bold; font-size:13px; margin-bottom:5px">{order.payment_mode.upper()}</span><br />' if settings.payment_type else ""}
                                    {f'<span style="font-weight:bold; font-size:13px; padding:5px">Rs {order.total_amount:.2f}</span>' if ((order.payment_mode == 'prepaid' and settings.prepaid_amount) or (order.payment_mode == 'COD' and settings.COD_amount)) else ""}
                                    
                                    <br />
                                    <span style="font-size: 8px">* All prices are including GST</span>
                                </td>
                            </tr>
                        </table>
                    
                    <div style="padding-top:10px; padding-bottom:10px;">
                       {f'<table class="header" style="font-size:10px; border-bottom: 1px solid black;"><tr style="border:0; padding-bottom:1px;"><td style="width:{product_col_width}%"><b>{main_column_header}</b></td>{sku_header_column}<td style="width:{qty_col_width}%; text-align:center"><b>Qty</b></td><td style="width:{amount_col_width}%; text-align:center"><b>Amt</b></td></tr>{product_rows}</table>' if client_id!=310 else "" }

                    </div>     
                    
                    {f'<br /><br /><br /><br /><br />' if client_id == 310 else ""}
                    
                
                    
                    
                <table class=" table" style="padding:3px; margin-top:auto; border-bottom: 1px solid black; font-size:9px">
                    {message}
                    
                    {branding}
                
                    </table>     
                    <table class="table">
                
                    </table
                </td>    
                </tr>
                </table>
            </body>
            </html>
            """

        return html_content

    except Exception as e:
        logger.error(msg=f"Could not create thermal shipping label: {str(e)}")
        return None
