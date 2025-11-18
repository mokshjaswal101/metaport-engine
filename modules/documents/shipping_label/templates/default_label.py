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


# Use shared `shorten_order_id` from common utils for display trimming


def generate_default_label(
    order: Order_Model,
    settings: LabelSettingResponseModel,
    client_name: str,
    client_id: int,
) -> str:
    """Generate default shipping label HTML"""
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

        ## handling products (settings-driven)
        product_rows = ""

        # Determine column display purely from settings
        show_sku_in_product = settings.SKU
        show_product_name = settings.product_name

        # Dynamic width calculation (percent strings for default layout)
        # Now we only have 3 columns: Product (with SKU merged), Quantity, Amount
        product_col_width = "70%"
        qty_col_width = "15%"
        amount_col_width = "15%"
        product_name_length = 70
        product_sku_length = 50

        # Build product rows (limit to show up to 8 rows, pad with empty rows)
        for product in order.products:
            # Build main column content with product name and SKU merged
            main_content = ""

            if show_product_name:
                main_content = sanitize_product_name(
                    product["name"], product_name_length
                )

            # Add SKU below product name if:
            # 1. SKU filter is enabled OR
            # 2. SKU is present (even if filter is disabled)
            product_sku = product.get("sku_code", "")
            if (show_sku_in_product) and product_sku:
                sku_display = truncate_text(product_sku, product_sku_length)
                if main_content:
                    main_content += f"<br/><span style='font-size:13px'><b>SKU</b> - {sku_display}</span>"
                else:
                    main_content = sku_display

            # If no product name and no SKU, show empty content
            if not main_content:
                main_content = "&nbsp;"

            amount = float(product.get("quantity", 0)) * float(
                product.get("unit_price", 0) or 0
            )

            product_rows += f"""
                <tr style="border:0; font-size:15px">
                    <td style="width:{product_col_width}; padding-top: 1px; padding-bottom:2px">{main_content}</td>
                    <td style="width:{qty_col_width}; padding-top: 1px; padding-bottom:2px; text-align:center">{product.get('quantity', '')}</td>
                    <td style="width:{amount_col_width}; padding-top: 1px; padding-bottom:2px; text-align:center">{amount:.2f}</td>
                </tr>
                """

        num_products = len(order.products)
        empty_rows_needed = max(0, 8 - num_products)

        # Add empty rows if needed (now only 3 columns)
        for _ in range(empty_rows_needed):
            main_td = f'<td style="width:{product_col_width}; padding-top: 4px; padding-bottom:4px">&nbsp;</td>'
            product_rows += f"""
                <tr style=" font-color:white;border:0;">
                    {main_td}
                    <td style="width:{qty_col_width}; padding-top: 4px; padding-bottom:4px">&nbsp;</td>
                    <td style="width:{amount_col_width}; padding-top: 4px; padding-bottom:4px">&nbsp;</td>
                </tr>
                """

        # Get Booking date for the label
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

        # Format weight for display: round to 2 decimals and strip trailing zeros
        weight_display = ""
        try:
            if getattr(order, "weight", None) is not None:
                w = float(order.weight)
                if w.is_integer():
                    weight_display = str(int(w))
                else:
                    weight_display = ("{:.3f}".format(w)).rstrip("0").rstrip(".")
        except Exception:
            # Fallback to raw value if formatting fails
            weight_display = order.weight

        # Generate message if enabled
        message = (
            f"All disputes will be resolved under {order.pickup_location.state} jurisdiction. "
            "Sold goods are eligible for return or exchange according to the store's policy."
            if settings.message
            else ""
        )

        # Generate branding if enabled
        branding = (
            f"Powered by <br /> <b>Last Miles @ Warehousity</b>"
            if settings.branding
            else ""
        )

        # TODO -> DYNAMICALLY FETCH THE GST NUMBER
        gst = ""
        if client_id == 186:
            gst = f"""<br /><span style="margin: 5px 0;"> GST - 03AABFF3773C3ZW  </span>"""

        # Combine and truncate consignee and pickup addresses for display
        consignee_combined = f"{order.consignee_address}{' ' + order.consignee_landmark if getattr(order, 'consignee_landmark', None) else ''}"
        consignee_display = consignee_combined

        pickup_combined = f"{order.pickup_location.address}{' ' + getattr(order.pickup_location, 'landmark', '') if getattr(order.pickup_location, 'landmark', None) else ''}"
        pickup_display = pickup_combined

        # Build HTML template

        # Defining the HTML for the label structure
        html_content = f"""
                    <html>
                    <head>
                        <style>
                            @page {{
                                margin: 25px;
                                border: 1px solid black;
                                
                            }}
                            body {{
                                font-family: Arial, sans-serif;
                                font-weight:100;
                                margin: 0;
                                padding: 0;
                                width: 100%;
                                font-size: 13px;
                            }}                   
                            
                            table {{
                                width: 100%;
                                border-collapse: collapse;
                                border: 0;
                            }}
                            td, th {{
                                padding: 10px;
                                text-align: left;
                            }}
                            .logo {{
                                height:150px;
                                width:auto;
                                
                            }}
                        </style>
                    </head>
                    <body>
                    <div>
                        <table style="border-bottom: 1px solid black">
                            <tr>                                        
                                <td style="width:70%">
                                    <span style="font-weight:bold">Ship To :</span><br />
                                    <span>{order.consignee_full_name}</span><br />
                                    <span>{consignee_display}</span><br />
                                    <span>{order.consignee_pincode}, {order.consignee_city}, {order.consignee_state}</span><br /> 
                                    {f'<span><b>Mobile Number</b> - {order.consignee_phone}</span>' if settings.consignee_phone else ""}
                                </td>  
                                <td style="width:30%">   
                                    {f'<img style="width:100px;" src="{base64_logo}" alt="Logo"/>' if settings.logo_shown  else "" }             
                                </td>    
                            </tr>
                        </table>
                        <table style="border-bottom: 1px solid black">
                            <tr>
                                <td style="width:70%;">  
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
                                    <br />
                                    {
                                        f'<span><b>Cluster Code</b> - {cluster_code}</span>' if cluster_code else ""
                                    }
                                </td>
                                
                                
                                <td style="width:30%;">
                                    
                                    {f'<span><b>Dimensions</b>: {int(order.length) if order.length == int(order.length) else order.length} x {int(order.breadth) if order.breadth == int(order.breadth) else order.breadth} x {int(order.height) if order.height == int(order.height) else order.height} cm </span>  <br /> ' if settings.package_dimensions else "" }
                                                                
                                    {f'<span><b>Weight</b>: {weight_display} kg </span>  <br />' if settings.weight else ''}
                                    {f'<span><b>Date</b>: {order_date if settings.order_date else ""}</span>  <br />' if settings.order_date else ''}
                                    
                                </td>                                        
                            </tr>
                        </table>                            
                    
                        
                        <table style="border-bottom: 1px solid black">
                            <tr>                                        
                                <td style="width:60%">
                                    {f'<span className=""><b>Shipped By</b>: ( if undelivered, return to )</span><br />' if settings.pickup_address else ""}
                                    {f'<span>{client_name}</span><br />' if settings.company_name else ""} 
                                    {f'<span>{pickup_display}</span><br /><span>{order.pickup_location.pincode}, {order.pickup_location.city}, {order.pickup_location.state}</span><br /><span style="margin: 5px 0;"><b>Mobile Number</b> - {order.pickup_location.contact_person_phone}</span><br /><span style="margin: 5px 0;"><b>Email</b> - {order.pickup_location.contact_person_email}</span>' if settings.pickup_address else ""}
                                    {gst}
                                </td>  
                                <td style="width:40%; text-align:center; padding-top: 20px; margin-top: 10px;">
                                    <span>
                                        <b>Order ID</b>: {truncate_text(order.order_id, 15)}
                                    </span><br />
                                    
                                    {f'<span><img style="width:400px; margin: 10px; height: 100px;" src="data:image/png;base64,{order_id_barcode}" alt="Order id Barcode" /></span><br />' if settings.order_id_barcode_enabled else ""}
                                    <br />
                                    {f'<span style="font-weight:bold; font-size:20px; margin-bottom:5px">{order.payment_mode.upper()}</span><br />' if settings.payment_type else ""}
                                    {f'<span style="font-weight:bold; font-size:20px; padding:5px">Rs {order.total_amount:.2f}</span>' if ((order.payment_mode == 'prepaid' and settings.prepaid_amount) or (order.payment_mode == 'COD' and settings.COD_amount)) else ""}
                                </td>
                            </tr>
                        </table>
                        
                        
                        </div>
                        <div>
                            <table style="border-bottom: 1px solid black;">
                                <tr style="border:0">
                                    {'<td style="width:70%; font-size:15px;"><b>Product</b></td>' if settings.product_name else '<td style="width:70%; font-size:15px;"><b>Item</b></td>'}
                                    <td style="width:15%; font-size:15px; text-align:center;"><b>Quantity</b></td>
                                    <td style="width:15%; font-size:15px; text-align:center;"><b>Amount</b></td>
                                </tr>                                    
                                {product_rows}
                                
                                <tr style=" font-color:white;border:0;">
                                    <td style="width:{product_col_width}; padding-top: 4px; padding-bottom:4px">&nbsp;</td>
                                    <td style="width:{qty_col_width}; padding-top: 4px; padding-bottom:4px">&nbsp;</td>
                                    <td style="width:{amount_col_width}; padding-top: 4px; padding-bottom:4px">&nbsp;</td>
                                </tr>
                            </table>
                        </div>     


                        

                        
                        
                        <table style="margin-top:auto; border-bottom: 1px solid black;">
                            <tr>
                                <td style="width:70%;">
                                {message}
                                </td>
                                <td style="width:30%;">{branding}</td>
                            </tr>
                        </table>                    
                    </div>                            
                    </body>
                    </html>
                    """

        return html_content

    except Exception as e:
        logger.error(msg=f"Could not create default shipping label: {str(e)}")
        return None
