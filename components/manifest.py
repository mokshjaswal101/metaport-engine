import io
import base64
import os
from typing import List
from io import BytesIO
from xhtml2pdf import pisa
from datetime import date

from logger import logger

import barcode
from barcode.writer import ImageWriter


# schema
from modules.orders.order_schema import Order_Model


def generate_barcode_image(data):
    """Generate barcode image from data and return as BytesIO buffer"""
    buffer = io.BytesIO()
    barcode_class = barcode.get_barcode_class("code128")

    writer_options = {
        "module_width": 0.3,
        "module_height": 12,
        "quiet_zone": 0,
        "font_size": 1,
        "text_distance": 6,
        "dpi": 300,
        "write_text": False,
    }

    my_barcode = barcode_class(data, writer=ImageWriter())
    my_barcode.write(buffer, options=writer_options)
    buffer.seek(0)
    return buffer


def convert_image_to_base64(image_buffer):
    """Convert image buffer to base64 string"""
    return base64.b64encode(image_buffer.getvalue()).decode("utf-8")


def generate_manifest(orders: List[Order_Model]) -> BytesIO:
    pdf_buffer = BytesIO()

    order_rows = ""
    order_count = 1

    for order in orders:

        # generate the barcode for the awb number
        barcode_base64 = convert_image_to_base64(
            generate_barcode_image(order.awb_number)
        )

        # handling products
        product_rows = ""
        total_quantity = 0

        for product in order.products:
            total_quantity += product["quantity"]

            name = product["name"].replace("-", " ")
            sku = product["sku_code"]

            product_rows += f"""
                <div class="break">{name}(QTY - {product["quantity"]})</div>
            """

        # Define the HTML template for the PDF
        order_rows += f"""
            <tr>
                <td>{order_count}</td>
                <td>{order.order_id}</td>
                <td>{order.awb_number}<br />{order.courier_partner}</td>
                <td>{product_rows}</td>
                <td>Rs {round(order.total_amount,2)}</td>
                <td class="barcode"><img src="data:image/png;base64,{barcode_base64}" width="200" /></td>
            </tr>
        """

        order_count += 1

    html_content = f"""
            <html>
            <head>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        margin: 0;
                        padding: 0;
                        width: 100%;
                        font-size: 10px;
                    }}
                    .container {{
                        padding: 10px;
                        width: 800px;
                        margin: 0 auto;
                    }}
                                        
                    .header, .footer {{
                        text-align: center;
                        font-size: 12px;
                        font-weight: bold;
                    }}                  
                    
                    table {{
                        width: 100%;
                        border-collapse: collapse;
                    }}
                    
                    th, td {{
                        padding: 5px 3px;
                        border: 1px solid black;
                        text-align: left;
                    }}
                    
                    .barcode {{
                        text-align: center;
                    }}
                    
                   .break {{
                        word-wrap: break-word;
                        overflow-wrap: break-word;
                        white-space: normal;
                        max-width: 100px; /* Set a max width to limit overflow */
                        display: inline-block;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        Order Manifest ({date.today().strftime("%d-%m-%Y")})<br />
                    </div>
                    <table>
                        <tr>
                            <th style="width:5%">S.No</th>
                            <th style="width:20%">Order Id</th>
                            <th style="width:20%">AWB Number</th>
                            <th style="width:30%">Content</th>
                            <th style="width:12%">Amount</th>
                            <th style="width:40%">Barcode</th>
                        </tr>
                        {order_rows}
                    </table>
                    <div class="footer" style="margin-top:30px">
                        Powered by Last Miles @ Warehousity
                    </div>
                </div>
            </body>
            </html>
            """

    return html_content
