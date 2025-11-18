import io
import base64
from datetime import datetime
from pytz import timezone
import os
from fastapi.encoders import jsonable_encoder
from decimal import Decimal

from logger import logger

import barcode
from barcode.writer import ImageWriter

# schema
from modules.orders.order_schema import Order_Model
from modules.documents.shipping_label.shipping_label_schema import (
    LabelSettingResponseModel,
)


def convert_image_to_base64(image_buffer):
    # Encode the image to base64
    return base64.b64encode(image_buffer.getvalue()).decode("utf-8")


def get_img_as_base64(partner):

    file_path = ""

    if (
        partner == "bluedart"
        or partner == "bluedart-air"
        or partner == "bluedart 1kg"
        or partner == "bluedart 2kg"
    ):
        file_path = os.getcwd() + "/courier_logo/bluedart.png"

    elif (
        partner == "delhivery"
        or partner == "delhivery-air"
        or partner == "delhivery 1kg"
        or partner == "delhivery 2kg"
        or partner == "delhivery 3kg"
        or partner == "delhivery 5kg"
        or partner == "delhivery 10kg"
        or partner == "delhivery 15kg"
        or partner == "delhivery 20kg"
    ):
        file_path = os.getcwd() + "/courier_logo/delhivery_logo.png"

    elif partner == "ekart":
        file_path = os.getcwd() + "/courier_logo/ekart.jpg"

    elif (
        partner == "ecom-express"
        or partner == "ecom-express 1kg"
        or partner == "ecom-express 2kg"
        or partner == "ecom-express 5kg"
        or partner == "ecom-express 10kg"
    ):
        file_path = os.getcwd() + "/courier_logo/ecom.png"

    elif (
        partner == "xpressbees"
        or partner == "xpressbees 1kg"
        or partner == "xpressbees 2kg"
        or partner == "xpressbees 5kg"
        or partner == "xpressbees 10kg"
        or partner == "xpressbees 15kg"
        or partner == "xpressbees 20kg"
    ):
        file_path = os.getcwd() + "/courier_logo/xpressbees.png"

    elif (
        partner == "dtdc"
        or partner == "dtdc-air"
        or partner == "dtdc 5kg"
        or partner == "dtdc 1kg"
        or partner == "dtdc 3kg"
    ):
        file_path = os.getcwd() + "/courier_logo/dtdc.png"

    elif partner == "shadowfax":
        file_path = os.getcwd() + "/courier_logo/shadowfax.png"

    elif (
        partner == "amazon"
        or partner == "amazon 1kg"
        or partner == "amazon 2kg"
        or partner == "amazon 5kg"
        or partner == "amazon 10kg"
        or partner == "amazon 15kg"
        or partner == "amazon 20kg"
    ):
        file_path = os.getcwd() + "/courier_logo/amazon.png"

    with open(file_path, "rb") as f:
        base64_code = base64.b64encode(f.read())
        return base64_code.decode("utf-8")


def default_invoice(order: Order_Model, client_name, client_id):

    try:
        print("welcome to customize label")
        # generate the barcode for the awb number

        print(client_id, "**client_id**")

        logo = ""

        ist = timezone("Asia/Kolkata")

        order_date = (
            order.booking_date.astimezone(ist).strftime("%Y-%m-%d, %H:%M:%S")
            if order.booking_date
            else order.order_date.astimezone(ist).strftime("%Y-%m-%d, %H:%M:%S")
        )
        print("yes i am correct")

        image = (
            "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAHcAAABPCAYAAADREFpKAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAAEnQAABJ0Ad5mH3gAABIxSURBVHhe7dp1qHbF9gdw/xK7u7u7u7tbsbvBbkXFbkGxFbu7EOzu7u7ubvePz8D3Yd5z3/t67jnX34XNXjDM7Ik1q2fNPM9wTQethU65LYZOuS2GTrkthk65LYZOuS2GTrkthk65LYZOuS2GTrkthk65LYZOuS2GTrkthk65LYZOuS2GTrkthk65LYZOuS2GTrkthk65LYZOuS2GTrkthr9V7l9//VXKn3/+2WsrIH1//PHHEP2gbg8Lsj5tBd76O5B+8Pvvv/e+a9pA2vV4QDvz6xpkHaj7Uqdk3dC+a0g/qMfTNt4XVwowNhjol3KjPHVfgvOtzryMpT0syLyU2lBqxuu5wBgFp68vHQrQZ16gngeyvm5nTt++33777V/Wg3pO2krG+taRY75rQ/VdtwcD/VJuvTkIE/qjjPSBjNVC/XeQuep6PUEC+H/99dfSzlyQNdk/a9LOOOhLW70m3xkLn5kDUoOh0aqv3s/a4Knr7BPIWMB3jWew0K8z12YEh7gwnQL0IQogMP2phwXBEwHVfXClpD916KgFkr3TruGXX37p0V/PyzeocaQ/pfbaFPvWfdo1ntCY74zXY8GhhI/0qwcD/T5z+xbE/fDDD83777/fvPvuu83HH39cBJixmplhQeYpwf355583b731VvPOO+80n332WWEUhGFzv/zyy7L3119/PcR6tf0Dwakvng3g+uSTTwrdP/74Y+nLXGM1PX3bUUxd9Kf0HQtuY4G6PyVyy9zMGSj0S7mEguEICHzzzTfN5Zdf3my66abNqquu2my33XbN/fff3/z8889lPIroLwT/p59+2uy///7Nkksu2aywwgrNQQcd1Dz55JNlHGCaQo866qhmww03bI4//viyBtTKU6fUglLD9dJLLzW77rprs+222zbXXHNNb//Mi6fqV4d330PD6Ruk39yffvqpOMBHH33UvP76680bb7zRfPHFF8UJHDVwwmeudtbX7cFAvz0X2FT56quvmuOOO66ZaKKJmnHGGacZa6yxmnHHHbeZc845m+uvv77MQVh/iYPfGkraZ599mvHHH7/gDO6ll166eeWVV4oQCIfyJ5hggjKmUDSa4Ai9EXJoIMQo5uWXX27WW2+9Zowxxijrp5566uboo49uvv32296cGofocemllxYDPvbYY5vXXnutN6ZY41vBwyOPPNKccsopzdZbb90su+yyzTzzzNPMPffcpV5kkUWatdZaqznkkEOa22+/vfnwww+Lout98akeLPytchFcb0yAd911VyF07LHHbiaccMJmkkkmKcX3VlttVawVZM3QIP1qe7DmW2+9tZliiima8cYbrygYTm2KPPTQQ0u0ePXVV5tZZpml188AFl544eaxxx7r0YrG4E4xpia4M844Y4h90L3KKquUMG1ebSTwXXLJJc2ss85a5k888cTNzjvvXLzSuLnmoP/ZZ59tDjzwwGaBBRYoclEYPQPKPmOOOWbBYwwfDOa+++4rR0OUiobQPxjol+dGMABTmEM04WNWQTxBU/oTTzzRE1AMA9R1cIYRoQujmKc0OAkgQpljjjlKaHMWzzvvvGUvc2JYPA8QUHDWdfYmROHceuvgV7bffvtyjsc4zFccM1tssUWha7LJJiuKmX322Yv3w2u+PR988MFmmWWWaUYcccSebBgQHkSI0UcfvSg5eIzhc4QRRmjmm2++5sYbbyy44OxL/0ChX8rFrI3Uzz33XBE0wghFLTwrUQrr/f7778v84Mj6QPrUmLrnnntK6IJDIZxYuG8C4nGMYPfddy9j2Zsw11577aK4ml5tBUQJzz//fDPXXHOVNXDDM/300xfhUqR1KYAxb7755kUxk08+eVnn+HnxxRcLTuXpp58uijUnRmkeBTpSttlmm2bfffdtdtttt2b11Vcv+0V2k046aVE4nLfddluhAf3whvaBQr+UayMbSmROPvnknrUTbAQUBWsLS/W5VAu6Jjj98DMIzMKHYcrlsYSlnwDWXXfd5u233y7HwnTTTVcEiA71DDPMUMJbPDcF7uz13XffNaeddlrZI8VaCaEzu14XOqNctNTKpVC4ZfQiQZQV5c4///zNqaeeWoxJRi5XcHa/+eabzWWXXVbOYrjMJTeGwRDMJ+sY6GCg32EZCIksDyPCGqIoYqaZZio1YgmBgs8555wizBAaYfVVLhAOZcfWExDcQhch+rYfZbN457IzXSZtXpRk3l577VXOZXtkz1pAH3zwQVEkGq2hCCUhvV6XwpO23HLLIZQrcj3++OOFjgsuuKAZ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"
            if client_id == 79
            else ""
        )

        gst = "27BAWPG3149K1Z5" if client_id == 79 else ""

        tax_line = (
            ""
            if client_id != 79
            else f""" <div style="text-align:right; padding: 10px">
                <span style="padding-right:10px">Tax ( Included ): &nbsp;</span>
                <span style="padding-right:10px; ; width:30px">{round(float(order.order_value) * 0.05, 2)}</span>
                </div> """
        )

        product_rows = ""

        for product in order.products:

            product_name_length = 100

            product_name = (
                product["name"][:product_name_length] + "..."
                if len(product["name"]) > product_name_length
                else product["name"]
            )
            product_quantity = float(product["quantity"])
            product_unit_price = float(product["unit_price"])
            product_total = product_quantity * product_unit_price

            product_rows += f"""
            <tr style="border:0; font-size: 9px; padding-bottom:1px; padding-top:1px">
                <td>{product_name}</td>
                <td>{product['sku_code'] if product['sku_code'] else '-'}</td>
                <td>{product_quantity}</td>
                <td>{product_total:.2f}</td>
            </tr>
            """

        # Defining the HTML for the label structure
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
                }}
                .table td, .table th {{
                    padding: 5px;
                }}                
                
            </style>
        </head>
        <body>
            <table style="border-top: 1px solid black; border-left: 1px solid black; border-right: 1px solid black;">
            <tr>
            <td style="">            
                <div>
                    <table class="header" style="border-bottom: 1px solid black;" >
                     <tr>
                            <td style="width:49%;  margin:0">
                                <span style="width:70%;">
                                    <img  style="width:180px; margin: 10px;  height: 100px;" src={image} alt="logo" />
                                </span>
                            </td>
                            <td style="width:49%; margin:0;text-align:right">
                                {f'<span><b>TAX INVOICE</b></span> <br />'}
                                {f'<span><b>Invoice No</b>: LM/{str(client_id)}/{order.order_id} </span>  <br />'}
                                {f'<span><b>Invoice Date</b> {order.booking_date.strftime("%d %B %Y, %H:%M:%S")} </span>  <br />'}
                                {f'<span><b>Order No</b>: {order.order_id} </span>  <br />'}
                            </td>
                            
                        </tr>
                    </table>
                </div>
                <div>
                    <table class="header" style="border-bottom: 1px solid black;" >
                     <tr>
                            <td style="width:50%; margin:0;text-align:left;clear:both">
                                {f'<b style="font-size:10px">STORE</b> <br />'}
                                {f'<span><b>{client_name}</b></span> <br />'}
                                {f'<span>{order.pickup_location.address}</span> <br />'}
                                {f'<span><b style="font-size:9px">Email:    </b>{order.pickup_location.contact_person_email}</span>  <br />'}
                                {f'<span><b style="font-size:9px">GSTIN</b>: {gst} </span>  <br />'}
                            </td>
                            <td style="width:50%; margin:0;text-align:right;clear:both">
                                {f'<b style="font-size:10px">BILL TO</b> <br />'}
                                {f'<span><b>{order.consignee_full_name}</b></span> <br />'}
                                {f'<span>{order.consignee_address}</span>  <br />'}
                                {f'<span><b style="font-size:9px">Email</b>:{order.consignee_email} </span>  <br />'}
                                {f'<span>{order.consignee_phone}</span>  <br />'}
                            </td>                            
                        </tr>
                    </table>
                </div>

                <div>
                     <table class="header" style="width:100%; font-size:10px; border-bottom: 1px solid black;">
                        <tr style="border:0; padding-bottom:1px;">
                            <td style="width:50%"><b>Product</b></td>
                            <td style="width:20%"><b>SKU</b></td>
                            <td style="width:15%"><b>Qty</b></td>
                            <td style="width:15%"><b>Amount</b></td>
                        </tr>                                    
                        {product_rows}
                    </table>
                    {tax_line}
                    
                <div style="text-align:right; padding: 10px">
                <span style="margin-right:10px">Shipping charge: &nbsp;</span>
                <span style="margin-right:10px; width:30px">{order.shipping_charges or 0}</span>
                </div>
                <table style="border-collapse: collapse; width: 30%; margin-left: auto; margin-top: 10px; table-layout: fixed; border: 0px solid black;">
                    <tbody>
                        <tr>
                            <td style="width: 80%; text-align: right; border: 0px solid black;">
                             <br /><br /><br />
                                <div style="border-top: 1px solid black; border-bottom: 1px solid black; padding: 5px 0;">
                                    <b>Grand Total:</b>
                                </div>
                            </td>
                            <td style="width: 20%; text-align: center; border: 0px solid black;">
                               <br /><br /><br />
                                <div style="border-top: 1px solid black; border-bottom: 1px solid black; padding: 5px 0;">
                                    <b>Rs {order.total_amount}</b>
                                </div>
                            </td>
                        </tr>
                    </tbody>
                </table>
                </div>
            </td>    
            </tr>
            </table>
        </body>
        </html>
        """

        return html_content

    except Exception as e:

        logger.error(
            msg="could not create shipping label : {}".format(str(e)),
        )

        return None


def order_invoice(order: Order_Model, client_name, client_id):
    try:

        logo = ""
        print("I am order Invoice")
        ist = timezone("Asia/Kolkata")
        order_date = (
            order.booking_date.astimezone(ist).strftime("%Y-%m-%d, %H:%M:%S")
            if order.booking_date
            else order.order_date.astimezone(ist).strftime("%Y-%m-%d, %H:%M:%S")
        )
        image = (
            "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAHcAAABPCAYAAADREFpKAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAAEnQAABJ0Ad5mH3gAABIxSURBVHhe7dp1qHbF9gdw/xK7u7u7u7tbsbvBbkXFbkGxFbu7EOzu7u7ubvePz8D3Yd5z3/t67jnX34XNXjDM7Ik1q2fNPM9wTQethU65LYZOuS2GTrkthk65LYZOuS2GTrkthk65LYZOuS2GTrkthk65LYZOuS2GTrkthk65LYZOuS2GTrkthk65LYZOuS2GTrkthk65LYZOuS2GTrkthr9V7l9//VXKn3/+2WsrIH1//PHHEP2gbg8Lsj5tBd76O5B+8Pvvv/e+a9pA2vV4QDvz6xpkHaj7Uqdk3dC+a0g/qMfTNt4XVwowNhjol3KjPHVfgvOtzryMpT0syLyU2lBqxuu5wBgFp68vHQrQZ16gngeyvm5nTt++33777V/Wg3pO2krG+taRY75rQ/VdtwcD/VJuvTkIE/qjjPSBjNVC/XeQuep6PUEC+H/99dfSzlyQNdk/a9LOOOhLW70m3xkLn5kDUoOh0aqv3s/a4Knr7BPIWMB3jWew0K8z12YEh7gwnQL0IQogMP2phwXBEwHVfXClpD916KgFkr3TruGXX37p0V/PyzeocaQ/pfbaFPvWfdo1ntCY74zXY8GhhI/0qwcD/T5z+xbE/fDDD83777/fvPvuu83HH39cBJixmplhQeYpwf355583b731VvPOO+80n332WWEUhGFzv/zyy7L3119/PcR6tf0Dwakvng3g+uSTTwrdP/74Y+nLXGM1PX3bUUxd9Kf0HQtuY4G6PyVyy9zMGSj0S7mEguEICHzzzTfN5Zdf3my66abNqquu2my33XbN/fff3/z8889lPIroLwT/p59+2uy///7Nkksu2aywwgrNQQcd1Dz55JNlHGCaQo866qhmww03bI4//viyBtTKU6fUglLD9dJLLzW77rprs+222zbXXHNNb//Mi6fqV4d330PD6Ruk39yffvqpOMBHH33UvP76680bb7zRfPHFF8UJHDVwwmeudtbX7cFAvz0X2FT56quvmuOOO66ZaKKJmnHGGacZa6yxmnHHHbeZc845m+uvv77MQVh/iYPfGkraZ599mvHHH7/gDO6ll166eeWVV4oQCIfyJ5hggjKmUDSa4Ai9EXJoIMQo5uWXX27WW2+9Zowxxijrp5566uboo49uvv32296cGofocemllxYDPvbYY5vXXnutN6ZY41vBwyOPPNKccsopzdZbb90su+yyzTzzzNPMPffcpV5kkUWatdZaqznkkEOa22+/vfnwww+Lout98akeLPytchFcb0yAd911VyF07LHHbiaccMJmkkkmKcX3VlttVawVZM3QIP1qe7DmW2+9tZliiima8cYbrygYTm2KPPTQQ0u0ePXVV5tZZpml188AFl544eaxxx7r0YrG4E4xpia4M844Y4h90L3KKquUMG1ebSTwXXLJJc2ss85a5k888cTNzjvvXLzSuLnmoP/ZZ59tDjzwwGaBBRYoclEYPQPKPmOOOWbBYwwfDOa+++4rR0OUiobQPxjol+dGMABTmEM04WNWQTxBU/oTTzzRE1AMA9R1cIYRoQujmKc0OAkgQpljjjlKaHMWzzvvvGUvc2JYPA8QUHDWdfYmROHceuvgV7bffvtyjsc4zFccM1tssUWha7LJJiuKmX322Yv3w2u+PR988MFmmWWWaUYcccSebBgQHkSI0UcfvSg5eIzhc4QRRmjmm2++5sYbbyy44OxL/0ChX8rFrI3Uzz33XBE0wghFLTwrUQrr/f7778v84Mj6QPrUmLrnnntK6IJDIZxYuG8C4nGMYPfddy9j2Zsw11577aK4ml5tBUQJzz//fDPXXHOVNXDDM/300xfhUqR1KYAxb7755kUxk08+eVnn+HnxxRcLTuXpp58uijUnRmkeBTpSttlmm2bfffdtdtttt2b11Vcv+0V2k046aVE4nLfddluhAf3whvaBQr+UayMbSmROPvnknrUTbAQUBWsLS/W5VAu6Jjj98DMIzMKHYcrlsYSlnwDWXXfd5u233y7HwnTTTVcEiA71DDPMUMJbPDcF7uz13XffNaeddlrZI8VaCaEzu14XOqNctNTKpVC4ZfQiQZQV5c4///zNqaeeWoxJRi5XcHa/+eabzWWXXVbOYrjMJTeGwRDMJ+sY6GCg32EZCIksDyPCGqIoYqaZZio1YgmBgs8555wizBAaYfVVLhAOZcfWExDcQhch+rYfZbN457IzXSZtXpRk3l577VXOZXtkz1pAH3zwQVEkGq2hCCUhvV6XwpO23HLLIZQrcj3++OOFjgsuuKAZ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"
            if client_id == 79
            else ""
        )

        gst = "27BAWPG3149K1Z5" if client_id == 79 else ""

        tax_line = (
            ""
            if client_id != 79
            else f""" <div style="text-align:right; padding: 10px">
                <span style="padding-right:10px">Tax ( Included ): &nbsp;</span>
                <span style="padding-right:10px; ; width:30px">{round(float(order.order_value) * 0.05, 2)}</span>
                </div> """
        )

        product_rows = ""
        sub_total = 0
        sub_total1 = []

        for product in order.products:

            product_name_length = 100

            product_name = (
                product["name"][:product_name_length] + "..."
                if len(product["name"]) > product_name_length
                else product["name"]
            )
            product_quantity = float(product["quantity"])
            product_unit_price = float(product["unit_price"])
            product_total = product_quantity * product_unit_price
            tax_rate = Decimal("0.05")  # 5% PERCENTAGE FOR STATIC TAX
            product_total = Decimal(str(product_total))
            product_total_with_tax_percent = product_total * tax_rate
            final_price = product_total + product_total_with_tax_percent
            sub_total = final_price
            sub_total1.append(final_price)
            product_rows += f"""
            <tr style="border:0; font-size: 12px; padding-bottom:1px; padding-top:1px">
                <td style="width:200px"><br />{product_name}</td>
                <td><br />{product['sku_code'] if product['sku_code'] else '-'}</td>
                <td><br />61091000</td>
                <td><br />{product_quantity}</td>
                <td><br />{product_total:.2f}</td>
               
                <td><br />5%</td>
                <td> <br />{product_total_with_tax_percent:.2f}</td>
                <td> <br />{final_price:.2f}</td>
            </tr>
            """
        sub_total1 = sum(sub_total1)
        grand_total = (order.shipping_charges or Decimal("0")) + Decimal(
            str(sub_total1)
        )

        # print(jsonable_encoder(sub_total1), "<<sub_total1>>")
        # sub_total1 = sum(sub_total1)

        html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <title>Invoice</title>
                <style>
                    body {{ font-family: Arial, sans-serif; padding: 20px; }}
                    table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
                    th, td {{  padding: 8px; text-align: left; vertical-align: top; }}
                    .no-border td {{ border: none; }}
                    .right-align {{ text-align: right; }}
                    .bold {{ font-weight: bold; }}
                </style>
            </head>
            <body>

                <!-- Logo and Header -->
                <table class="no-border" style="width: 100%;">
                    <tr>
                        <!-- Left: Logo -->
                        <td style="width: 100px; text-align: left;">
                           
                        </td>
                        
                        <!-- Right: Text -->
                        <td style="text-align: right; width: 100%;">
                            <table style="width: 100%;">
                                <tr>
                                    <td style="width: 60%; border:0px solid red"></td> <!-- Empty space on the left -->
                                    <td style="width: 40%; border:0px solid red;text-align: right;">
                                        <strong style="font-size:20px;">Invoice</strong> <br />
                                        
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>
                
                <hr style="padding: 0px 5px 0px 0px"></hr>

                <!-- Company & Order Info -->
                <table style="border:0px solid red !important; font-size: 13px">
                    <tr>
                        <td style="border:0px solid red !important; ">
                        <table style="width: 100%;  color: #000; font-weight: bold;border:0px solid red !important">
                            <tr>
                                <td style="padding:0px 4px;text-align: left;font-size:16px">FASHION SPORTS(Shopaustin.in)</td>
                                
                            </tr>
                        </table>                    
                       E1 2172 Jasdeep complex Rk road Near bansal dharam kanda141003 <br /> 
                       Ludhiana, Punjab
                       9888017670 <br />
                        <strong>Phone </strong>: 9888017670<br />
                        <strong>Email </strong>:  fashionsportsonline@gmail.com<br />
                        <strong>Website </strong>: shopaustin.in<br />
                        <strong>GST </strong>: 03AABFF3773C3ZW
                        </td>
                        <td style="border:0px solid red !important">
                            <table style="width: 100%;  color: #000; font-weight: bold;border:0px solid red !important">
                                <tr>
                                     <td style="padding:0px 4px;text-align: left;font-size:16px">Order #{order.order_id}</td>
                                </tr>
                            </table>   
                            <strong>Invoice Number:</strong> LM/{str(client_id)}/{order.order_id}<br />
                            <strong>Invoice Date:</strong> {order.booking_date.strftime("%d %B %Y, %H:%M:%S")}<br />
                            <strong>Status:</strong> {order.status}<br />
                            <strong>Generated Date:</strong> {order.booking_date.strftime("%d %B %Y, %H:%M:%S")}<br />
                            <strong>Payment Method:</strong> {order.payment_mode}<br />
                        </td>
                    </tr>
                </table>

                <!-- Billing & Shipping Info -->
                <table style="border:0px solid red !important;  font-size: 13px">
                    <tr>
                        <td>
                              <table style="width: 100%;  color: #000; font-weight: bold;border:0px solid red !important">
                                <tr>
                                    <td style="padding:0px 4px;text-align: left;font-size:16px">Bill to</td>
                                </tr>
                            </table>
                            {order.consignee_full_name if order.billing_is_same_as_consignee else order.billing_full_name}<br />
                            {order.consignee_address if order.billing_is_same_as_consignee else order.billing_address}<br />
                            {order.consignee_city if order.billing_is_same_as_consignee else order.billing_city},
                            {order.consignee_state if order.billing_is_same_as_consignee else order.billing_state},
                            {order.consignee_country if order.billing_is_same_as_consignee else order.billing_country}<br />   
                            {order.consignee_phone if order.billing_is_same_as_consignee else order.billing_phone}<br />
                        </td>
                        <td>
                             <table style="width: 100%;  color: #000; font-weight: bold;border:0px solid red !important">
                                <tr>
                                    <td style="padding:0px 4px;text-align: left;font-size:16px">Ship to</td>
                                </tr>
                            </table>
                            {order.consignee_full_name}<br/>
                            {order.consignee_address}<br/>    
                            {order.consignee_city},{order.consignee_state} ,{order.consignee_country}<br/>  
                            {order.consignee_phone}     
                        </td>
                    </tr>
                </table>

                <!-- Product Table -->
                <table>
                    <thead>
                        <tr style="background-color: #CCCCCC; color: #000;font-size:10px">
                            <th>Product</th>
                            <th>SKU</th>
                            <th>HSN</th>
                            <th>QTY</th>
                            <th>Unit price</th>
                            <th>Tax Rate</th>
                            <th>Tax Amt</th>
                            <th>Final Amt</th>
                        </tr>
                    </thead>
                    {product_rows}
                </table>

                <!-- Summary -->
                <table style="width: 100%; margin-top: 1px; border:0px solid red !important">
                    <tr style=" border:0px solid red !important">
                        <td style="width: 60%;border:0px">
                    </td>
                    <td style="border: 1px solid black;text-align: right; border:0px solid red">
                        <table style="width: 100%;border:0px solid red">                                
                            <!-- Spacer Row -->
                            <tr style="border:0px solid red"><td colspan="2" style="height: 8px;border:0px solid red"></td></tr>

                            <tr style="background-color: #CCCCCC; color: #000;border:0px solid red">
                                <td style="text-align: left; width: 120px; font-weight: bold; padding: 6px 12px;">Subtotal :</td>
                                <td style="text-align: right; padding: 6px 12px;">Rs {sub_total1:.2f}</td>
                            </tr>

                            <!-- Spacer Row -->
                            <tr style="border:0px solid red"><td colspan="2" style="height: 8px; border:0px solid red"></td></tr>

                            <tr style="background-color: #CCCCCC; color: #000;border:0px solid red">
                                <td style="text-align: left; width: 120px; font-weight: bold; padding: 6px 12px;">Shipping Cost:</td>
                                <td style="text-align: right; padding: 6px 12px;">Rs {order.shipping_charges or 0:.2f}</td>
                            </tr>

                            <!-- Spacer Row -->
                            <tr><td colspan="2" style="height: 8px;border:0px solid red"></td></tr>

                            <tr style="background-color: #CCCCCC; color: #000;border:0px solid red">
                                <td style="text-align: left; width: 120px; font-weight: bold; padding: 6px 12px;">Grand Total :</td>
                                <td style="text-align: right; padding: 6px 12px;">Rs {order.total_amount:.2f}</td> </tr>                                
                                
                        </table>
                    </td>
                    </tr>
                </table>
            </body>
            </html>
            """
        return html_content

    except Exception as e:

        logger.error(
            msg="could not create shipping label : {}".format(str(e)),
        )

        return None
