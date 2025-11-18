import base64
import os
from datetime import datetime, timezone
import pytz
from num2words import num2words
from models import Client

# schema
from modules.orders.order_schema import Order_Model
from modules.documents.billing_invoice.billing_invoice_schema import BillingInvoiceModel


from context_manager.context import context_user_data, get_db_session

from .data import client_data


def billing_invoice(invoice: BillingInvoiceModel):
    try:

        client_id = context_user_data.get().client_id

        print(client_id)

        with get_db_session() as db:

            client = db.query(Client).filter(Client.id == client_id).first()

        file_path = (
            os.getcwd()
            + "/modules/documents/shipping_label/templates/client_logos/logo-wh.png"
        )

        print(file_path)

        with open(file_path, "rb") as f:
            base64_code = base64.b64encode(f.read()).decode("utf-8")
            image_src = f"data:image/png;base64,{base64_code}"

        adata = client_data[client_id]

        print(adata)

        grand_total = invoice.total_amount + invoice.tax_amount

        tax_line = f"""  <tr>
                    <td style="width:30px"></td>
                    <td>IGST ( 18% )</td>
                    <td></td>
                    <td style=" text-align:center"> {invoice.tax_amount}</td>
                    <td style="width:30px"></td>
                </tr> """

        if adata["State Code"] == 7:
            tax_line = f"""  <tr>
                    <td style="width:30px"></td>
                    <td>CGST ( 9% )</td>
                    <td></td>
                    <td style=" text-align:center"> {invoice.tax_amount / 2}</td>
                    <td style="width:30px"></td>
                </tr> <tr>
                    <td style="width:30px"></td>
                    <td>SGST ( 9% )</td>
                    <td></td>
                    <td style=" text-align:center"> {invoice.tax_amount / 2}</td>
                    <td style="width:30px"></td>
                </tr>  """

        # Defining the HTML for the label structure
        html_content = f"""
        <html>
        <head>
            <style>
               @page {{
                    margin: 15px 25px; /* Optional: remove default margin */
                    border: 1px solid black
                }}
                body {{
                    font-family: Arial, sans-serif;
                    margin: 0;
                    padding: 0;
                    width: 100%;
                    font-size: 12px;
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
            
            
            <div style:'background:gray'>
           
           
           
           <div style="">
            
            <table style="font-size:12px; width:100%;padding:0 15px">
            
                <tr><td style="height:15px"></td></tr>
            
              
                <tr><td style="height:15px"></td></tr>
                
                <tr>
                    
                    <td style="width:55%">
                        <div>
                            <img src={image_src} style="width:150px"/><br /><br />
                            <span><b>4PL SCM TECHNOLOGIES PRIVATE LIMITED</b></span><br />
                            <span>3rd Floor, LSC-2, Plot No-10, Near ICICI Bank,<br /> Sector-6, Dwarka, New Delhi, <br /> South West Delhi-110075</span><br />
                            
                            <span style="height:5px"></span>
                            <span><b>Email -</b> accounts@warehousity.com </span><br />
                            <span><b>Phone -</b> 8929863104</span><br />
                            
                        </div>
                    </td>
                    
                    <td style="width:45%">
                        <span style="font-size:20px"><b>Tax Invoice</b></span> <br /><br />
                        <span> <span style="font-size:24px; color:green"><b>PAID</b></span></span>
                    </td>
                    
                </tr>
                
                <tr><td style="height:20px"></td></tr>
                
                 <tr>
                    
                    <td style="width:55%">
                        <div>
                            <span><b>PAN No. - </b>AABCZ3969M</span><br />
                            <span><b>GSTIN - </b> 07AABCZ3969M1ZL</span><br />
                            <span><b>State Code - </b>Delhi, Code : 07</span><br />
                            
                          
                        </div>
                    </td>
                    
                    <td style="width:45%">
                            <span><b>Invoice No. - </b>{invoice.invoice_number}</span><br />
                            <span><b> Invoice Date - </b> 1st June, 2025  </span><br />
                    </td>
                    
                </tr>
                
                
            </table>
            
            </div>
            
            <hr style="margin:20px 0 30px 0"/>
         <table style="font-size:12px; width:100%;padding:0 15px">
                
                <tr>
                    
                    <td style="width:55%">
                        <div>
                            <span>Buyer ( Bill To )</span><br />
                            <span><b><span><b>{client.client_name}</b></span><br /></b></span>
                            <span>{adata['Address'] if adata['Address'] else ""}</span><br />
                            
                           
                        </div>
                    </td>
                    
                    <td style="width:45%">
                        <span><b>GSTIN. - </b>{adata['GSTIN/UIN'] if adata['GSTIN/UIN'] else 'NA'}</span><br />
                        <span><b>PAN - </b> {adata['PAN No.'] if adata['PAN No.'] else 'NA'}</span><br />
                        <span><b>State Code - </b> {adata['State']} - {adata['State Code']}</span><br />
                    </td>
                    
                </tr>
                
                
                
                
            </table>
            
            <hr style="margin:20px 0 30px 0"/>
            
            
            <table  style="font-size:12px;">
                
                <tr style="font-size:14px">
                    <th style="width:30px"></th>
                    <th style="width:50%; text-align:left">Description</th>
                    <th style=" text-align:left">HSN/SAC</th>
                    <th>Amount</th>
                    <th style="width:30px"></th>
                </tr>
                
                <tr><td style="height:5px"></td></tr>
                
                <tr>
                    <td style="width:30px"></td>
                    <td>Freight Charges</td>
                    <td>996812</td>
                    <td style=" text-align:center"> {invoice.total_amount}</td>
                    <td style="width:30px"></td>
                </tr>
                
               
               {tax_line}
                
                <tr>
                    <td style="height:10px"></td>
                </tr>
                
                <tr>
                    <td style="width:30px"></td>
                    <td><b>Grand Total</b></td>
                    <td></td>
                    <td style=" text-align:center"> {grand_total}</td>
                    <td style="width:30px"></td>
                </tr>
                
            </table>
            
            
            
            <hr style="margin:20px 0 20px 0"/>
            
            
           <table  style="font-size:13px; padding: 0 15px; width:100%">
            
                <tr>
                    <td><b>Amount Chargeable in words : </b> {num2words(grand_total, to='currency', lang='en', currency='INR')}</td>
                </tr>
                
            </table>
            
             <hr style="margin:10px 0 30px 0"/>
           
           
           <table  style="font-size:12px; padding: 0 15px">
               
               <tr>
                   <td>Declaration:<br /> We declare that this invoice shows the actual price of the goods described and that all particulars are true and
correct.</td>
               </tr>
           </table>
           
           
            <hr style="margin: 20px 0 30px 0 "/>
           
           
           <table style="font-size:12px; width:100%; padding:0 15px">
               
               <tr>
                   <td style='text-align:center'>SUBJECT TO DELHI JURISDICTION</td>
               </tr>
               
               <tr>
                   <td style='text-align:center; margin:5px 0'>This is a Computer Generated Invoice</td>
               </tr>
           </table>
           
           
           </div>
           
        </body>
        </html>
        """

        return html_content

    except Exception as e:

        print(str)

        return None
