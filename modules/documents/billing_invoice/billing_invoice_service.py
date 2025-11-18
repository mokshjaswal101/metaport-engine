import http
import uuid
from psycopg2 import DatabaseError
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from sqlalchemy import func, cast, DateTime
import pandas as pd
import base64
import io
from xhtml2pdf import pisa

from context_manager.context import context_user_data, get_db_session

# models
from models import Client, Company, Order, Billing_Invoice, Return_Order

# schema
from schema.base import GenericResponseModel
from .billing_invoice_schema import BillingInvoiceModel


# service
from modules.user.user_service import UserService
from .templates.default import billing_invoice

# from modules.wallet.wallet_service import WalletService

# utils
from database.utils import get_primary_key_by_uuid

from logger import logger


# Function to convert HTML to PDF
def convert_html_to_pdf(html_content: str):
    pdf_buffer = io.BytesIO()
    pisa_status = pisa.CreatePDF(io.StringIO(html_content), dest=pdf_buffer)

    if pisa_status.err:
        print("Error creating PDF")
        return None

    pdf_buffer.seek(0)
    return pdf_buffer


class BillingInvoiceService:

    @staticmethod
    def create_billing_invoice() -> GenericResponseModel:
        try:

            print("in")
            with get_db_session() as db:

                clients = db.query(Client).all()

                print(clients)

                count = 1

                updated_orders = []

                for client in clients:
                    client_id = client.id

                    orders = (
                        db.query(Return_Order)
                        .filter(
                            Return_Order.client_id == client_id,
                            Return_Order.invoice_id == None,
                            Return_Order.status == "delivered",
                            Return_Order.booking_date
                            > datetime.fromisoformat("2025-04-01T00:00:00+05:30"),
                        )
                        .all()
                    )

                    if not orders:
                        continue

                    invoice = (
                        db.query(Billing_Invoice)
                        .filter(Billing_Invoice.client_id == client_id)
                        .first()
                    )

                    if not invoice:
                        continue

                    total_amount = 0
                    tax_amount = 0

                    for order in orders:
                        count += 1
                        print(count)
                        order.invoice_id = invoice.id

                        # Calculate totals
                        amount = order.forward_freight + order.forward_cod_charge
                        tax = order.forward_tax

                        if order.status == "RTO":
                            amount += order.rto_freight
                            tax += order.rto_tax

                        total_amount += amount
                        tax_amount += tax

                        updated_orders.append(order)

                        db.add(order)

                    invoice.total_amount = invoice.total_amount + total_amount
                    invoice.tax_amount = invoice.tax_amount + tax_amount

                    db.add(invoice)
                    db.commit()

                    print("updated orders - ", len(updated_orders))

                # Fetch the order details

            return GenericResponseModel(
                status=True,
                status_code=http.HTTPStatus.OK,
                message="An error occurred while creating the Client.",
            )

        except Exception as e:

            print(str(e))
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating client: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Client.",
            )

    @staticmethod
    def get_invoice() -> GenericResponseModel:
        try:

            client_id = context_user_data.get().client_id

            with get_db_session() as db:

                invoices = (
                    db.query(Billing_Invoice)
                    .filter(Billing_Invoice.client_id == client_id)
                    .all()
                )

                formatted_invoices = [
                    BillingInvoiceModel(**invoice.to_model().model_dump())
                    for invoice in invoices
                ]

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={"invoice_data": formatted_invoices},
                    message="success",
                )

        except Exception as e:
            # Log database error
            print(str(e))
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating client: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Client.",
            )

    @staticmethod
    def download_invoice(invoice_id: str) -> GenericResponseModel:
        try:

            client_id = context_user_data.get().client_id

            with get_db_session() as db:

                print(1)

                invoice = (
                    db.query(Billing_Invoice)
                    .filter(
                        Billing_Invoice.client_id == client_id,
                        Billing_Invoice.id == invoice_id,
                    )
                    .first()
                )

                if not invoice:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="Invalid Invoice",
                    )

                invoice_pdf = billing_invoice(invoice=invoice)
                pdf_buffer = convert_html_to_pdf(invoice_pdf)

                if not pdf_buffer:
                    logger.error(
                        extra=context_user_data.get(),
                        msg="PDF buffer generation failed.",
                    )
                    return "Error generating PDF"

                pdf_buffer.seek(0)
                db.commit()

                # Return the PDF as a downloadable file
                return base64.b64encode(pdf_buffer.getvalue()).decode("utf-8")

        except Exception as e:
            # Log database error
            print(str(e))
            logger.error(
                extra=context_user_data.get(),
                msg="Error creating client: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while creating the Client.",
            )
