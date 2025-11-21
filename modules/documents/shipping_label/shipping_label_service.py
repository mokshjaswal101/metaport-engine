"""
Optimized shipping label service with improved settings handling and template organization
"""

import http
import base64
import io
from psycopg2 import DatabaseError
from typing import List, Optional
from sqlalchemy.orm import joinedload
from fastapi import APIRouter, File, UploadFile, Form
from xhtml2pdf import pisa
from pypdf import PdfWriter, PdfReader
from sqlalchemy import select
from pathlib import Path
from logger import logger
from fastapi.responses import StreamingResponse
from io import BytesIO
from modules.aws_s3.aws_s3 import upload_file_to_s3, delete_file_from_s3
from http import HTTPStatus
from context_manager.context import context_user_data, get_db_session

# New optimized templates
from modules.documents.shipping_label.templates import LabelTemplateFactory

# models
from models import Shipping_Label_Setting, Order, Client, User

# schema
from schema.base import GenericResponseModel
from .shipping_label_schema import LabelSettingUpdateModel, LabelSettingResponseModel

from shipping_partner.shiprocket.shiprocket import Shiprocket
from shipping_partner.ats.ats import ATS


class LabelSettingsCache:
    """Simple cache for label settings to avoid repeated database calls"""

    _cache = {}

    @classmethod
    def get(cls, client_id: int) -> Optional[LabelSettingResponseModel]:
        return cls._cache.get(client_id)

    @classmethod
    def set(cls, client_id: int, settings: LabelSettingResponseModel):
        cls._cache[client_id] = settings

    @classmethod
    def invalidate(cls, client_id: int):
        cls._cache.pop(client_id, None)


class ShippingLabelService:

    # Default settings moved to a more organized structure
    DEFAULT_LABEL_SETTINGS = {
        "logo_url": "",
        "label_format": "default",
        "order_id_barcode_enabled": True,
        "barcode_format": "code-128A",
        "logo_shown": False,
        "consignee_phone": True,
        "package_dimensions": True,
        "weight": True,
        "order_date": True,
        "payment_type": True,
        "company_name": True,
        "pickup_address": True,
        "SKU": False,
        "product_name": True,
        "prepaid_amount": True,
        "COD_amount": True,
        "message": True,
        "branding": True,
    }

    @staticmethod
    def convert_html_to_pdf(html_content: str) -> Optional[BytesIO]:
        """Convert HTML to PDF with error handling"""
        try:
            pdf_buffer = io.BytesIO()
            pisa_status = pisa.CreatePDF(io.StringIO(html_content), dest=pdf_buffer)

            if pisa_status.err:
                logger.error("Error creating PDF from HTML")
                return None

            pdf_buffer.seek(0)
            return pdf_buffer
        except Exception as e:
            logger.error(f"Exception in HTML to PDF conversion: {str(e)}")
            return None

    @staticmethod
    def get_client_info(
        client_id: int, db_session
    ) -> tuple[str, Optional[LabelSettingResponseModel]]:
        """Get client name and cached/fresh label settings"""

        # Try to get settings from cache first
        cached_settings = LabelSettingsCache.get(client_id)
        if cached_settings:
            client = db_session.query(Client).filter(Client.id == client_id).first()
            return client.client_name if client else "", cached_settings

        # Fetch both client and settings in one go if not cached
        client = db_session.query(Client).filter(Client.id == client_id).first()
        client_name = client.client_name if client else ""

        # Get fresh settings and cache them
        settings_response = ShippingLabelService._get_fresh_label_settings(
            client_id, db_session
        )

        if settings_response.status:
            settings = settings_response.data["label_settings"]
            LabelSettingsCache.set(client_id, settings)
            return client_name, settings

        return client_name, None

    @staticmethod
    async def _get_fresh_label_settings(
        client_id: int, db_session
    ) -> GenericResponseModel:
        """Get fresh label settings from database asynchronously"""
        try:
            # Execute async query
            result = await db_session.execute(
                select(Shipping_Label_Setting).where(
                    Shipping_Label_Setting.client_id == client_id
                )
            )
            settings = result.scalars().first()  # Get the first row

            if not settings:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={
                        "label_settings": LabelSettingResponseModel(
                            **ShippingLabelService.DEFAULT_LABEL_SETTINGS
                        )
                    },
                    message="Label settings retrieved successfully.",
                )

            serialized_settings = LabelSettingResponseModel(
                **settings.to_model().model_dump()
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data={"label_settings": serialized_settings},
                message="Label settings retrieved successfully.",
            )

        except Exception as e:
            logger.error(f"Error fetching label settings: {str(e)}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Could not retrieve label settings.",
            )

    # @staticmethod
    # def get_label_settings() -> GenericResponseModel:
    #     try:
    #         client_id = context_user_data.get().client_id

    #         # Return cached settings if available
    #         cached = LabelSettingsCache.get(client_id)
    #         if cached:
    #             return GenericResponseModel(
    #                 status_code=http.HTTPStatus.OK,
    #                 status=True,
    #                 data={"label_settings": cached},
    #                 message="Label settings retrieved successfully (cached).",
    #             )

    #         # Fallback to database
    #         db = get_db_session()
    #         return ShippingLabelService._get_fresh_label_settings(client_id, db)

    #     except DatabaseError as db_error:
    #         logger.error(
    #             f"Database error while retrieving label settings: {str(db_error)}"
    #         )
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             status=False,
    #             message="Could not retrieve label settings. Please try again.",
    #         )

    #     except Exception as ex:
    #         logger.error(f"Unhandled error while retrieving label settings: {str(ex)}")
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             status=False,
    #             message="An internal server error occurred. Please try again later.",
    #         )
    @staticmethod
    async def get_label_settings() -> GenericResponseModel:
        db = None
        try:
            client_id = context_user_data.get().client_id

            # Return cached settings if available
            cached = LabelSettingsCache.get(client_id)
            if cached:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data={"label_settings": cached},
                    message="Label settings retrieved successfully (cached).",
                )

            # Get AsyncSession from context
            db = get_db_session()  # AsyncSession
            return await ShippingLabelService._get_fresh_label_settings(client_id, db)

        except DatabaseError as db_error:
            logger.error(
                f"Database error while retrieving label settings: {str(db_error)}"
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Could not retrieve label settings. Please try again.",
            )

        except Exception as ex:
            logger.error(f"Unhandled error while retrieving label settings: {str(ex)}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="An internal server error occurred. Please try again later.",
            )

        finally:
            if db:
                await db.close()  # Ensure async session is closed

    @staticmethod
    def generate_label(order_ids: List[str]) -> str:
        try:
            from ..invoice.invoice import default_invoice, order_invoice

            db = get_db_session()
            client_id = context_user_data.get().client_id

            # Fetch all the relevant orders
            orders = (
                db.query(Order)
                .filter(
                    Order.order_id.in_(order_ids),
                    Order.client_id == client_id,
                    Order.awb_number.isnot(None),
                )
                .order_by(Order.created_at.desc())
                .options(joinedload(Order.pickup_location))
                .all()
            )

            if not orders:
                logger.warning("No valid orders found for label generation")
                return "No valid orders found"

            total_orders = len(orders)
            if client_id == 186:
                logger.info(
                    f"Starting label generation with invoices for {total_orders} orders (client 186)"
                )
            else:
                logger.info(f"Starting label generation for {total_orders} orders")

            # Get client info and settings efficiently
            client_name, label_settings = ShippingLabelService.get_client_info(
                client_id, db
            )

            if not label_settings:
                logger.error("Could not retrieve label settings")
                return "Error retrieving label settings"

            # Initialize PDF merger
            try:
                merger = PdfWriter()
            except Exception as e:
                logger.error(f"Could not initialize PDF merger: {str(e)}")
                return "Error initializing PDF generation"

            # Process orders efficiently - generate label with invoice included for client 186
            orders_to_update = []
            for index, order in enumerate(orders, 1):
                logger.info(
                    f"Processing order {index}/{total_orders}: {order.order_id}"
                )

                # Generate and add label (with invoice included for client 186)
                pdf_buffer = ShippingLabelService._process_single_order(
                    order, label_settings, client_name, client_id
                )

                if pdf_buffer:
                    pdf_buffer.seek(0)
                    merger.append(PdfReader(pdf_buffer))
                    logger.info(
                        f"Label (with invoice) generated for order {index}/{total_orders}: {order.order_id}"
                    )

                    # Mark order for status update if needed
                    if order.status == "booked":
                        orders_to_update.append(order)
                else:
                    logger.warning(
                        f"Failed to generate label for order {order.order_id}"
                    )

            # Bulk update order statuses
            if orders_to_update:
                for order in orders_to_update:
                    order.status = "pickup"
                    order.sub_status = "pickup pending"
                    order.is_label_generated = True
                    db.add(order)

            # Generate final PDF
            merged_pdf = io.BytesIO()
            merger.write(merged_pdf)
            merger.close()
            merged_pdf.seek(0)

            # Commit all changes at once
            db.commit()

            pdf_buffer = merged_pdf.getvalue()

            # Log message based on client
            if client_id == 186:
                logger.info(
                    f"Successfully generated combined labels with invoices for {total_orders} orders (client 186)"
                )
            else:
                logger.info(f"Successfully generated labels for {total_orders} orders")

            # Choose filename: use AWB if only one label, otherwise default to label.pdf
            if len(orders) == 1 and getattr(orders[0], "awb_number", None):
                filename = f"{orders[0].awb_number}.pdf"
            else:
                if client_id == 186:
                    filename = "labels_with_invoices.pdf"
                else:
                    filename = "labels.pdf"

            return StreamingResponse(
                BytesIO(pdf_buffer),
                media_type="application/pdf",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )

        except Exception as e:
            logger.error(f"Error in generate_label: {str(e)}")
            return "Error generating labels"

    @staticmethod
    def _process_single_order(
        order, label_settings, client_name: str, client_id: int
    ) -> Optional[BytesIO]:
        """Process a single order for label generation (with invoice for client 186)"""
        try:
            # Handle ATS courier separately
            # Amazon only accepts their own Shipping Label
            if order.aggregator == "ats":
                base64_pdf = ATS.generate_label(order)

                if base64_pdf:
                    pdf_binary = base64.b64decode(base64_pdf)
                    pdf_buffer = io.BytesIO(pdf_binary)

                    # For client 186, append invoice to ATS label
                    if client_id == 186:
                        return ShippingLabelService._append_invoice_to_pdf(
                            pdf_buffer, order, client_name, client_id
                        )

                    return pdf_buffer
                else:
                    logger.warning(
                        f"Could not generate ATS label for order {order.order_id}"
                    )
                    return None

            # Generate label using template factory -> automatically decides which type of label is to generated
            shipping_label_html = LabelTemplateFactory.create_label(
                order, label_settings, client_name, client_id
            )

            if not shipping_label_html:
                logger.warning(
                    f"Could not generate label HTML for order {order.order_id}"
                )
                return None

            # Convert label to PDF
            label_pdf_buffer = ShippingLabelService.convert_html_to_pdf(
                shipping_label_html
            )

            if not label_pdf_buffer:
                return None

            # For client 186, append invoice to label
            if client_id == 186:
                return ShippingLabelService._append_invoice_to_pdf(
                    label_pdf_buffer, order, client_name, client_id
                )

            return label_pdf_buffer

        except Exception as e:
            logger.error(f"Error processing order {order.order_id}: {str(e)}")
            return None

    @staticmethod
    def _append_invoice_to_pdf(
        label_pdf_buffer: BytesIO, order, client_name: str, client_id: int
    ) -> Optional[BytesIO]:
        """Append invoice to label PDF for client 186"""
        try:
            from ..invoice.invoice import order_invoice

            # Generate invoice HTML
            invoice_html = order_invoice(order, client_name, client_id)
            invoice_pdf_buffer = ShippingLabelService.convert_html_to_pdf(invoice_html)

            if not invoice_pdf_buffer:
                logger.warning(
                    f"Failed to generate invoice for order {order.order_id}, returning label only"
                )
                return label_pdf_buffer

            # Merge label and invoice PDFs
            merger = PdfWriter()
            label_pdf_buffer.seek(0)
            invoice_pdf_buffer.seek(0)

            merger.append(PdfReader(label_pdf_buffer))
            merger.append(PdfReader(invoice_pdf_buffer))

            combined_pdf = io.BytesIO()
            merger.write(combined_pdf)
            merger.close()
            combined_pdf.seek(0)

            logger.info(f"Invoice appended to label for order {order.order_id}")
            return combined_pdf

        except Exception as e:
            logger.error(
                f"Error appending invoice to label for order {order.order_id}: {str(e)}"
            )
            # Return label only if invoice fails
            return label_pdf_buffer

    # @staticmethod
    # def update_label_settings(
    #     label_parameters: LabelSettingUpdateModel,
    # ) -> GenericResponseModel:
    #     """Update label settings and invalidate cache"""
    #     client_id = context_user_data.get().client_id

    #     try:
    #         with get_db_session() as db:
    #             settings = (
    #                 db.query(Shipping_Label_Setting)
    #                 .filter(Shipping_Label_Setting.client_id == client_id)
    #                 .first()
    #             )

    #             if not settings:
    #                 settings = Shipping_Label_Setting(client_id=client_id)
    #                 db.add(settings)

    #             # Update settings
    #             updated_values = label_parameters.model_dump(exclude_unset=True)
    #             for key, value in updated_values.items():
    #                 setattr(settings, key, value)

    #             db.add(settings)
    #             db.commit()

    #             # Invalidate cache
    #             LabelSettingsCache.invalidate(client_id)

    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.OK,
    #             status=True,
    #             message="Settings updated successfully.",
    #         )

    #     except DatabaseError as db_error:
    #         logger.error(
    #             f"Database error while updating label settings: {str(db_error)}"
    #         )
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             status=False,
    #             message="Could not update label settings. Please try again.",
    #         )

    #     except Exception as ex:
    #         logger.error(f"Unhandled error during label settings update: {str(ex)}")
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             status=False,
    #             message="An internal server error occurred. Please try again later.",
    #         )

    @staticmethod
    async def update_label_settings(
        label_parameters: LabelSettingUpdateModel,
    ) -> GenericResponseModel:
        """Update label settings and invalidate cache asynchronously"""
        client_id = context_user_data.get().client_id
        db_session = get_db_session()  # Should be async session from context

        try:
            # Fetch existing settings asynchronously
            result = await db_session.execute(
                select(Shipping_Label_Setting).where(
                    Shipping_Label_Setting.client_id == client_id
                )
            )
            settings = result.scalars().first()

            if not settings:
                settings = Shipping_Label_Setting(client_id=client_id)
                db_session.add(settings)

            # Update only provided values
            updated_values = label_parameters.model_dump(exclude_unset=True)
            for key, value in updated_values.items():
                setattr(settings, key, value)

            db_session.add(settings)
            await db_session.commit()  # Async commit

            # Invalidate cache
            LabelSettingsCache.invalidate(client_id)

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Settings updated successfully.",
            )

        except Exception as ex:
            await db_session.rollback()  # Rollback on error
            logger.error(f"Error updating label settings: {str(ex)}")
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                status=False,
                message="Could not update label settings. Please try again later.",
            )

        finally:
            await db_session.close()  # Close async session

    @staticmethod
    def generate_invoice(order_ids: List[str]) -> str:
        """Generate invoice - keeping existing functionality"""
        try:
            from ..invoice.invoice import default_invoice, order_invoice

            with get_db_session() as db:
                client_id = context_user_data.get().client_id

                orders = (
                    db.query(Order)
                    .filter(
                        Order.order_id.in_(order_ids),
                        Order.client_id == client_id,
                        Order.awb_number.isnot(None),
                    )
                    .order_by(Order.created_at.desc())
                    .options(joinedload(Order.pickup_location))
                )

                client = db.query(Client).filter(Client.id == client_id).first()
                client_name = client.client_name

                try:
                    merger = PdfWriter()
                except Exception as e:
                    logger.error(f"Could not generate PDFs: {str(e)}")
                    return "Error generating PDFs"

                for order in orders:
                    if order.awb_number is not None:
                        if client_id == 186:
                            invoice = order_invoice(order, client_name, client_id)
                        else:
                            invoice = default_invoice(order, client_name, client_id)

                        pdf_buffer = ShippingLabelService.convert_html_to_pdf(invoice)
                        if pdf_buffer:
                            pdf_buffer.seek(0)
                            merger.append(PdfReader(pdf_buffer))
                        else:
                            logger.warning(
                                f"Error creating PDF for order {order.order_id}"
                            )

                merged_pdf = io.BytesIO()
                merger.write(merged_pdf)
                merger.close()
                merged_pdf.seek(0)

                return base64.b64encode(merged_pdf.getvalue()).decode("utf-8")

        except Exception as e:
            logger.error(f"Could not generate invoices: {str(e)}")
            return "Error generating invoices"

    # @staticmethod
    # def upload_logo(file: UploadFile) -> GenericResponseModel:
    #     """Upload logo with optimized S3 handling"""
    #     try:
    #         filename = file.filename
    #         content_type = file.content_type
    #         client_id = context_user_data.get().client_id

    #         # Validate file extension
    #         file_extension = filename.split(".")[-1].lower()
    #         allowed_extensions = {"jpg", "jpeg", "png"}
    #         if file_extension not in allowed_extensions:
    #             raise ValueError("Invalid file type. Allowed: jpg, jpeg, png")

    #         # Standard filename and S3 key
    #         company_logo_filename = f"company_logo.{file_extension}"
    #         s3_key = f"{client_id}/shipping_labels/{company_logo_filename}"

    #         # Handle existing logo deletion
    #         with get_db_session() as db:
    #             existing_settings = (
    #                 db.query(Shipping_Label_Setting)
    #                 .filter(Shipping_Label_Setting.client_id == client_id)
    #                 .first()
    #             )

    #             if existing_settings and existing_settings.logo_url:
    #                 try:
    #                     # Extract S3 key from URL and delete old logo
    #                     old_s3_key = existing_settings.logo_url.split(
    #                         ".amazonaws.com/", 1
    #                     )[1]
    #                     delete_result = delete_file_from_s3(old_s3_key)
    #                     if not delete_result["success"]:
    #                         logger.warning(
    #                             f"Failed to delete old logo: {delete_result.get('error')}"
    #                         )
    #                 except Exception as delete_error:
    #                     logger.warning(f"Error deleting old logo: {str(delete_error)}")

    #         # Upload new logo
    #         upload_result = upload_file_to_s3(
    #             file_obj=file.file, s3_key=s3_key, content_type=content_type
    #         )

    #         if not upload_result["success"]:
    #             raise Exception(f"S3 upload failed: {upload_result.get('error')}")

    #         file_url = upload_result["url"]

    #         # Invalidate cache since logo URL changed
    #         LabelSettingsCache.invalidate(client_id)

    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.OK,
    #             data={"file": file_url},
    #             message="Image uploaded successfully.",
    #         )

    #     except Exception as e:
    #         logger.error(f"Error uploading image: {str(e)}")
    #         return GenericResponseModel(
    #             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
    #             data=str(e),
    #             message="An error occurred during image upload.",
    #         )

    @staticmethod
    async def upload_logo(file: UploadFile) -> GenericResponseModel:
        try:
            filename = file.filename
            content_type = file.content_type
            client_id = context_user_data.get().client_id

            # Validate extension
            ext = filename.split(".")[-1].lower()
            if ext not in {"jpg", "jpeg", "png"}:
                raise ValueError("Invalid image type")

            s3_key = f"{client_id}/shipping_labels/company_logo.{ext}"

            # Read file bytes
            file_bytes = await file.read()
            if not file_bytes:
                raise ValueError("Empty file received")

            print("FILE BYTES LENGTH =", len(file_bytes))

            # ---- Delete old logo using AsyncSession ----
            async with get_db_session() as db:
                result = await db.execute(
                    select(Shipping_Label_Setting).where(
                        Shipping_Label_Setting.client_id == client_id
                    )
                )
                existing = result.scalars().first()

                if (
                    existing
                    and existing.logo_url
                    and ".amazonaws.com/" in existing.logo_url
                ):
                    old_key = existing.logo_url.split(".amazonaws.com/", 1)[1]
                    delete_file_from_s3(old_key)

            print("OLD LOGO DELETED IF EXISTS")

            # ---- Upload new logo ----
            upload_result = await upload_file_to_s3(
                file_bytes=file_bytes,
                s3_key=s3_key,
                content_type=content_type,
            )
            print("UPLOAD RESULT =", upload_result)

            if not upload_result["success"]:
                raise Exception(f"S3 upload failed: {upload_result['error']}")

            # Invalidate cache
            LabelSettingsCache.invalidate(client_id)

            return GenericResponseModel(
                status_code=200,
                status=True,
                data={"file": upload_result["url"]},
                message="Image uploaded successfully.",
            )

        except Exception as e:
            logger.error(f"Error uploading image: {e}")
            return GenericResponseModel(
                status_code=500,
                status=False,
                message=str(e),
            )
