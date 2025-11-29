from typing import List, Optional
from datetime import datetime
import http
from logger import logging
from sqlalchemy.future import select
from fastapi.encoders import jsonable_encoder

from modules.qc.qc_schema import QCItemSchema


# from models import Qc
# models
from models import Qc

from context_manager.context import get_db_session, context_user_data


from schema.base import GenericResponseModel


class QcService:

    @staticmethod
    async def get_list() -> GenericResponseModel:
        try:
            async with get_db_session() as db:  # AsyncSession
                user_data = context_user_data.get()
                client_id = user_data.client_id
                # Execute query
                result = await db.execute(select(Qc).where(Qc.client_id == client_id))
                # Get ORM objects as list
                qc_items = result.scalars().all()
                # Encode to JSON-serializable format
                data = [
                    {
                        "category": item.category,
                        "reason_name": item.reason_name,
                        "brand_name": item.brand_name,
                        "item_name": item.item_name,
                        "item_description": item.item_description,
                        "is_mandatory": item.is_mandatory,
                    }
                    for item in qc_items
                ]

                return GenericResponseModel(
                    status=True,
                    status_code=http.HTTPStatus.OK,
                    message="QC list fetched successfully",
                    data=jsonable_encoder(data),
                )
        except Exception as e:
            logging.error(f"Error fetching QC list: {str(e)}")
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to fetch QC list",
                data=[],
            )

    @staticmethod
    async def add_qc(qc_item: QCItemSchema) -> GenericResponseModel:
        try:
            async with get_db_session() as db:  # AsyncSession required
                user_data = context_user_data.get()
                existing_qc = await db.execute(
                    select(Qc).where(
                        Qc.reason_name == qc_item.reasonName,
                    )
                )
                existing_qc = existing_qc.scalars().first()
                if existing_qc:
                    return GenericResponseModel(
                        status=False,
                        status_code=http.HTTPStatus.CONFLICT,  # 409
                        message="Reason is already exist",
                    )
                #  STEP 2 — Create new QC item
                new_qc = Qc(
                    client_id=user_data.client_id,
                    category=qc_item.category,
                    reason_name=qc_item.reasonName,
                    brand_name=qc_item.brandName,
                    item_name=qc_item.itemName,
                    item_description=qc_item.itemDescription,
                    is_mandatory=qc_item.isMandatory,
                )
                db.add(new_qc)
                await db.commit()
                await db.refresh(new_qc)
                return GenericResponseModel(
                    status=True,
                    status_code=http.HTTPStatus.OK,
                    message="QC added successfully",
                )
        except Exception as e:
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Failed to add QC",
                data={},
            )
