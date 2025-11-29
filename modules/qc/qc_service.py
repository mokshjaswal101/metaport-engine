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
from collections import defaultdict

from context_manager.context import get_db_session, context_user_data


from schema.base import GenericResponseModel


class QcService:

    @staticmethod
    async def get_list() -> GenericResponseModel:
        try:
            async with get_db_session() as db:
                user_data = context_user_data.get()
                client_id = user_data.client_id

                # Fetch all QC items for this client
                result = await db.execute(select(Qc).where(Qc.client_id == client_id))
                qc_items = result.scalars().all()

                # Group items by category and reason_name
                grouped = defaultdict(list)
                for item in qc_items:
                    key = (item.category, item.reason_name)
                    grouped[key].append(
                        {
                            "parameters_name": item.parameters_value,
                            "parameters_value": item.parameters_value,
                            "is_mandatory": item.is_mandatory,
                        }
                    )

                # Build structured data
                data = [
                    {"category": category, "reason_name": reason, "items": items}
                    for (category, reason), items in grouped.items()
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
            async with get_db_session() as db:
                user_data = context_user_data.get()

                # STEP 1 — Check if reason already exists
                existing_reasons = await db.execute(
                    select(Qc).where(
                        Qc.client_id == user_data.client_id,
                        Qc.reason_name == qc_item.reasonName,
                    )
                )
                existing_reasons = existing_reasons.scalars().all()

                if existing_reasons:
                    return GenericResponseModel(
                        status=False,
                        status_code=http.HTTPStatus.CONFLICT,
                        message=f"Reason already exists: {qc_item.reasonName}",
                    )

                # STEP 2 — Add all items for the reason
                for item in qc_item.items:
                    new_qc = Qc(
                        client_id=user_data.client_id,
                        category=qc_item.category,
                        reason_name=qc_item.reasonName,
                        parameters_name=item.parametersName,
                        parameters_value=item.parametersValue,
                        is_mandatory=item.isMandatory,
                    )
                    db.add(new_qc)

                await db.commit()

                return GenericResponseModel(
                    status=True,
                    status_code=http.HTTPStatus.OK,
                    message="QC items added successfully",
                )
        except Exception as e:
            return GenericResponseModel(
                status=False,
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message=f"Failed to add QC: {str(e)}",
                data={},
            )
