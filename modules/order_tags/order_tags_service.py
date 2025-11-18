import http
from psycopg2 import DatabaseError
from sqlalchemy import asc
import re
from fastapi.encoders import jsonable_encoder

from context_manager.context import context_user_data, get_db_session

from logger import logger

# models
from models import OrderTags, OrderTagsAssignment

# schema
from schema.base import GenericResponseModel
from .order_tags_schema import OrderTagsBaseModel, OrderTagsAssignmentModel


class OrderTagsService:

    @staticmethod
    def create_tag(tags_data: OrderTagsBaseModel) -> GenericResponseModel:

        user = context_user_data.get()

        with get_db_session() as db:

            # check if existing tag already exisits
            existing = (
                db.query(OrderTags)
                .filter_by(
                    name=tags_data.name,
                    client_id=user.client_id,
                    is_deleted=False,
                )
                .first()
            )

            if existing:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    message="Tag with the same name already exists.",
                    data={"tag_id": existing.id},
                )

            new_tag = OrderTags(
                name=tags_data.name,
                color=tags_data.colour,
                description=tags_data.description,
                client_id=user.client_id,
            )

            db.add(new_tag)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.CREATED,
                status=True,
                message="Tag created successfully.",
                data={"tag_id": new_tag.id},
            )

    @staticmethod
    def get_all_tags() -> GenericResponseModel:
        user = context_user_data.get()
        with get_db_session() as db:
            tags = (
                db.query(OrderTag)
                .filter_by(
                    client_id=user.client_id,
                    company_id=user.company_id,
                    is_deleted=False,
                )
                .all()
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data=[{"id": tag.id, "name": tag.name} for tag in tags],
                message="Tags fetched successfully.",
            )

    @staticmethod
    def get_tags_for_order(order_id: int) -> GenericResponseModel:
        with get_db_session() as db:
            tags = (
                db.query(OrderTag)
                .join(OrderTagAssociation, OrderTag.id == OrderTagAssociation.tag_id)
                .filter(OrderTagAssociation.order_id == order_id)
                .all()
            )

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                data=[{"id": tag.id, "name": tag.name} for tag in tags],
                message="Tags for the order fetched successfully.",
            )

    @staticmethod
    def assign_tag(order_id: int, tag_id: int) -> GenericResponseModel:
        with get_db_session() as db:
            exists = (
                db.query(OrderTagAssociation)
                .filter_by(order_id=order_id, tag_id=tag_id)
                .first()
            )

            if exists:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.CONFLICT,
                    message="Tag already assigned to the order.",
                )

            assoc = OrderTagAssociation(order_id=order_id, tag_id=tag_id)
            db.add(assoc)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Tag assigned to order successfully.",
            )

    @staticmethod
    def unassign_tag(order_id: int, tag_id: int) -> GenericResponseModel:
        with get_db_session() as db:
            assoc = (
                db.query(OrderTagAssociation)
                .filter_by(order_id=order_id, tag_id=tag_id)
                .first()
            )

            if not assoc:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND,
                    message="Tag not assigned to the order.",
                )

            db.delete(assoc)
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Tag unassigned from order successfully.",
            )

    @staticmethod
    def delete_tag(tag_id: int) -> GenericResponseModel:
        user = context_user_data.get()
        with get_db_session() as db:
            tag = (
                db.query(OrderTag)
                .filter_by(
                    id=tag_id,
                    client_id=user.client_id,
                    company_id=user.company_id,
                    is_deleted=False,
                )
                .first()
            )

            if not tag:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.NOT_FOUND, message="Tag not found."
                )

            tag.is_deleted = True
            db.commit()

            return GenericResponseModel(
                status_code=http.HTTPStatus.OK,
                status=True,
                message="Tag deleted successfully.",
            )
