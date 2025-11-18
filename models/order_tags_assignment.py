import json
from sqlalchemy import Column, String, Integer, ForeignKey, Boolean, FLOAT
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import relationship

from logger import logger

from database import DBBaseClass, DBBase
from context_manager.context import get_db_session


class OrderTagsAssignment(DBBase, DBBaseClass):

    __tablename__ = "order_tags_assignment"

    order_id = Column(Integer, ForeignKey("order.id"), primary_key=True)
    tag_id = Column(Integer, ForeignKey("order_tags.id"), primary_key=True)
    assigned_at = Column(String(100), nullable=True)

    def to_model(self):
        from modules.order_tags.order_tags_schema import OrderTagsAssignmentModel

        return OrderTagsAssignmentModel.model_validate(self)

    def create_db_entity(order):
        return OrderTagsAssignment(**order)
