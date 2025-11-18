import json
from sqlalchemy import Column, String, Integer, ForeignKey, Boolean, FLOAT
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import relationship

from logger import logger

from database import DBBaseClass, DBBase
from context_manager.context import get_db_session


class OrderTags(DBBase, DBBaseClass):

    __tablename__ = "order_tags"

    name = Column(String(100), nullable=False)
    description = Column(String(100), nullable=True)
    color = Column(String(100), nullable=True)

    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)
    client = relationship("Client", lazy="noload")

    def to_model(self):
        from modules.order_tags.order_tags_schema import OrderTagsModel

        return OrderTagsModel.model_validate(self)

    def create_db_entity(order):
        return OrderTags(**order)
