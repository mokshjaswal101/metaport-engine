from sqlalchemy import Column, String, Boolean, Integer, ForeignKey, Text

from sqlalchemy.orm import Session, relationship

from database import DBBaseClass, DBBase


class Shipping_Label_Files(DBBase, DBBaseClass):
    __tablename__ = "shipping_label_files"

    order = Column(Integer, ForeignKey("order.id"), nullable=False)
    document = Column(Text, nullable=False)
