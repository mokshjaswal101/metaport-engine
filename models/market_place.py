from sqlalchemy import Column, String, Integer
from database import DBBaseClass, DBBase


class Market_Place(DBBase, DBBaseClass):
    __tablename__ = "market_place"
    which_market_place = Column(String(255), nullable=False, index=True)
    oauth_state = Column(String(255), nullable=True)
    access_token = Column(String(255), nullable=True)
