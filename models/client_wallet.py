from sqlalchemy import Column, Integer, ForeignKey, Numeric, String
from sqlalchemy.orm import relationship

from database import DBBaseClass, DBBase


class Client_Wallet(DBBase, DBBaseClass):
    __tablename__ = "client_wallet"

    amount = Column(Numeric(10, 3), nullable=False)
    currency = Column(String(10), nullable=False, default="INR")

    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)

    client = relationship("Client")

    def __to_model(self):
        from modules.user.user_schema import UserModel

        return UserModel.model_validate(self)

    # convert the received object into an instance of the model
    def create_db_entity(self):
        entity = self.model_dump()
        return Client_Wallet(**entity)
