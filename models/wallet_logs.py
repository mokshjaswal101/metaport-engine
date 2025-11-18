from sqlalchemy import Column, Integer, ForeignKey, Numeric, String, TIMESTAMP
from sqlalchemy.orm import relationship

from database import DBBaseClass, DBBase


class Wallet_Logs(DBBase, DBBaseClass):
    __tablename__ = "wallet_logs"

    datetime = Column(TIMESTAMP(timezone=True), nullable=False)
    transaction_type = Column(String(20), nullable=False)

    credit = Column(Numeric(20, 3), nullable=True)
    debit = Column(Numeric(20, 3), nullable=True)
    wallet_balance_amount = Column(Numeric(20, 3), nullable=False)
    cod_balance_amount = Column(Numeric(20, 3), nullable=False)

    reference = Column(String(255), nullable=True)
    description = Column(String(255), nullable=True)

    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)
    wallet_id = Column(Integer, ForeignKey("wallet.id"), nullable=False)

    client = relationship("Client")

    def to_model(self):
        from modules.wallet.wallet_schema import wallet_log

        return wallet_log.model_validate(self)

    # convert the received object into an instance of the model
    def create_db_entity(self):
        entity = self.model_dump()
        return Wallet_Logs(**entity)
