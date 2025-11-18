from sqlalchemy import Column, Integer, Numeric, String, Float

from sqlalchemy.orm import Session

from database import DBBaseClass, DBBase


class Wallet(DBBase, DBBaseClass):
    __tablename__ = "wallet"
    amount = Column(Numeric(20, 3), nullable=False, default=0.0)
    cod_amount = Column(Numeric(20, 3), nullable=False, default=0.0)
    provisional_cod_amount = Column(Numeric(20, 3), nullable=False, default=0.0)
    shipping_notifications = Column(Numeric(20, 3), nullable=False, default=0.0)

    wallet_type = Column(String, nullable=False)
    credit_limit = Column(Numeric(20, 3), nullable=False, default=0.0)

    client_id = Column(Integer, nullable=True)

    hold_amount = Column(Float, nullable=True, default=0.0)

    def to_model(self):
        from modules.wallet.wallet_schema import WalletModel

        return WalletModel.model_validate(self)
