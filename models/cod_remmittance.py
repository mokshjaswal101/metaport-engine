from sqlalchemy import Column, Integer, ForeignKey, Numeric, String, TIMESTAMP
from sqlalchemy.orm import relationship

from database import DBBaseClass, DBBase


class COD_Remittance(DBBase, DBBaseClass):
    __tablename__ = "cod_remittance"

    payout_date = Column(TIMESTAMP(timezone=True), nullable=False)
    generated_cod = Column(Numeric(10, 3), nullable=False, default=0.00)

    freight_deduction = Column(Numeric(10, 3), nullable=False, default=0.00)
    early_cod_charges = Column(Numeric(10, 3), nullable=False, default=0.00)
    rto_reversal_amount = Column(Numeric(10, 3), nullable=False, default=0.00)
    remittance_amount = Column(Numeric(10, 3), nullable=False, default=0.00)

    tax_deduction = Column(Numeric(10, 3), nullable=False, default=0.00)
    amount_paid = Column(Numeric(10, 3), nullable=False, default=0.00)

    payment_method = Column(String, nullable=True)

    order_count = Column(Integer, nullable=False, default=0)

    utr_number = Column(String, nullable=True)

    remarks = Column(String(255), nullable=True)
    status = Column(String(100), nullable=True)

    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)

    client = relationship("Client")

    def to_model(self):
        from modules.orders.order_schema import COD_Remitance_Model

        return COD_Remitance_Model.model_validate(self)

    # convert the received object into an instance of the model
    def create_db_entity(self):
        entity = self.model_dump()
        return COD_Remittance(**entity)
