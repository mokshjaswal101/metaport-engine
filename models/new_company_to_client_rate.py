from sqlalchemy import Column, String, Integer, Numeric, Boolean, ForeignKey
from database import DBBaseClass, DBBase
from sqlalchemy.orm import relationship


class New_Company_To_Client_Rate(DBBase, DBBaseClass):
    __tablename__ = "new_company_to_client_rate"

    company_id = Column(Integer, index=True, nullable=False)
    client_id = Column(Integer, index=True, nullable=False)
    client_contract_id = Column(
        Integer, ForeignKey("client_contract.id"), nullable=False
    )
    shipping_partner_id = Column(
        Integer, ForeignKey("shipping_partner.id"), nullable=False
    )

    client_contract = relationship("Client_Contract", lazy="noload")
    shipping_partner = relationship("Shipping_Partner", lazy="noload")

    percentage_rate = Column(Numeric(5, 2), nullable=False)
    absolute_rate = Column(Numeric(5, 2), nullable=False)
    # base_rate
    base_rate_zone_a = Column(Numeric(5, 2), nullable=False)
    base_rate_zone_b = Column(Numeric(5, 2), nullable=False)
    base_rate_zone_c = Column(Numeric(5, 2), nullable=False)
    base_rate_zone_d = Column(Numeric(5, 2), nullable=False)
    base_rate_zone_e = Column(Numeric(5, 2), nullable=False)
    # additional_rate
    additional_rate_zone_a = Column(Numeric(5, 2), nullable=False)
    additional_rate_zone_b = Column(Numeric(5, 2), nullable=False)
    additional_rate_zone_c = Column(Numeric(5, 2), nullable=False)
    additional_rate_zone_d = Column(Numeric(5, 2), nullable=False)
    additional_rate_zone_e = Column(Numeric(5, 2), nullable=False)
    # rto_base
    rto_base_rate_zone_a = Column(Numeric(5, 2), nullable=False)
    rto_base_rate_zone_b = Column(Numeric(5, 2), nullable=False)
    rto_base_rate_zone_c = Column(Numeric(5, 2), nullable=False)
    rto_base_rate_zone_d = Column(Numeric(5, 2), nullable=False)
    rto_base_rate_zone_e = Column(Numeric(5, 2), nullable=False)
    # rto_additional
    rto_additional_rate_zone_a = Column(Numeric(5, 2), nullable=False)
    rto_additional_rate_zone_b = Column(Numeric(5, 2), nullable=False)
    rto_additional_rate_zone_c = Column(Numeric(5, 2), nullable=False)
    rto_additional_rate_zone_d = Column(Numeric(5, 2), nullable=False)
    rto_additional_rate_zone_e = Column(Numeric(5, 2), nullable=False)

    rate_type = Column(String(150), nullable=False)
    # status
    isActive = Column(Boolean, default=True, nullable=False)

    # def to_model(self):
    #     from modules.client_contract.client_contract_schema import (
    #         CompanyToClientContractModel,
    #     )

    #     return CompanyToClientContractModel.model_validate(self)

    # # convert the received object into an instance of the model
    # def create_db_entity(self):
    #     entity = self.model_dump()
    #     return New_Company_To_Client_Rate(**entity)
