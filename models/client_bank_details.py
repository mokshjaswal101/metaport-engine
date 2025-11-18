from sqlalchemy import Column, String, Boolean, Integer, ForeignKey

from sqlalchemy.orm import Session, relationship

from database import DBBaseClass, DBBase


class Client_Bank_Details(DBBase, DBBaseClass):
    __tablename__ = "client_bank_details"

    user_id = Column(Integer, ForeignKey("user.id"), nullable=False)
    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)
    client_onboarding_id = Column(
        Integer, ForeignKey("client_onboarding_details.id"), nullable=False
    )

    beneficiary_name = Column(String(255), nullable=True)
    bank_name = Column(String(255), nullable=True)
    account_no = Column(String(255), nullable=True)
    account_type = Column(String(255), nullable=True)
    ifsc_code = Column(String(200), nullable=True)
    upload_cheque = Column(String(255), nullable=True)
