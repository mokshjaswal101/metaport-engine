from sqlalchemy import Column, String, Boolean, Integer, ForeignKey, DateTime

from sqlalchemy.orm import Session, relationship

from database import DBBaseClass, DBBase


class Client_Onboarding_Details(DBBase, DBBaseClass):
    __tablename__ = "client_onboarding_details"

    onboarding_user_id = Column(Integer, ForeignKey("user.id"), nullable=False)
    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)

    company_legal_name = Column(String(255), nullable=True)
    company_name = Column(String(255), nullable=True)
    office_address = Column(String(255), nullable=True)
    landmark = Column(String(255), nullable=True)
    pincode = Column(String(200), nullable=True)
    city = Column(String(255), nullable=True)
    state = Column(String(255), nullable=True)
    country = Column(String(255), nullable=True)
    phone_number = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)

    is_coi = Column(Boolean, nullable=True, default=True)
    is_otp = Column(String(255), nullable=True)
    is_otp_verified = Column(String(255), nullable=True)
    otp_expires_at = Column(DateTime, nullable=True)

    is_gst = Column(Boolean, nullable=True, default=True)
    gst = Column(String(255), nullable=True)
    upload_gst = Column(String(255), nullable=True)

    is_cod_order = Column(Boolean, nullable=True, default=False)

    is_stepper = Column(Integer, nullable=False, default=0)
    is_company_details = Column(Boolean, nullable=True, default=False)
    is_term = Column(Boolean, nullable=True, default=False)
    is_review = Column(Boolean, nullable=True, default=False)
    is_form_access = Column(Boolean, nullable=True, default=True)

    onboarding = relationship(
        "Client_Onboarding", back_populates="onboarding_details", lazy="noload"
    )
