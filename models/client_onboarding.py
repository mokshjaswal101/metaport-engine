from sqlalchemy import Column, String, Boolean, Integer, ForeignKey

from sqlalchemy.orm import Session, relationship

from database import DBBaseClass, DBBase


class Client_Onboarding(DBBase, DBBaseClass):
    __tablename__ = "client_onboarding"

    client_onboarding_details_id = Column(
        Integer, ForeignKey("client_onboarding_details.id"), nullable=False
    )

    remarks = Column(String(255), nullable=True)
    action_type = Column(String(255), nullable=True)
    client_id = Column(Integer, ForeignKey("client.id"), nullable=False, index=True)

    status = Column(Boolean, nullable=True, default=False)

    onboarding_details = relationship(
        "Client_Onboarding_Details",
        back_populates="onboarding",
        lazy="noload",
    )
