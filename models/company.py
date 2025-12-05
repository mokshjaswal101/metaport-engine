from sqlalchemy import Column, String
from sqlalchemy.orm import Session, relationship

from database import DBBaseClass, DBBase

import uuid as uuid


class Company(DBBase, DBBaseClass):
    __tablename__ = "company"

    company_name = Column(String(255), nullable=False)
    company_code = Column(String(255), nullable=False)

    # relationships
    clients = relationship("Client", back_populates="company", lazy="noload")
    users = relationship("User", back_populates="company", lazy="noload")
    # Note: Orders are now linked directly to Client, not Company

    # whenever a company is created, automatically create a company code for it as well
    def __init__(self, company_name):
        self.company_name = company_name
        self.company_code = self.generate_code()

    # function to generate the company code from the company name
    def generate_code(self):
        company_words = self.company_name.split()
        # If company name has only one word
        if len(company_words) == 1:
            initials = company_words[0][:2] + company_words[0][-2:]
        # If company name has more than one word
        else:
            initials = "".join(word[0] for word in company_words)[:4]

        # Random 7 characters from UUID
        random_num = uuid.uuid4().hex[:7]
        return f"{initials}_{random_num}"

    def __to_model(self):
        from modules.company.company_schema import CompanyModel

        return CompanyModel.model_validate(self)

    # convert the received object into an instance of the model
    def create_db_entity(self):
        entity = self.model_dump()
        return Company(**entity)

    @classmethod
    def create_company(cls, company_data):
        from context_manager.context import get_db_session

        db: Session = get_db_session()
        db.add(company_data)
        db.flush()

        return company_data.__to_model()

    @classmethod
    def get_by_id(cls, id):
        company = super().get_by_id(id)
        return company.__to_model() if company else None

    @classmethod
    def get_by_uuid(cls, uuid):
        company = super().get_by_uuid(uuid)
        return company.__to_model() if company else None
