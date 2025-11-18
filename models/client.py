from sqlalchemy import Column, String, Integer, ForeignKey, Boolean
from sqlalchemy.orm import relationship, Session

from database import DBBaseClass, DBBase
from fastapi.encoders import jsonable_encoder
import uuid as uuid


class Client(DBBase, DBBaseClass):
    __tablename__ = "client"

    client_name = Column(String(255), nullable=False)
    client_code = Column(String(255), nullable=False)

    is_onboarding_completed = Column(Boolean, default=False, nullable=False)

    company_id = Column(Integer, ForeignKey("company.id"), nullable=False)

    # relationships
    company = relationship("Company", back_populates="clients", lazy="noload")
    users = relationship("User", back_populates="client", lazy="noload")
    orders = relationship("Order", back_populates="client", lazy="noload")

    # whenever a client is created, automatically create a company code for it as well
    def __init__(self, client_name, company_id):
        self.client_name = client_name
        self.company_id = company_id
        self.client_code = self.generate_code()

    # function to generate the client id from the client name
    def generate_code(self):
        client_words = self.client_name.split()
        # If client name has only one word
        if len(client_words) == 1:
            initials = client_words[0][:2] + client_words[0][-2:]
        # If client name has more than one word
        else:
            initials = "".join(word[0] for word in client_words)[:4]

        # Random 7 characters from UUID
        random_num = uuid.uuid4().hex[:7]
        return f"{initials}_{random_num}"

    def to_model(self):
        from modules.client.client_schema import ClientModel

        return ClientModel.model_validate(self)

    # convert the received object into an instance of the model
    def create_db_entity(self):
        entity = self.model_dump()
        return Client(**entity)

    @classmethod
    def create_client(cls, client_data):
        from context_manager.context import get_db_session

        db: Session = get_db_session()
        # print(jsonable_encoder(client_data))
        db.add(client_data)
        db.flush()

        return client_data.to_model()

    @classmethod
    def get_by_id(cls, id):
        client = super().get_by_id(id)
        return client.to_model() if client else None

    @classmethod
    def get_by_uuid(cls, uuid):
        client = super().get_by_uuid(uuid)
        return client.to_model() if client else None
