from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import Session, relationship
from sqlalchemy.dialects.postgresql import JSON

from database import DBBaseClass, DBBase


class User(DBBase, DBBaseClass):
    __tablename__ = "user"

    first_name = Column(String(255), nullable=False)
    last_name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    phone = Column(String(255), nullable=False)
    password_hash = Column(String(255), nullable=False)
    status = Column(
        String(20), nullable=False, default="active"
    )  # user can be inactive or active

    company_id = Column(Integer, ForeignKey("company.id"), nullable=False)
    client_id = Column(Integer, ForeignKey("client.id"), nullable=False)

    company = relationship("Company", back_populates="users")
    client = relationship("Client", back_populates="users")
    
    extra_credentials = Column(String, nullable=True)

    def __to_model(self):
        from modules.user.user_schema import UserModel

        return UserModel.model_validate(self)

    # convert the received object into an instance of the model
    def create_db_entity(self, password_hash: str):
        entity = self.model_dump()
        entity["password_hash"] = password_hash
        entity.pop("password")
        return User(**entity)

    @classmethod
    def create_user(cls, user):
        from context_manager.context import get_db_session

        db: Session = get_db_session()
        db.add(user)
        db.flush()

        return user.__to_model()

    @classmethod
    def get_by_id(cls, id):
        user = super().get_by_id(id)
        return user.__to_model() if user else None

    @classmethod
    def get_by_uuid(cls, uuid):
        user = super().get_by_uuid(uuid)
        return user.__to_model() if user else None

    @classmethod
    def get_active_user_by_email(cls, email):
        from context_manager.context import get_db_session

        db = get_db_session()
        user = (
            db.query(cls)
            .filter(
                cls.status == "active",
                cls.email == email,
                cls.is_deleted.is_(False),
            )
            .first()
        )

        return user.__to_model() if user else None

    @classmethod
    def update_user_by_uuid(cls, user_uuid: str, update_dict: dict) -> int:
        from context_manager.context import get_db_session

        db = get_db_session()
        update_query = db.query(cls).filter(
            cls.uuid == user_uuid, cls.is_deleted.is_(False)
        )

        updates = update_query.update(update_dict)
        db.flush()
        return updates
