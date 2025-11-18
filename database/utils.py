import uuid
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound


def get_primary_key_by_uuid(session: Session, table, uuid_value):
    try:
        primary_key_id = session.query(table).filter_by(uuid=uuid_value).one().id
        return primary_key_id

    except NoResultFound:
        return None


def get_uuid_by_primary_key(session: Session, table, primary_key_value):
    try:
        uuid_value = session.query(table).filter_by(id=primary_key_value).one().uuid
        return uuid_value

    except NoResultFound:
        return None
