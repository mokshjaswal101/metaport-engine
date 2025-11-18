from passlib.context import CryptContext
from passlib.exc import UnknownHashError


class PasswordHasher:
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    @staticmethod
    def get_password_hash(password: str) -> str:
        try:
            return PasswordHasher.pwd_context.hash(password)

        except Exception as e:
            print("Error occurred during password hashing:", e)
            return None

    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        try:
            return PasswordHasher.pwd_context.verify(plain_password, hashed_password)

        except UnknownHashError as e:
            # Handle case where the hash algorithm is unknown
            print("Unknown hash algorithm:", e)
            return False

        except Exception as e:
            # Handle other unexpected exceptions
            print("Unexpected error occurred during password verification:", e)
            return False
