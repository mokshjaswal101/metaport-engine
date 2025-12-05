import unicodedata
import re


def round_to_2_decimal_place(value):
    """
    Round a float to 2 decimal places.
    """
    return round(value, 2)


def clean_text(text, max_length: int = None):
    """
    Clean and normalize text.

    - Normalizes Unicode (NFKC)
    - Replaces non-breaking spaces
    - Collapses multiple spaces to single space
    - Trims whitespace
    - Optionally truncates to max_length
    """
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)
    # Normalize Unicode and replace non-breaking spaces with normal spaces
    text = unicodedata.normalize("NFKC", text).replace("\xa0", " ").strip()
    # Replace multiple spaces with a single space
    text = re.sub(r"\s+", " ", text).strip()
    # Truncate if max_length specified
    if max_length and len(text) > max_length:
        text = text[:max_length]
    return text


def clean_text_alphanumeric(text):
    """
    Clean text and remove special characters (except comma and hyphen).
    """
    if text is None:
        return ""
    # Normalize Unicode and replace non-breaking spaces with normal spaces
    text = unicodedata.normalize("NFKC", text).replace("\xa0", " ").strip()
    # Replace all special characters except comma and hyphen with a space
    text = re.sub(r"[^a-zA-Z0-9\s,-]", " ", text)
    # Replace multiple spaces with a single space
    return re.sub(r"\s+", " ", text).strip()


def clean_phone(phone: str) -> str:
    """
    Clean and normalize Indian phone number.
    """
    if not phone:
        return ""
    phone = str(phone).strip()
    # Remove +91 or 91 prefix
    if phone.startswith("+91"):
        phone = phone[3:]
    elif phone.startswith("91") and len(phone) > 10:
        phone = phone[2:]
    # Remove any non-digit characters
    phone = re.sub(r"\D", "", phone)
    # Return last 10 digits
    return phone[-10:] if len(phone) >= 10 else phone


def truncate_text(value: str, max_len: int = 15, ellipsis: str = "...") -> str:
    """
    Truncate a string to `max_len` characters and append `ellipsis` when truncated.
    """
    try:
        s = str(value) if value is not None else ""
        return s if len(s) <= max_len else s[:max_len] + (ellipsis or "")
    except Exception:
        return str(value) if value is not None else ""
