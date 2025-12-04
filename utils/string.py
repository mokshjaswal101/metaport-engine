import unicodedata
import re


def clean_text(text, max_length: int = None):
    """
    Clean and normalize text.
    
    - Normalizes Unicode (NFKC)
    - Replaces non-breaking spaces
    - Collapses multiple spaces to single space
    - Trims whitespace
    - Optionally truncates to max_length
    
    Args:
        text: Text to clean
        max_length: Optional maximum length to truncate to
        
    Returns:
        Cleaned text string
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
    
    Args:
        text: Text to clean
        
    Returns:
        Cleaned text with only alphanumeric, spaces, commas, and hyphens
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
    
    - Removes +91 or 91 country code prefix
    - Removes all non-digit characters
    - Returns last 10 digits
    
    Args:
        phone: Phone number string
        
    Returns:
        Cleaned 10-digit phone number or original if less than 10 digits
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
    phone = re.sub(r'\D', '', phone)
    # Return last 10 digits
    return phone[-10:] if len(phone) >= 10 else phone


def truncate_text(value: str, max_len: int = 15, ellipsis: str = "...") -> str:
    """
    Truncate a string to `max_len` characters and append `ellipsis` when truncated.

    Args:
        value: value to truncate (converted to string)
        max_len: maximum allowed length before truncation
        ellipsis: string to append when truncation occurs (default '...')

    Returns:
        Truncated string with ellipsis when truncated.
    """
    try:
        s = str(value) if value is not None else ""
        return s if len(s) <= max_len else s[:max_len] + (ellipsis or "")
    except Exception:
        return str(value) if value is not None else ""
