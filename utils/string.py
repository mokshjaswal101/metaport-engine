import unicodedata
import re


def clean_text(text):
    if text is None:
        return ""
    # Normalize Unicode and replace non-breaking spaces with normal spaces
    text = unicodedata.normalize("NFKC", text).replace("\xa0", " ").strip()
    # Replace all special characters except comma and hyphen with a space
    text = re.sub(r"[^a-zA-Z0-9\s,-]", " ", text)
    # Replace multiple spaces with a single space
    return re.sub(r"\s+", " ", text).strip()


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
