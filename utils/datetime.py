from datetime import datetime
import pytz


def parse_datetime(datetime_str: str) -> datetime:
    """
    Parses a datetime string in IST to a UTC datetime object, supporting multiple formats.

    Args:
        datetime_str (str): The datetime string to parse.

    Returns:
        datetime: The parsed and converted UTC datetime object.

    Raises:
        ValueError: If the datetime string does not match any known format.
    """
    date_formats = [
        "%d-%m-%Y %H:%M:%S",  # With seconds
        "%d-%m-%Y %H:%M",  # Without seconds
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y %H:%M:%S.%f",  # With milliseconds
        "%d %m %Y %H:%M:%S",
    ]

    # Timezone for IST
    ist_tz = pytz.timezone("Asia/Kolkata")
    utc_tz = pytz.utc

    for fmt in date_formats:
        try:
            # Parse the datetime string as naive datetime
            ist_datetime = datetime.strptime(datetime_str, fmt)

            # Localize the naive datetime to IST and convert to UTC
            # ist_datetime = ist_tz.localize(ist_datetime)
            return ist_datetime.astimezone(ist_tz)
        except ValueError:
            continue  # Try the next format if this one fails

    # If no format matched, log and raise an error
    # logger.error(f"Failed to parse datetime: {datetime_str}")
    raise ValueError(f"Invalid datetime format: {datetime_str}")


def convert_ist_to_utc(input_time):
    # Define the IST timezone
    ist = pytz.timezone("Asia/Kolkata")

    # If input is a string, parse it into a datetime object
    if isinstance(input_time, str):
        # Assuming the format of the input string is something like "YYYY-MM-DD HH:MM:SS"
        input_time = datetime.strptime(input_time, "%Y-%m-%d %H:%M:%S")
        # Localize the datetime to IST
        input_time = ist.localize(input_time)
    # If the input is already a datetime, we assume it's naive or in IST timezone
    elif isinstance(input_time, datetime):
        if input_time.tzinfo is None:
            # Localize naive datetime to IST
            input_time = ist.localize(input_time)

    # Convert the IST time to UTC
    utc_time = input_time.astimezone(pytz.utc)

    return utc_time
