from datetime import datetime
import pytz


def convert_to_utc(order_date, original_tz="Asia/Kolkata"):
    """
    Convert a datetime to UTC timezone.
    """
    # Check if order_date is a string and needs parsing
    if isinstance(order_date, str):
        # Parse the date from the string if it's a string
        order_date = datetime.strptime(order_date, "%Y-%m-%d %H:%M:%S")

    # Set the original timezone if it's naive
    local_tz = pytz.timezone(original_tz)
    if order_date.tzinfo is None:
        order_date_localized = local_tz.localize(order_date)
    else:
        order_date_localized = order_date

    # Convert to UTC
    order_date_utc = order_date_localized.astimezone(pytz.utc)

    return order_date_utc


def parse_datetime(datetime_str: str) -> datetime:
    """
    Parses a datetime string in IST to a UTC datetime object, supporting multiple formats.
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
