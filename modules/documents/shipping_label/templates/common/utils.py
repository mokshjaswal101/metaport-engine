"""
Common utilities for shipping label generation
"""

import io
import base64
import requests
import barcode
from barcode.writer import ImageWriter
from logger import logger
import os
import re
from utils.string import truncate_text


def generate_barcode_image(data):
    """Generate barcode image from data and return as BytesIO buffer"""
    buffer = io.BytesIO()
    barcode_class = barcode.get_barcode_class("code128")

    writer_options = {
        "module_width": 0.4,
        "module_height": 15,
        "quiet_zone": 0,
        "font_size": 1,
        "text_distance": 6,
        "dpi": 300,
        "write_text": False,
    }

    my_barcode = barcode_class(data, writer=ImageWriter())
    my_barcode.write(buffer, options=writer_options)
    buffer.seek(0)
    return buffer


def convert_image_to_base64(image_buffer):
    """Convert image buffer to base64 string"""
    return base64.b64encode(image_buffer.getvalue()).decode("utf-8")


def get_base64_from_s3_url(url):
    """Fetch image from S3 URL and convert to base64"""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        encoded_string = base64.b64encode(response.content).decode("utf-8")
        return f"data:image/png;base64,{encoded_string}"
    except requests.RequestException as e:
        logger.error(f"Error fetching image from URL: {url}, error: {e}")
        return None


# Courier partner to logo file mapping
COURIER_LOGO_MAPPING = {
    "bluedart": "bluedart.png",
    "bluedart-air": "bluedart.png",
    "bluedart 1kg": "bluedart.png",
    "bluedart 2kg": "bluedart.png",
    "delhivery": "delhivery_logo.png",
    "delhivery-air": "delhivery_logo.png",
    "delhivery 1kg": "delhivery_logo.png",
    "delhivery 2kg": "delhivery_logo.png",
    "delhivery 3kg": "delhivery_logo.png",
    "delhivery 5kg": "delhivery_logo.png",
    "delhivery 10kg": "delhivery_logo.png",
    "delhivery 15kg": "delhivery_logo.png",
    "delhivery 20kg": "delhivery_logo.png",
    "ekart": "ekart.jpg",
    "ecom-express": "ecom.png",
    "ecom-express 1kg": "ecom.png",
    "ecom-express 2kg": "ecom.png",
    "ecom-express 5kg": "ecom.png",
    "ecom-express 10kg": "ecom.png",
    "xpressbees": "xpressbees.png",
    "xpressbees 1kg": "xpressbees.png",
    "xpressbees 2kg": "xpressbees.png",
    "xpressbees 5kg": "xpressbees.png",
    "xpressbees 10kg": "xpressbees.png",
    "xpressbees 15kg": "xpressbees.png",
    "xpressbees 20kg": "xpressbees.png",
    "dtdc": "dtdc.png",
    "dtdc-air": "dtdc.png",
    "dtdc 5kg": "dtdc.png",
    "dtdc 1kg": "dtdc.png",
    "dtdc 3kg": "dtdc.png",
    "shadowfax": "shadowfax.png",
    "amazon": "amazon.png",
    "amazon 1kg": "amazon.png",
    "amazon 2kg": "amazon.png",
    "amazon 5kg": "amazon.png",
    "amazon 10kg": "amazon.png",
    "amazon 15kg": "amazon.png",
    "amazon 20kg": "amazon.png",
}


def get_courier_logo_base64(partner):
    """Get courier logo as base64 string"""
    logo_filename = COURIER_LOGO_MAPPING.get(partner)
    if not logo_filename:
        return None

    file_path = os.path.join(os.getcwd(), "courier_logo", logo_filename)

    try:
        with open(file_path, "rb") as f:
            base64_code = base64.b64encode(f.read())
            return base64_code.decode("utf-8")
    except FileNotFoundError:
        logger.error(f"Logo file not found: {file_path}")
        return None


def sanitize_product_name(product_name, max_length=64):
    """Sanitize and truncate product name"""
    words_to_remove = ["panty", "panties"]
    pattern = r"\b(" + "|".join(map(re.escape, words_to_remove)) + r")\b"
    sanitized_name = re.sub(pattern, "", product_name, flags=re.IGNORECASE).strip()

    return truncate_text(sanitized_name, max_length)
