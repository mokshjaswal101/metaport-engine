"""
Report Generators Package

Each generator is responsible for creating a specific type of report.
Generators are registered in the registry and invoked by the Celery worker.
"""

from .base_generator import BaseReportGenerator
from .pickup_locations_generator import PickupLocationsGenerator

# Registry of all available generators
# Add new generators here as they are implemented
GENERATOR_REGISTRY = {
    "pickup_locations": PickupLocationsGenerator,
    # Future generators:
    # "orders_mis": OrdersMISGenerator,
    # "shipment_report": ShipmentReportGenerator,
    # "ndr_report": NDRReportGenerator,
}


def get_generator(report_type: str) -> type:
    """
    Get the generator class for a report type.

    Args:
        report_type: The report type identifier

    Returns:
        The generator class, or None if not found
    """
    return GENERATOR_REGISTRY.get(report_type)


def get_available_report_types() -> list:
    """Get list of all available report types."""
    return list(GENERATOR_REGISTRY.keys())
