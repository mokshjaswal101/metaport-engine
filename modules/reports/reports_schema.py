"""
Reports Module - Pydantic Schemas

Defines request/response schemas for report generation and management.
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field

from models.report_job import ReportType, ReportFormat, ReportJobStatus


class GenerateReportRequest(BaseModel):
    """Request schema for generating a new report."""

    report_type: str = Field(
        ..., description="Type of report to generate", examples=["pickup_locations"]
    )
    report_format: str = Field(
        default=ReportFormat.CSV.value, description="Output format for the report"
    )
    filters: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional filters for the report (date range, status, etc.)",
    )

    class Config:
        json_schema_extra = {
            "example": {
                "report_type": "pickup_locations",
                "report_format": "csv",
                "filters": {
                    "status": "active",
                    "date_from": "2024-01-01",
                    "date_to": "2024-12-31",
                },
            }
        }


class ReportJobResponse(BaseModel):
    """Response schema for a single report job."""

    id: int
    report_type: str
    report_name: str
    report_format: str
    status: str
    progress_percentage: int
    records_count: Optional[int]
    file_name: Optional[str]
    file_size_bytes: Optional[int]
    created_at: Optional[str]
    completed_at: Optional[str]
    duration_seconds: Optional[int]
    expires_at: Optional[str]
    error_message: Optional[str]
    retry_count: int = 0


class ReportJobListItem(BaseModel):
    """Minimal response schema for downloads list."""

    id: int
    report_type: str
    report_name: str
    report_format: str
    status: str
    records_count: Optional[int]
    file_size_bytes: Optional[int]
    created_at: Optional[str]
    completed_at: Optional[str]
    error_message: Optional[str] = None


class PaginationInfo(BaseModel):
    """Pagination metadata."""

    page: int
    page_size: int
    total_count: int
    total_pages: int
    has_next: bool
    has_prev: bool


class DownloadsListResponse(BaseModel):
    """Response schema for paginated downloads list."""

    downloads: List[ReportJobListItem]
    pagination: PaginationInfo


class GenerateReportResponse(BaseModel):
    """Response schema for report generation request."""

    job_id: int
    message: str


class DownloadUrlResponse(BaseModel):
    """Response schema for download URL request."""

    download_url: str
    file_name: str
    expires_in_seconds: int = 900  # 15 minutes


class ReportTypeInfo(BaseModel):
    """Information about an available report type."""

    type: str
    name: str
    description: str
    formats: List[str]


class AvailableReportsResponse(BaseModel):
    """Response schema for available report types."""

    reports: List[ReportTypeInfo]


# Report type metadata for the API
REPORT_TYPE_METADATA = {
    ReportType.PICKUP_LOCATIONS.value: {
        "name": "Pickup Locations Summary",
        "description": "Export all pickup locations with address, contact, and status information",
        "formats": [ReportFormat.CSV.value],
    },
    # Future report types:
    # ReportType.ORDERS_MIS.value: {
    #     "name": "Orders MIS Report",
    #     "description": "Comprehensive order data with shipment details",
    #     "formats": [ReportFormat.CSV.value],
    # },
}


def get_report_name(report_type: str) -> str:
    """Get human-readable name for a report type."""
    metadata = REPORT_TYPE_METADATA.get(report_type, {})
    return metadata.get("name", report_type.replace("_", " ").title())


def get_available_reports() -> List[ReportTypeInfo]:
    """Get list of all available report types."""
    reports = []
    for report_type, metadata in REPORT_TYPE_METADATA.items():
        reports.append(
            ReportTypeInfo(
                type=report_type,
                name=metadata["name"],
                description=metadata["description"],
                formats=metadata["formats"],
            )
        )
    return reports


def validate_report_type(report_type: str) -> bool:
    """Check if a report type is valid and available."""
    return report_type in REPORT_TYPE_METADATA
