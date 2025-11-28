"""
Base Report Generator

Abstract base class for all report generators. Each specific report type
should inherit from this class and implement the required methods.
"""

import csv
import io
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from sqlalchemy.orm import Session

from logger import logger


class ReportGenerationResult:
    """Result object returned by report generators."""

    def __init__(
        self,
        success: bool,
        content: bytes = None,
        records_count: int = 0,
        file_name: str = None,
        error_message: str = None,
    ):
        self.success = success
        self.content = content
        self.records_count = records_count
        self.file_name = file_name
        self.error_message = error_message


class BaseReportGenerator(ABC):
    """
    Abstract base class for report generators.

    Each report type should implement:
    - report_type: The identifier for this report type
    - report_name: Human-readable name
    - get_headers(): Return CSV column headers
    - get_data(): Fetch and return data rows
    """

    # Must be overridden in subclasses
    report_type: str = None
    report_name: str = None

    def __init__(
        self,
        db_session: Session,
        client_id: int,
        company_id: int,
        filters: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the generator.

        Args:
            db_session: Database session for queries
            client_id: Client ID for data filtering
            company_id: Company ID for data filtering
            filters: Optional filters (date range, status, etc.)
        """
        self.db_session = db_session
        self.client_id = client_id
        self.company_id = company_id
        self.filters = filters or {}

    @abstractmethod
    def get_headers(self) -> List[str]:
        """
        Return the column headers for the CSV.

        Returns:
            List of column header strings
        """
        pass

    @abstractmethod
    def get_data(self) -> Tuple[List[List[Any]], int]:
        """
        Fetch and return the data rows for the report.

        Returns:
            Tuple of (data rows, total count)
            Each row is a list of values matching the headers
        """
        pass

    def generate_file_name(self) -> str:
        """Generate a unique file name for the report."""
        timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
        return f"{self.report_type}_{timestamp}.csv"

    def generate(self) -> ReportGenerationResult:
        """
        Generate the report and return the result.

        This method orchestrates the report generation:
        1. Get headers
        2. Fetch data
        3. Build CSV content
        4. Return result

        Returns:
            ReportGenerationResult with success status and content
        """
        try:
            logger.info(
                msg=f"Starting report generation: {self.report_type} for client {self.client_id}"
            )

            # Get headers
            headers = self.get_headers()
            if not headers:
                return ReportGenerationResult(
                    success=False, error_message="Failed to get report headers"
                )

            # Get data
            rows, records_count = self.get_data()

            logger.info(
                msg=f"Report {self.report_type}: fetched {records_count} records"
            )

            # Build CSV content
            output = io.StringIO()
            writer = csv.writer(output)

            # Write header row
            writer.writerow(headers)

            # Write data rows
            for row in rows:
                writer.writerow(row)

            # Get content as bytes
            csv_content = output.getvalue()
            output.close()

            content_bytes = csv_content.encode("utf-8")

            # Generate file name
            file_name = self.generate_file_name()

            logger.info(
                msg=f"Report {self.report_type}: generated successfully, size={len(content_bytes)} bytes"
            )

            return ReportGenerationResult(
                success=True,
                content=content_bytes,
                records_count=records_count,
                file_name=file_name,
            )

        except Exception as e:
            logger.error(
                msg=f"Report generation failed: {self.report_type}, error={str(e)}"
            )
            return ReportGenerationResult(success=False, error_message=str(e))

    def apply_date_filter(
        self,
        query,
        date_column,
        date_from_key: str = "date_from",
        date_to_key: str = "date_to",
    ):
        """
        Helper method to apply date range filter to a query.

        Args:
            query: SQLAlchemy query object
            date_column: The column to filter on
            date_from_key: Key in filters for start date
            date_to_key: Key in filters for end date

        Returns:
            Modified query with date filters applied
        """
        date_from = self.filters.get(date_from_key)
        date_to = self.filters.get(date_to_key)

        if date_from:
            try:
                if isinstance(date_from, str):
                    date_from = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
                query = query.filter(date_column >= date_from)
            except (ValueError, TypeError):
                pass

        if date_to:
            try:
                if isinstance(date_to, str):
                    date_to = datetime.fromisoformat(date_to.replace("Z", "+00:00"))
                query = query.filter(date_column <= date_to)
            except (ValueError, TypeError):
                pass

        return query
