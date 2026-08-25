"""Dataset service for TCRM Toolkit."""

import asyncio
import base64
import json
import math
import structlog
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator

import pandas as pd

from tcrm_toolkit.core.client import SalesforceClient
from tcrm_toolkit.core.config import Settings, get_settings
from tcrm_toolkit.core.exceptions import DatasetError, UploadError
from tcrm_toolkit.core.models import (
    Dataset,
    DatasetListResponse,
    DatasetXMD,
    ExtractionJob,
    UploadJob,
)

logger = structlog.get_logger(__name__)


@dataclass
class ExtractionProgress:
    """Progress information for dataset extraction."""
    total_rows: int
    processed_rows: int
    current_chunk: int
    total_chunks: int
    status: str


@dataclass
class UploadProgress:
    """Progress information for CSV upload."""
    total_rows: int
    uploaded_rows: int
    current_part: int
    total_parts: int
    status: str


class DatasetService:
    """Service for dataset operations."""

    def __init__(
        self,
        client: SalesforceClient,
        settings: Settings | None = None,
    ):
        """Initialize the dataset service."""
        self.client = client
        self.settings = settings or get_settings()

    # =========================================================================
    # Listing and Retrieval
    # =========================================================================

    async def list_datasets(
        self,
        page_size: int = 50,
        sort: str = "Mru",
    ) -> list[Dataset]:
        """List all datasets with pagination."""
        all_datasets = []
        page_token = None

        while True:
            response = await self.client.list_datasets(
                page_size=page_size,
                sort=sort,
                page_token=page_token,
            )
            data = DatasetListResponse(**response)
            all_datasets.extend(data.datasets)

            if not data.next_page_url:
                break

            # Extract page token from next_page_url
            page_token = self._extract_page_token(data.next_page_url)

        return all_datasets

    async def get_dataset(self, dataset_id: str) -> Dataset:
        """Get dataset details by ID."""
        response = await self.client.get_dataset(dataset_id)
        return Dataset(**response)

    async def get_dataset_xmd(self, dataset_id: str, version_id: str) -> DatasetXMD:
        """Get dataset XMD (Extended Metadata)."""
        response = await self.client.get_dataset_xmd(dataset_id, version_id)
        return DatasetXMD(**response)

    async def get_dataset_dependencies(self, dataset_id: str) -> list[dict[str, Any]]:
        """Get dataset dependencies (downstream dataflows/dashboards)."""
        response = await self.client.get_dataset_dependencies(dataset_id)
        return response.get("dependencies", [])

    # =========================================================================
    # Extraction
    # =========================================================================

    def _calculate_chunk_size(self, total_rows: int) -> int:
        """Calculate optimal chunk size based on row count."""
        if total_rows <= 5_000_000:
            return 50_000
        return 150_000

    def _build_saql_query(
        self,
        dataset_id: str,
        version_id: str,
        fields: list[str],
        offset: int = 0,
        limit: int | None = None,
    ) -> str:
        """Build SAQL query for dataset extraction."""
        fields_str = ", ".join(f"'{f}'" for f in fields)
        query = f"q = load \"{dataset_id}/{version_id}\"; q = foreach q generate {fields_str};"
        if offset > 0:
            query += f" q = skip q {offset};"
        if limit:
            query += f" q = limit q {limit};"
        return query

    def _extract_fields_from_xmd(self, xmd: DatasetXMD) -> list[str]:
        """Extract field names from XMD, excluding system fields."""
        fields = []

        # Add measures (excluding _epoch fields)
        for measure in xmd.measures:
            field = measure.get("field", "")
            if field and not field.endswith("_epoch"):
                fields.append(field)

        # Add dimensions (excluding date granularity fields)
        excluded_suffixes = [
            "_Second", "_Minute", "_Hour", "_Day", "_Week",
            "_Month", "_Quarter", "_Year", "_epoch",
        ]
        for dimension in xmd.dimensions:
            field = dimension.get("field", "")
            if field and not any(field.endswith(s) for s in excluded_suffixes):
                fields.append(field)

        return fields

    async def get_row_count(self, dataset_id: str, version_id: str) -> int:
        """Get total row count for a dataset using SAQL."""
        saql = f'q = load "{dataset_id}/{version_id}"; q = group q by all; q = foreach q generate count() as "count"; q = limit q 1;'
        response = await self.client.saql_query(saql)
        records = response.get("results", {}).get("records", [])
        if records:
            return records[0].get("count", 0)
        return 0

    async def extract_dataset(
        self,
        dataset_id: str,
        output_path: Path,
        progress_callback: callable | None = None,
    ) -> ExtractionJob:
        """Extract dataset to CSV file with progress tracking."""
        # Get dataset info
        dataset = await self.get_dataset(dataset_id)
        version_id = dataset.current_version_id
        if not version_id:
            raise DatasetError(f"Dataset {dataset_id} has no current version")

        # Get XMD to determine fields
        xmd = await self.get_dataset_xmd(dataset_id, version_id)
        fields = self._extract_fields_from_xmd(xmd)

        if not fields:
            raise DatasetError("No valid fields found in dataset XMD")

        # Get total row count
        total_rows = await self.get_row_count(dataset_id, version_id)
        if total_rows == 0:
            logger.warning("dataset_empty", dataset_id=dataset_id)
            # Create empty CSV with headers
            df = pd.DataFrame(columns=fields)
            df.to_csv(output_path, index=False)
            return ExtractionJob(
                id=f"extract-{dataset_id}",
                dataset_id=dataset_id,
                status="completed",
                total_rows=0,
                processed_rows=0,
                current_chunk=0,
                total_chunks=0,
                result_path=str(output_path),
            )

        # Calculate chunking
        chunk_size = self._calculate_chunk_size(total_rows)
        total_chunks = math.ceil(total_rows / chunk_size)

        logger.info(
            "extraction_started",
            dataset_id=dataset_id,
            total_rows=total_rows,
            chunk_size=chunk_size,
            total_chunks=total_chunks,
        )

        # Create job record
        job = ExtractionJob(
            id=f"extract-{dataset_id}",
            dataset_id=dataset_id,
            status="running",
            total_rows=total_rows,
            total_chunks=total_chunks,
        )

        # Extract in chunks
        all_chunks = []
        for chunk_num in range(total_chunks):
            offset = chunk_num * chunk_size
            saql = self._build_saql_query(
                dataset_id, version_id, fields, offset, chunk_size
            )

            response = await self.client.saql_query(saql)
            records = response.get("results", {}).get("records", [])

            if records:
                chunk_df = pd.DataFrame(records)
                all_chunks.append(chunk_df)

            job.processed_rows += len(records)
            job.current_chunk = chunk_num + 1

            if progress_callback:
                progress = ExtractionProgress(
                    total_rows=total_rows,
                    processed_rows=job.processed_rows,
                    current_chunk=job.current_chunk,
                    total_chunks=total_chunks,
                    status="running",
                )
                await progress_callback(progress)

        # Combine and save
        if all_chunks:
            combined_df = pd.concat(all_chunks, ignore_index=True)
            combined_df.to_csv(output_path, index=False)
        else:
            # Empty dataset
            pd.DataFrame(columns=fields).to_csv(output_path, index=False)

        job.status = "completed"
        job.result_path = str(output_path)
        job.completed_at = pd.Timestamp.utcnow().to_pydatetime()

        logger.info(
            "extraction_completed",
            dataset_id=dataset_id,
            rows=job.processed_rows,
            output_path=str(output_path),
        )

        return job

    async def extract_dataset_streaming(
        self,
        dataset_id: str,
        output_path: Path,
        progress_callback: callable | None = None,
    ) -> AsyncIterator[ExtractionProgress]:
        """Extract dataset to CSV with streaming progress updates."""
        dataset = await self.get_dataset(dataset_id)
        version_id = dataset.current_version_id
        if not version_id:
            raise DatasetError(f"Dataset {dataset_id} has no current version")

        xmd = await self.get_dataset_xmd(dataset_id, version_id)
        fields = self._extract_fields_from_xmd(xmd)

        if not fields:
            raise DatasetError("No valid fields found in dataset XMD")

        total_rows = await self.get_row_count(dataset_id, version_id)
        if total_rows == 0:
            pd.DataFrame(columns=fields).to_csv(output_path, index=False)
            yield ExtractionProgress(0, 0, 0, 0, "completed")
            return

        chunk_size = self._calculate_chunk_size(total_rows)
        total_chunks = math.ceil(total_rows / chunk_size)

        # Write header first
        header_written = False

        for chunk_num in range(total_chunks):
            offset = chunk_num * chunk_size
            saql = self._build_saql_query(
                dataset_id, version_id, fields, offset, chunk_size
            )

            response = await self.client.saql_query(saql)
            records = response.get("results", {}).get("records", [])

            if records:
                chunk_df = pd.DataFrame(records)
                # Write chunk to CSV (append mode after first chunk)
                chunk_df.to_csv(
                    output_path,
                    mode="a" if header_written else "w",
                    header=not header_written,
                    index=False,
                )
                header_written = True

            processed = min((chunk_num + 1) * chunk_size, total_rows)

            progress = ExtractionProgress(
                total_rows=total_rows,
                processed_rows=processed,
                current_chunk=chunk_num + 1,
                total_chunks=total_chunks,
                status="running",
            )
            yield progress

            if progress_callback:
                await progress_callback(progress)

        yield ExtractionProgress(
            total_rows=total_rows,
            processed_rows=total_rows,
            current_chunk=total_chunks,
            total_chunks=total_chunks,
            status="completed",
        )

    # =========================================================================
    # CSV Upload
    # =========================================================================

    def _generate_metadata_json(self, df: pd.DataFrame, dataset_name: str) -> str:
        """Generate metadata JSON for CSV upload."""
        fields = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            if dtype in ("int64", "int32", "int16", "int8"):
                field_type = "Numeric"
            elif dtype in ("float64", "float32"):
                field_type = "Numeric"
            elif dtype == "bool":
                field_type = "Text"  # Boolean as text
            elif dtype.startswith("datetime"):
                field_type = "Date"
            else:
                field_type = "Text"

            fields.append({
                "name": col,
                "label": col.replace("_", " ").title(),
                "type": field_type,
                "isNullable": True,
            })

        metadata = {
            "format": "Csv",
            "edgemartAlias": dataset_name,
            "fields": fields,
        }
        return base64.b64encode(json.dumps(metadata).encode()).decode()

    async def upload_csv(
        self,
        dataset_id: str,
        file_path: Path,
        dataset_name: str | None = None,
        operation: str = "Overwrite",
        chunk_size: int = 50000,
        progress_callback: callable | None = None,
    ) -> UploadJob:
        """Upload CSV file to dataset with chunked multipart upload."""
        if not file_path.exists():
            raise UploadError(f"File not found: {file_path}")

        # Get dataset info if not provided
        if not dataset_name:
            dataset = await self.get_dataset(dataset_id)
            dataset_name = dataset.name

        # Read first chunk to generate metadata
        first_chunk = pd.read_csv(file_path, nrows=1)
        metadata_json = self._generate_metadata_json(first_chunk, dataset_name)

        # Create external data job
        job_response = await self.client.create_insights_external_data(
            edgemart_alias=dataset_name,
            metadata_json=metadata_json,
            operation=operation,
        )
        external_data_id = job_response["id"]

        # Count total rows
        total_rows = sum(1 for _ in open(file_path)) - 1  # Subtract header
        total_parts = math.ceil(total_rows / chunk_size)

        job = UploadJob(
            id=f"upload-{dataset_id}",
            dataset_id=dataset_id,
            edgemart_alias=dataset_name,
            file_path=str(file_path),
            operation=operation,
            status="uploading",
            total_rows=total_rows,
            total_parts=total_parts,
            external_data_id=external_data_id,
        )

        logger.info(
            "upload_started",
            dataset_id=dataset_id,
            external_data_id=external_data_id,
            total_rows=total_rows,
            total_parts=total_parts,
        )

        # Upload in chunks
        part_number = 1
        uploaded_rows = 0

        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            csv_data = chunk.to_csv(index=False)
            data_file_base64 = base64.b64encode(csv_data.encode()).decode()

            await self.client.upload_insights_external_data_part(
                external_data_id=external_data_id,
                part_number=part_number,
                data_file_base64=data_file_base64,
            )

            uploaded_rows += len(chunk)
            job.uploaded_rows = uploaded_rows
            job.current_part = part_number

            if progress_callback:
                progress = UploadProgress(
                    total_rows=total_rows,
                    uploaded_rows=uploaded_rows,
                    current_part=part_number,
                    total_parts=total_parts,
                    status="uploading",
                )
                await progress_callback(progress)

            part_number += 1

        # Process the data
        job.status = "processing"
        if progress_callback:
            await progress_callback(UploadProgress(
                total_rows=total_rows,
                uploaded_rows=uploaded_rows,
                current_part=total_parts,
                total_parts=total_parts,
                status="processing",
            ))

        result = await self.client.process_insights_external_data(external_data_id)

        job.status = "completed"
        job.completed_at = pd.Timestamp.utcnow().to_pydatetime()

        logger.info(
            "upload_completed",
            dataset_id=dataset_id,
            external_data_id=external_data_id,
            rows=uploaded_rows,
        )

        return job

    async def upload_csv_streaming(
        self,
        dataset_id: str,
        file_path: Path,
        dataset_name: str | None = None,
        operation: str = "Overwrite",
        chunk_size: int = 50000,
    ) -> AsyncIterator[UploadProgress]:
        """Upload CSV file with streaming progress updates."""
        if not file_path.exists():
            raise UploadError(f"File not found: {file_path}")

        if not dataset_name:
            dataset = await self.get_dataset(dataset_id)
            dataset_name = dataset.name

        first_chunk = pd.read_csv(file_path, nrows=1)
        metadata_json = self._generate_metadata_json(first_chunk, dataset_name)

        job_response = await self.client.create_insights_external_data(
            edgemart_alias=dataset_name,
            metadata_json=metadata_json,
            operation=operation,
        )
        external_data_id = job_response["id"]

        total_rows = sum(1 for _ in open(file_path)) - 1
        total_parts = math.ceil(total_rows / chunk_size)

        part_number = 1
        uploaded_rows = 0

        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            csv_data = chunk.to_csv(index=False)
            data_file_base64 = base64.b64encode(csv_data.encode()).decode()

            await self.client.upload_insights_external_data_part(
                external_data_id=external_data_id,
                part_number=part_number,
                data_file_base64=data_file_base64,
            )

            uploaded_rows += len(chunk)

            yield UploadProgress(
                total_rows=total_rows,
                uploaded_rows=uploaded_rows,
                current_part=part_number,
                total_parts=total_parts,
                status="uploading",
            )

            part_number += 1

        yield UploadProgress(
            total_rows=total_rows,
            uploaded_rows=uploaded_rows,
            current_part=total_parts,
            total_parts=total_parts,
            status="processing",
        )

        result = await self.client.process_insights_external_data(external_data_id)

        yield UploadProgress(
            total_rows=total_rows,
            uploaded_rows=uploaded_rows,
            current_part=total_parts,
            total_parts=total_parts,
            status="completed",
        )

    # =========================================================================
    # Deletion
    # =========================================================================

    async def delete_dataset(self, dataset_id: str) -> bool:
        """Delete a dataset."""
        await self.client.delete_dataset(dataset_id)
        return True

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _extract_page_token(self, url: str) -> str | None:
        """Extract page token from next page URL."""
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        return params.get("pageToken", [None])[0]


from urllib.parse import urlparse, parse_qs