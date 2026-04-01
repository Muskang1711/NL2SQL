from pydantic import BaseModel
from typing import Optional
from enum import Enum


class ProcessingStatus(str, Enum):
    PENDING = "pending"
    ANALYZING = "analyzing"
    INGESTING = "ingesting"
    DETECTING = "detecting"
    COMPLETED = "completed"
    FAILED = "failed"


class FileUploadResponse(BaseModel):
    session_id: str
    filename: str
    sheets_found: list[str]
    status: ProcessingStatus


class SheetAnalysis(BaseModel):
    sheet_name: str
    row_count: int
    column_count: int
    skip_rows: int
    columns: dict
    issues: list[str]
    pk_candidates: list[str]
    fk_candidates: list[str]


class FKRelationship(BaseModel):
    fk_table: str
    fk_column: str
    ref_table: str
    ref_column: str
    match_percentage: float
    orphan_count: int
    status: str


class PipelineResult(BaseModel):
    tables: list[str]
    pk_candidates: dict
    confirmed_fks: list[FKRelationship]
    rejected_fks: list[FKRelationship]
    alter_statements: list[str]
    summary: list[str]


class AnalysisResponse(BaseModel):
    session_id: str
    sheets: list[SheetAnalysis]
    status: ProcessingStatus


class RelationshipInfo(BaseModel):
    table1: str
    column1: str
    table2: str
    column2: str
    relationship_type: str


class IngestResponse(BaseModel):
    session_id: str
    tables_created: list[str]
    status: ProcessingStatus