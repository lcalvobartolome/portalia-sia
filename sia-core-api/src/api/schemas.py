"""
Pydantic schemas for request/response models.

Design principles
-----------------
Responses
- ResponseBase: every successful response inherits from this (success + message).
- ErrorResponse: standardised error envelope (documented in OpenAPI, raised by exceptions).
- Processing single responses extend ResponseBase with a typed data field.
- Processing batch / pipeline responses use BatchProcessingResponse (job_id + status).
- Admin / exploitation responses extend ResponseBase with domain-specific fields.

Requests
- BatchRequestBase: common fields for every batch endpoint (parquet_path, text_column,
  id_column, output_path).  Each module adds its own fields on top.
- SingleRequestBase: common fields for every single-document endpoint (document_id, text).
  Each module adds its own fields on top.
- PipelineRequest: orchestrates a full ingestion run — picks a data source, selects
  which modules to execute (respecting dependency order), and carries the per-module
  configuration in a single payload.
- OnDemandInferenceRequest: separate from the pipeline — operates on external
  (non-indexed) documents and optionally compares against an existing index.

Author: Lorena Calvo-Bartolomé
Date: 04/02/2026
"""

import math
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field, field_validator
from enum import Enum


def _sanitize_nan(obj: Any) -> Any:
    """Recursively replace float NaN/Inf with None so JSON serialization never fails."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_nan(v) for v in obj]
    return obj


# ======================================================
# Enums
# ======================================================
class QueryOperator(str, Enum):
    """Query operators for Solr queries."""
    AND = "AND"
    OR = "OR"

class ProcessingModule(str, Enum):
    
    PDF_EXTRACTION = "pdf_extraction"
    SUMMARIZATION = "summarization"
    METADATA_ENRICHMENT = "metadata_enrichment"
    AI_REL_CLASSIFICATION = "ai_relevance_classification"
    TOPIC_MODELING = "topic_modeling"
    EMBEDDINGS = "embeddings"
    INGESTION = "ingestion"
    ALL = "all"
    

class PipelineModule(str, Enum):
    """
    Available processing modules.

    Execution order (when running the pipeline):
        download --> pdf_extraction --> summarization | metadata_enrichment |
        ai_relevance_classification | topic_modeling | embeddings --> ingestion

    Dependencies:
    - All modules from summarization onwards require pdf_extraction.
    - Note that pdf_extraction requires download (unless documents are already local).
    - ingestion runs last.
    """
    DOWNLOAD = "download"
    PDF_EXTRACTION = "pdf_extraction"
    SUMMARIZATION = "summarization"
    METADATA_ENRICHMENT = "metadata_enrichment"
    AI_REL_CLASSIFICATION = "ai_relevance_classification"
    TOPIC_MODELING = "topic_modeling"
    EMBEDDINGS = "embeddings"
    INGESTION = "ingestion"
    ALL = "all"
    
class MetadataField(str, Enum):
    """Predefined metadata fields for extraction."""
    CPV = "cpv"
    OBJECTIVE = "objective"


class DataSource(str, Enum):
    """Available data sources for ingestion."""
    PLACE = "PLACE"
    TED = "TED"
    BDNS = "BDNS"


# ══════════════════════════════════════════════════════
#  RESPONSE SCHEMAS
# ══════════════════════════════════════════════════════

# ======================================================
# Base Response
# ======================================================
class ResponseBase(BaseModel):
    """
    Base response model for ALL API endpoints.

    Every endpoint must return a schema that extends this class.
    """
    success: bool = True
    message: Optional[str] = None


class ErrorResponse(BaseModel):
    """
    Standardised error response.

    Used in the responses parameter of endpoint decorators to
    document error payloads in the OpenAPI spec.
    """
    success: bool = False
    error: str
    error_code: str
    details: Optional[Dict[str, Any]] = None

    class Config:
        json_schema_extra = {
            "example": {
                "success": False,
                "error": "Collection not found",
                "error_code": "NOT_FOUND",
                "details": {"collection": "my_collection"}
            }
        }


# ======================================================
# Processing Responses — batch / pipeline
# ======================================================
class BatchProcessingResponse(ResponseBase):
    """
    Generic response for batch and pipeline processing endpoints.

    Batch/pipeline operations save their results internally, so the
    response only confirms acceptance and provides a job_id.
    """
    job_id: Optional[str] = None
    status: str = "queued"

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Batch processing started",
                "job_id": "job_abc123",
                "status": "queued"
            }
        }


# ======================================================
# Processing Responses — single
# (Each extends ResponseBase with a typed data field)
# ======================================================
class TextExtractionSingleResponse(ResponseBase):
    """Response for single PDF text extraction."""
    data: Optional[str] = Field(None, description="Extracted (and normalized if applicable) text")

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Text extraction completed",
                "data": "El presente contrato tiene por objeto el suministro de equipos informáticos..."
            }
        }


class SummarizationSingleResponse(ResponseBase):
    """Response for single document summarisation."""
    data: Optional[str] = Field(None, description="Generated summary text")
    grounding: Optional[List[Dict[str, Any]]] = Field(
        None, description="Traceability references to original text segments"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Summary generation completed",
                "data": "Este documento describe un contrato de suministro...",
                "grounding": [{"segment": "párrafo 1", "source_start": 0, "source_end": 150}]
            }
        }


class MetadataExtractionSingleResponse(ResponseBase):
    """Response for single document metadata extraction."""
    data: Optional[Dict[str, Any]] = Field(
        None, description="Extracted metadata key-value pairs"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Metadata extraction completed",
                "data": {"organization": "Ministerio de Ciencia", "budget": "500000", "deadline": "2026-06-30"}
            }
        }


class AIRelevanceSingleResponse(ResponseBase):
    """Response for single document AI relevance classification."""
    data: Optional[str] = Field(
        None, description="Classification label (e.g. 'relevant') or score"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "AI relevance classification completed",
                "data": "relevant"
            }
        }


class TopicInferenceSingleResponse(ResponseBase):
    """Response for single document topic inference."""
    data: Optional[List[Dict[str, Any]]] = Field(
        None, description="Topic distribution: list of {id, probability} entries"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Topic inference completed",
                "data": [
                    {"id": "t0", "probability": 0.45},
                    {"id": "t1", "probability": 0.30},
                    {"id": "t2", "probability": 0.25}
                ]
            }
        }


class EmbeddingsSingleResponse(ResponseBase):
    """Response for single document embeddings generation."""
    data: Optional[List[float]] = Field(None, description="Embedding vector")

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Embeddings generation completed",
                "data": [0.0123, -0.0456, 0.0789, 0.0012]
            }
        }


class OnDemandInferenceSingleResponse(ResponseBase):
    """Response for single on-demand inference on an external document."""
    document_id: Optional[str] = None
    embeddings: Optional[List[float]] = None
    topic_distribution: Optional[List[Dict[str, Any]]] = None
    summary: Optional[str] = None
    similar_documents: Optional[List[Dict[str, Any]]] = None
    processing_time_ms: Optional[int] = None

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "On-demand inference completed",
                "document_id": "EXT-ABC12345",
                "embeddings": [0.1, 0.2, 0.3],
                "topic_distribution": [{"id": "t0", "probability": 0.4}, {"id": "t1", "probability": 0.6}],
                "summary": None,
                "similar_documents": [{"doc_id": "DOC-001", "similarity": 0.92}],
                "processing_time_ms": 340
            }
        }


# ======================================================
# Health Check Response
# ======================================================
class HealthResponse(ResponseBase):
    """Health check response including Solr status."""
    status: str = Field(..., description="Overall status: 'healthy' or 'unhealthy'")
    timestamp: str = Field(..., description="ISO timestamp of check")
    solr_connected: bool = Field(..., description="Solr connection status")
    version: str = Field("1.0.0", description="API version")

    class Config:
        json_schema_extra = {
            "example": {
                "success": True, "message": None,
                "status": "healthy", "timestamp": "2026-02-04T10:30:00Z",
                "solr_connected": True, "version": "1.0.0"
            }
        }


# ======================================================
# Infrastructure Administration Responses
# ======================================================
class CollectionResponse(ResponseBase):
    """Response for collection operations (create / delete)."""
    collection: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {"success": True, "message": "Collection 'my_collection' created successfully", "collection": "my_collection"}
        }


class CollectionListResponse(ResponseBase):
    """Response for listing collections."""
    collections: List[str] = []

    class Config:
        json_schema_extra = {
            "example": {"success": True, "message": None, "collections": ["corpus_1", "corpus_2", "model_45_10_topics"]}
        }


class SolrQueryResponse(ResponseBase):
    """Response for raw Solr query execution."""
    data: List[Dict[str, Any]] = []
    num_found: int = 0

    class Config:
        json_schema_extra = {
            "example": {"success": True, "message": None, "data": [{"id": "doc1", "title": "Document 1"}], "num_found": 1}
        }


class CorpusListResponse(ResponseBase):
    """Response for listing corpora."""
    corpora: List[str] = []

    class Config:
        json_schema_extra = {
            "example": {"success": True, "message": None, "corpora": ["procurement_2023", "procurement_2024"]}
        }


class ModelsListResponse(ResponseBase):
    """Response for listing all models."""
    models: Dict[str, List[Dict[str, int]]] = {}

    class Config:
        json_schema_extra = {
            "example": {"success": True, "message": None, "models": ["0_45_10_topics", "0_45_50_topics"]}
        }


class CorpusModelsResponse(ResponseBase):
    """Response for listing models of a specific corpus."""
    models: Dict[str, List[Dict[str, int]]] = {}

    class Config:
        json_schema_extra = {
            "example": {"success": True, "message": None, "models": ["0_45_10_topics", "0_45_50_topics"]}
        }


class MetadataFieldsResponse(ResponseBase):
    """Response for metadata fields queries."""
    fields: List[str] = []

    class Config:
        json_schema_extra = {
            "example": {"success": True, "message": None, "fields": ["title", "date", "description", "amount"]}
        }


class DisplayConfigResponse(ResponseBase):
    """Response for complete display configuration."""
    metadata_displayed: List[str] = []
    searchable_fields: List[str] = []
    active_filters: List[str] = []

    class Config:
        json_schema_extra = {
            "example": {
                "success": True, "message": None,
                "metadata_displayed": ["title", "date"],
                "searchable_fields": ["title", "description"],
                "active_filters": ["category", "year"]
            }
        }


class IndexingResponse(ResponseBase):
    """Response for corpus / model indexing operations."""
    job_id: Optional[str] = None
    status: str = "queued"
    documents_processed: int = 0

    class Config:
        json_schema_extra = {
            "example": {
                "success": True, "message": "Corpus 'procurement_2024' indexed successfully",
                "job_id": None, "status": "completed", "documents_processed": 0
            }
        }


class ProcessingJobResponse(ResponseBase):
    """Response for processing jobs (e.g. model indexing)."""
    job_id: Optional[str] = None
    status: str = "queued"
    progress: Optional[float] = None
    results: Optional[Dict[str, Any]] = None

    class Config:
        json_schema_extra = {
            "example": {
                "success": True, "message": "Model '0_45_10_topics' indexed successfully",
                "job_id": None, "status": "completed", "progress": None, "results": None
            }
        }


# ======================================================
# Exploitation Service Responses
# ======================================================
class DataResponse(ResponseBase):
    """
    Generic data response for exploitation service endpoints.

    The data field carries the query-specific payload (documents, metadata,
    topic distributions, counts, year lists, etc.).
    """
    data: Any = None

    class Config:
        json_schema_extra = {
            "example": {"success": True, "message": None, "data": [{"id": "doc1", "title": "Document 1", "score": 0.95}]}
        }


# ══════════════════════════════════════════════════════
#  REQUEST SCHEMAS
# ══════════════════════════════════════════════════════

# ======================================================
# Request Base Classes
# ======================================================

class BatchRequestBase(BaseModel):
    """
    Common fields shared by every batch processing request.

    Every batch module request inherits from this and adds its own
    module-specific parameters.
    """
    parquet_path: str = Field(..., description="Path to the parquet file containing documents")
    apply_column: str = Field("text", description="Column name containing the information to apply the processing on (e.g. text for summarization, pdf_path for text extraction)")
    id_column: str = Field("doc_id", description="Column name containing document IDs")
    output_path: Optional[str] = Field(None, description="Path to save processing results")


class SingleRequestBase(BaseModel):
    """
    Common fields shared by every single-document processing request.

    Every single-document module request inherits from this and adds its
    own module-specific parameters.

    Note: TextExtractionSingleRequest overrides these fields because
    it works with PDF files rather than plain text.
    """
    document_id: Optional[str] = Field(None, description="Document ID (if already indexed)")
    text: Optional[str] = Field(None, description="Raw text to process (if not indexed)")


# ======================================================
# Module-Specific Batch Requests
# ======================================================
class TextExtractionBatchRequest(BatchRequestBase):
    """Batch PDF text extraction from a parquet file of PDF paths."""
    apply_column: str = Field("pdf_path", description="Column name containing PDF file paths")
    normalize: bool = Field(True, description="Apply text normalisation")

    class Config:
        json_schema_extra = {
            "example": {
                "parquet_path": "/data/documents/batch_2024.parquet",
                "apply_column": "pdf_path", "id_column": "doc_id",
                "normalize": True
            }
        }


class SummarizationBatchRequest(BatchRequestBase):
    """Batch document summarisation."""
    focus_dimensions: Optional[List[str]] = Field(None, description="Focus dimensions for summary")
    include_traceability: bool = Field(True, description="Include traceability to original text")

    class Config:
        json_schema_extra = {
            "example": {
                "parquet_path": "/data/documents/batch_2024.parquet",
                "apply_column": "full_text", "id_column": "doc_id",
            }
        }


class MetadataExtractionBatchRequest(BatchRequestBase):
    """Batch automatic metadata extraction."""
    metadata_fields: List[MetadataField] = Field(..., description="Metadata fields to extract")

    class Config:
        json_schema_extra = {
            "example": {
                "parquet_path": "/data/documents/batch_2024.parquet",
                "apply_column": "full_text", "id_column": "doc_id",
                "metadata_fields": ["cpv", "objective"],
            }
        }


class AIRelevanceBatchRequest(BatchRequestBase):
    """Batch AI relevance classification."""
    output_format: str = Field("binary", description="Output format: 'binary' or 'score'")

    class Config:
        json_schema_extra = {
            "example": {
                "parquet_path": "/data/documents/batch_2024.parquet",
                "apply_column": "full_text", "id_column": "doc_id",
                "output_format": "score"
            }
        }


class TopicInferenceBatchRequest(BatchRequestBase):
    """Batch topic inference using a trained topic model."""
    model_name: str = Field(..., description="Name of the trained topic model to use")

    class Config:
        json_schema_extra = {
            "example": {
                "parquet_path": "/data/documents/batch_2024.parquet",
                "apply_column": "full_text", "id_column": "doc_id",
                "model_name": "topic_model_v1"
            }
        }


class EmbeddingsBatchRequest(BatchRequestBase):
    """Batch embeddings generation."""
    model_type: str = Field("multi-qa-mpnet-base-dot-v1", description="Embedding model type")
    batch_size: int = Field(32, ge=1, le=128, description="Batch size for processing")

    class Config:
        json_schema_extra = {
            "example": {
                "parquet_path": "/data/documents/batch_2024.parquet",
                "apply_column": "full_text", "id_column": "doc_id",
                "model_type": "multi-qa-mpnet-base-dot-v1", "batch_size": 32
            }
        }


# ======================================================
# Module-Specific Single Requests
# ======================================================
class TextExtractionSingleRequest(BaseModel):
    """
    Single PDF text extraction.

    Does NOT inherit from SingleRequestBase because it works with
    PDF files (path or base64) rather than plain text.
    """
    document_id: Optional[str] = Field(None, description="Document ID (auto-generated if not provided)")
    pdf_path: Optional[str] = Field(None, description="Path to the PDF file")
    pdf_content: Optional[str] = Field(None, description="Base64-encoded PDF content")
    normalize: bool = Field(True, description="Apply text normalisation")

    class Config:
        json_schema_extra = {
            "example": {
                "document_id": "DOC-2024-001",
                "pdf_path": "/data/pdfs/document.pdf",
                "normalize": True
            }
        }


class SummarizationSingleRequest(SingleRequestBase):
    """Single document summarisation."""
    focus_dimensions: Optional[List[str]] = Field(None, description="Focus dimensions for summary")
    include_traceability: bool = Field(True, description="Include traceability to original text")

    class Config:
        json_schema_extra = {
            "example": {
                "text": "El presente contrato tiene por objeto...",
                "focus_dimensions": ["technical"],
                "include_traceability": True
            }
        }


class MetadataExtractionSingleRequest(SingleRequestBase):
    """Single document metadata extraction."""
    metadata_fields: List[MetadataField] = Field(..., description="Metadata fields to extract")

    class Config:
        json_schema_extra = {
            "example": {
                "text": "El presente contrato tiene por objeto...",
                "metadata_fields": ["cpv", "objective"],
            }
        }


class AIRelevanceSingleRequest(SingleRequestBase):
    """Single document AI relevance classification."""
    output_format: str = Field("binary", description="Output format: 'binary' or 'score'")

    class Config:
        json_schema_extra = {
            "example": {
                "text": "Desarrollo de sistema de inteligencia artificial...",
                "output_format": "score"
            }
        }


class TopicInferenceSingleRequest(SingleRequestBase):
    """Single document topic inference."""
    text: str = Field(..., description="Text to analyse for topic distribution")
    model_name: str = Field(..., description="Name of the trained topic model to use")

    class Config:
        json_schema_extra = {
            "example": {
                "text": "El presente contrato tiene por objeto...",
                "model_name": "topic_model_v1"
            }
        }


class EmbeddingsSingleRequest(SingleRequestBase):
    """Single document embeddings generation."""
    model_type: str = Field("multi-qa-mpnet-base-dot-v1", description="Embedding model type")

    class Config:
        json_schema_extra = {
            "example": {
                "text": "El presente contrato tiene por objeto...",
                "model_type": "multi-qa-mpnet-base-dot-v1"
            }
        }


# ======================================================
# Pipeline Request (full ingestion orchestration)
# ======================================================
class PipelineModuleConfig(BaseModel):
    """
    Per-module configuration for the pipeline.

    Only the fields relevant to the selected modules need to be provided.
    Irrelevant fields are ignored.
    """
    # Download
    source: Optional[DataSource] = Field(None, description="Data source (required if download is selected)")
    start_date: Optional[str] = Field(None, description="Start date for download (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date for download (YYYY-MM-DD)")
    download_filters: Optional[Dict[str, Any]] = Field(None, description="Additional download filters")

    # Text extraction
    normalize: bool = Field(True, description="Apply text normalisation during extraction")

    # Summarisation
    focus_dimensions: Optional[List[str]] = Field(None, description="Focus dimensions for summaries")
    include_traceability: bool = Field(True, description="Include traceability in summaries")

    # Metadata extraction
    metadata_fields: Optional[List[str]] = Field(None, description="Metadata fields to extract (required if metadata_enrichment is selected)")
    metadata_validation: bool = Field(True, description="Validate extracted metadata")

    # AI relevance classification
    relevance_output_format: str = Field("binary", description="'binary' or 'score'")

    # Topic modelling
    model_name: Optional[str] = Field(None, description="Topic model name (required if topic_modeling is selected)")

    # Embeddings
    embedding_model_type: str = Field("multi-qa-mpnet-base-dot-v1", description="Embedding model type")
    embedding_batch_size: int = Field(32, ge=1, le=128, description="Batch size for embedding generation")

    class Config:
        json_schema_extra = {
            "example": {
                "source": "TED",
                "normalize": True,
                "focus_dimensions": ["technical"],
                "metadata_fields": ["organization", "budget"],
                "model_name": "topic_model_v1"
            }
        }


class PipelineRequest(BaseModel):
    """
    Request for triggering a full ingestion pipeline.

    Selects which modules to execute and provides their configuration
    in a single payload.  Modules are always executed in dependency
    order regardless of the order they appear in the modules list::

        download --> pdf_extraction --> summarization | metadata_enrichment |
        ai_relevance_classification | topic_modeling | embeddings --> ingestion

    Use PipelineModule.ALL to run every module.
    """
    modules: List[PipelineModule] = Field(
        default=[PipelineModule.ALL],
        description="Pipeline modules to execute (order is enforced automatically)"
    )
    config: PipelineModuleConfig = Field(
        default_factory=PipelineModuleConfig,
        description="Per-module configuration (only relevant fields need to be set)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "modules": ["download", "pdf_extraction", "summarization", "embeddings", "ingestion"],
                "config": {
                    "source": "TED",
                    "start_date": "2024-01-01",
                    "normalize": True,
                    "focus_dimensions": ["technical", "economic"],
                    "embedding_model_type": "sentence-transformers"
                }
            }
        }


# ======================================================
# On-Demand Inference Requests (external documents)
# ======================================================
class OnDemandInferenceBatchRequest(BatchRequestBase):
    """
    Batch on-demand inference on external (non-indexed) PDF documents.

    Processes multiple documents from a parquet file through the
    requested operations.
    """
    parquet_path: str = Field(..., description="Path to the parquet file containing documents")
    id_column: Optional[str] = Field(None, description="Column name for document IDs")
    pdf_column: str = Field(..., description="Column name containing PDF file paths or base64 content")
    operations: List[ProcessingModule] = Field(
        default=[ProcessingModule.ALL],
        description="Processing modules to apply"
    )
    model_name: Optional[str] = Field(
        None, description="Model name (required for topic inference)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "parquet_path": "/data/external_docs.parquet",
                "id_column": "doc_id",
                "pdf_column": "pdf_content",
                "operations": ["all"],
                "model_name": "my_topic_model"
            }
        }


class OnDemandInferenceSingleRequest(BaseModel):
    """
    On-demand inference on a single external (non-indexed) PDF document.

    Accepts a PDF via file path or base64-encoded content, processes it
    through the requested operations and optionally compares results
    against an existing index.
    """
    document_id: Optional[str] = Field(None, description="Document ID (auto-generated if not provided)")
    pdf_path: Optional[str] = Field(None, description="Path to the PDF file")
    pdf_content: Optional[str] = Field(None, description="Base64-encoded PDF content")
    operations: List[ProcessingModule] = Field(
        default=[ProcessingModule.ALL],
        description="Processing modules to apply"
    )
    model_name: Optional[str] = Field(
        None, description="Model name (required for topic inference)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "document_id": "EXT-001",
                "pdf_path": "/data/pdfs/external_document.pdf",
                "operations": ["all"],
                "model_name": "my_topic_model"
            }
        }

class ModelIndexRequest(BaseModel):
    """Request for indexing a topic model into Solr."""
    model_name: str = Field(..., description="Name of the model folder", min_length=1)

    class Config:
        json_schema_extra = {"example": {"model_name": "0_45_10_topics"}}


class TopicModelTrainingRequest(BaseModel):
    """Request for topic model training."""
    corpus_name: str = Field(..., description="Corpus for topic modelling")
    model_name: str = Field(..., description="Name for the model")
    num_topics: int = Field(10, ge=2, description="Number of topics")
    # other params will come here.


# ======================================================
# Admin-Specific Requests
# ======================================================
class CorpusIndexRequest(BaseModel):
    """Request for indexing a corpus into Solr."""
    corpus_name: Literal["ted", "place", "bdns"] = Field(
        ..., description="Name of the corpus to index"
    )

    class Config:
        json_schema_extra = {"example": {"corpus_name": "ted"}}
        
class CollectionCreateRequest(BaseModel):
    """Request body for creating a collection."""
    collection: str = Field(..., description="Name of the collection to create", min_length=1)
    
    class Config:
        json_schema_extra = {
            "example": {"collection": "my_new_collection"}
        }

class SearchableFieldsRequest(BaseModel):
    """Request body for modifying searchable fields."""
    searchable_fields: str = Field(
        ..., 
        description="Fields to add/remove (comma-separated)",
        min_length=1
    )
    
    class Config:
        json_schema_extra = {
            "example": {"searchable_fields": "title,description,content"}
        }
        
class SolrQueryParams(BaseModel):
    """
    Query parameters for raw Solr queries.

    Used as a dependency via Depends(SolrQueryParams) on the
    execute_raw_query endpoint.  The collection field is NOT
    included here because it is a path parameter.
    """
    q: str = Field(..., description="Query string using standard query syntax")
    q_op: Optional[QueryOperator] = Field(None, alias="q.op", description="Default operator (AND/OR)")
    fq: Optional[str] = Field(None, description="Filter query")
    sort: Optional[str] = Field(None, description="Sort order (e.g., 'field asc')")
    start: Optional[int] = Field(0, ge=0, description="Offset for pagination")
    rows: Optional[int] = Field(10, ge=1, le=1000, description="Number of rows to return")
    fl: Optional[str] = Field(None, description="Fields to return")
    df: Optional[str] = Field(None, description="Default field")

    class Config:
        populate_by_name = True
        
# ======================================================
# Exploitation Search — common filter/pagination params
# ======================================================
class MetadataFilter(BaseModel):
    """
    Optional metadata filters applicable to all search endpoints.

    Allows narrowing results by date, CPV code, or any additional
    key-value metadata.

    Field mapping to Solr:
    - ``date``  : ``updated`` (supports year, exact timestamp, or range)
    - ``cpv``   : ``cpv_list`` (string, multivalued; prefix or exact code)
    - ``extra`` : arbitrary indexed field names and their values (e.g., {"estado": "ADJ", "tender_type": "insiders"})
    """
    date: Optional[str] = Field(
        None,
        description=(
            "Filter by publication date. Accepted formats: "
            "(1) 4-digit year (e.g. '2024'), which gets expanded to a full-year range automatically; "
            "(2) exact ISO-8601 timestamp (e.g. '2024-06-15T00:00:00Z'); "
            "(3) explicit Solr range expression "
            "(e.g. '[2024-01-01T00:00:00Z TO 2024-12-31T23:59:59Z]'). "
            "Maps to the 'updated' field in the index."
        ),
    )
    cpv: Optional[str] = Field(
        None,
        description=(
            "Filter by CPV code (Common Procurement Vocabulary). "
            "Accepts an exact code (e.g. '72000000') or a prefix wildcard "
            "(e.g. '72*'). Maps to the 'cpv_list' field in the index."
        ),
    )
    extra: Optional[Dict[str, str]] = Field(
        None,
        description=(
            "Additional metadata key-value filters against any indexed field. "
            "Keys must be valid Solr field names; values are matched exactly "
            "(e.g. {\"estado\": \"ADJ\", \"tender_type\": \"insiders\"})."
        ),
    )

    class Config:
        json_schema_extra = {
            "example": {
                "date": "2025",
                "cpv": "72*",
                "extra": {"estado": "ADJ", "tender_type": "insiders"},
            }
        }


class SearchPagination(BaseModel):
    """Common pagination parameters for search endpoints."""
    start: int = Field(0, ge=0, description="Offset for pagination")
    rows: int = Field(10, ge=1, le=1000, description="Number of results to return")


class SearchRequestBase(BaseModel):
    """
    Common fields shared by all search/similarity requests.

    - ``filters``: structured metadata filters (date, CPV, extras).
    - ``pagination``: start + rows.
    """
    filters: Optional[MetadataFilter] = Field(None, description="Structured metadata filters")
    pagination: SearchPagination = Field(default_factory=SearchPagination)


class SemanticSearchByTextRequest(SearchRequestBase):
    """Request for semantic search by text query."""
    query_text: str = Field(..., description="Text to search for semantically")

    class Config:
        json_schema_extra = {
            "example": {
                "query_text": "inteligencia artificial en contratación pública",
                "filters": {"date": "2024", "cpv": "72000000"},
                "pagination": {"start": 0, "rows": 10}
            }
        }


class ThematicSearchByTextRequest(SearchRequestBase):
    """Request for thematic search by text query using topic model inference."""
    query_text: str = Field(..., description="Text to search for thematically")
    model_name: str = Field(..., description="Topic model name for inference")

    class Config:
        json_schema_extra = {
            "example": {
                "query_text": "suministro de equipos informáticos",
                "model_name": "topic_model_v1",
                "filters": {"date": "2024"},
                "pagination": {"start": 0, "rows": 10}
            }
        }

class SimilarByDocumentRequest(SearchRequestBase):
    """
    Request for finding documents similar to one or more existing documents.

    Used by both semantic (BERT) and thematic (topic model) similarity
    endpoints.  Accepts a list of document IDs to enable multi-document
    similarity queries.
    """
    doc_ids: List[str] = Field(..., description="One or more reference document IDs", min_length=1)
    model_name: Optional[str] = Field(None, description="Topic model name (required for thematic similarity)")

    class Config:
        json_schema_extra = {
            "example": {
                "doc_ids": ["DOC-2024-001", "DOC-2024-042"],
                "model_name": "topic_model_v1",
                "filters": {"date": "2024"},
                "pagination": {"start": 0, "rows": 10}
            }
        }


class TemporalSearchRequest(SearchRequestBase):
    """Request for retrieving documents by year with metadata filters."""
    year: int = Field(..., description="Publication year", ge=1900, le=2100)

    class Config:
        json_schema_extra = {
            "example": {
                "year": 2024,
                "filters": {"cpv": "72000000"},
                "pagination": {"start": 0, "rows": 20}
            }
        }
        
class IndicatorRequest(BaseModel):
    date_start:   str          = Field("2025-01-01T00:00:00Z", example="2025-01-01T00:00:00Z")
    date_end:     str          = Field("2026-01-01T00:00:00Z", example="2026-01-01T00:00:00Z")
    date_field:   str          = Field("updated", example="updated")
    tender_type:  str | None   = Field(None, example="minors")
    cpv_prefixes: list[str] | None = Field(None, example=["72", "48"])
    budget_min:   float | None = Field(None, example=None)
    budget_max:   float | None = Field(None, example=None)
    subentidad:   str | None   = Field(None, example=None)
    cod_subentidad: str | None = Field(None, example=None)
    organo_id:    str | None   = Field(None, example=None)
    topic_model:  str | None   = Field(None, example=None)
    topic_id:     str | None   = Field(None, example=None)
    topic_min_weight: float | None = Field(None, example=None)