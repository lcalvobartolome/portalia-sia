"""
Pydantic schemas for request/response models.

Design principles
-----------------
Responses
- ResponseBase: every successful response inherits from this (success + message).
- ErrorResponse: standardised error envelope (documented in OpenAPI, raised by exceptions).
- Processing batch / pipeline responses use BatchProcessingResponse (job_id + status).
- Admin / exploitation responses extend ResponseBase with domain-specific fields.

Requests
- Each module adds its own fields as needed.

Author: Lorena Calvo-Bartolomé
Date: 04/02/2026
"""

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field
from enum import Enum


# ======================================================
# Enums
# ======================================================
class QueryOperator(str, Enum):
    """Query operators for Solr queries."""
    AND = "AND"
    OR = "OR"


class CorpusCapability(str, Enum):
    """
    Exploitation features that may or may not be available for a given
    corpus collection, declared per corpus in config.cf.
    """
    METADATA = "metadata"
    SEMANTIC_BY_TEXT = "semantic_by_text"
    SEMANTIC_BY_DOCUMENT = "semantic_by_document"
    INDICATORS = "indicators"
    CPV_FILTER = "cpv_filter"


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
                "message": "Extract pipeline started for tipo='insiders' (data_dir=/mnt/data_place/2025_26/metadata/test_transalte_14_abril/insiders)",
                "job_id": "extract_a1b2c3d4",
                "status": "running"
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
            "example": {"success": True, "message": "Collection 'place' created successfully", "collection": "place"}
        }


class CollectionListResponse(ResponseBase):
    """Response for listing collections."""
    collections: List[str] = []

    class Config:
        json_schema_extra = {
            "example": {"success": True, "message": None, "collections": ["place", "ted", "bdns", "place_tm_0_25_topics"]}
        }


class SolrQueryResponse(ResponseBase):
    """Response for raw Solr query execution."""
    data: List[Dict[str, Any]] = []
    num_found: int = 0

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": None,
                "data": [{"id": "https://contrataciondelestado.es/sindicacion/PlataformasAgregadasSinMenores/19192364", "title": "Suministro de equipos informáticos"}],
                "num_found": 1,
            }
        }


class CorpusListResponse(ResponseBase):
    """Response for listing corpora."""
    corpora: List[str] = []

    class Config:
        json_schema_extra = {
            "example": {"success": True, "message": None, "corpora": ["place", "ted", "bdns"]}
        }


class ModelsListResponse(ResponseBase):
    """Response for listing all models."""
    models: Dict[str, List[Dict[str, int]]] = {}

    class Config:
        json_schema_extra = {
            "example": {"success": True, "message": None, "models": ["place_tm_0_25_topics", "place_tm_0_50_topics"]}
        }


class CorpusModelsResponse(ResponseBase):
    """Response for listing models of a specific corpus."""
    models: Dict[str, List[Dict[str, int]]] = {}

    class Config:
        json_schema_extra = {
            "example": {"success": True, "message": None, "models": ["place_tm_0_25_topics", "place_tm_0_50_topics"]}
        }


class IndexingResponse(ResponseBase):
    """Response for corpus / model indexing operations."""
    job_id: Optional[str] = None
    status: str = "queued"
    documents_processed: int = 0

    class Config:
        json_schema_extra = {
            "example": {
                "success": True, "message": "Corpus 'place' indexed successfully",
                "job_id": None, "status": "completed", "documents_processed": 0
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
            "example": {
                "success": True,
                "message": None,
                "data": [{
                    "id": "https://contrataciondelestado.es/sindicacion/PlataformasAgregadasSinMenores/19192364",
                    "title": "Suministro de equipos informáticos",
                    "score": 0.95,
                }],
            }
        }


# ══════════════════════════════════════════════════════
#  REQUEST SCHEMAS
# ══════════════════════════════════════════════════════

# ======================================================
# Admin-Specific Requests
# ======================================================
class CorpusIndexRequest(BaseModel):
    """Request for indexing a corpus into Solr."""
    corpus_name: Literal["ted", "place", "bdns"] = Field(
        ..., description="Name of the corpus to index"
    )

    class Config:
        json_schema_extra = {"example": {"corpus_name": "place"}}


class CollectionCreateRequest(BaseModel):
    """Request body for creating a collection."""
    collection: str = Field(..., description="Name of the collection to create", min_length=1)

    class Config:
        json_schema_extra = {
            "example": {"collection": "my_new_collection"}
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
    - ``date``  : ``date`` (canonical, corpus-agnostic; supports year, exact timestamp, or range)
    - ``cpv``   : ``cpv_list`` (string, multivalued; prefix or exact code — only
      corpora with the 'cpv_filter' capability, e.g. 'place', have this field)
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
            "Maps to the 'date' field in the index (the canonical, "
            "corpus-agnostic date field every corpus is indexed into)."
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
            "Keys must be valid Solr field names; values are matched exactly, "
            "including case, against the field as indexed (e.g. "
            "{\"estado\": \"ADJ\", \"organo_entidad\": \"MINISTERIO DE CULTURA\"})."
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


class SimilarByDocumentRequest(SearchRequestBase):
    """
    Request for finding documents similar to one or more existing documents.

    Accepts document IDs and/or alternate identifiers as reference. When
    secondary_ids are provided, all documents matching each are resolved
    first and included as reference documents.
    """
    doc_ids: List[str] = Field(default_factory=list, description="One or more reference document IDs")
    secondary_ids: Optional[Dict[str, List[str]]] = Field(
        None,
        description=(
            "Alternate identifiers to resolve into doc_ids, keyed by the corpus-specific "
            "field name (see GET /corpora/{corpus_collection}/capabilities for the valid "
            "names per corpus), e.g. {'expediente': ['2025/180']} for 'place' or "
            "{'codigo_bdns': ['123456']} for 'bdns'."
        ),
    )
    expedientes: Optional[List[str]] = Field(
        None,
        deprecated=True,
        description=(
            "Deprecated — use secondary_ids={'expediente': [...]} instead. "
            "Kept for backward compatibility; only valid for corpora that declare "
            "'expediente' as a secondary_id_field (e.g. 'place')."
        ),
    )
    model_name: Optional[str] = Field(None, description="Topic model name (required for thematic similarity)")

    class Config:
        json_schema_extra = {
            "example": {
                "doc_ids": [
                    "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/17311447",
                    "https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/17716704",
                ],
                "secondary_ids": {"expediente": ["2025/180"]},
                "filters": {"date": "2024"},
                "pagination": {"start": 0, "rows": 10},
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


# ======================================================
# Alia Pipeline Schemas
# ======================================================
class ExtractPipelineRequest(BaseModel):
    """Parameters for the extract and infer pipeline modes."""
    base_dir: str = Field(..., description="Base directory, e.g. /mnt/data_place/2025_26")
    tipo: Literal["minors", "outsiders", "insiders"] = Field(
        ..., description="Document type to process"
    )
    calculate_on: str = Field("texto_traducido", description="Column to calculate objectives on")
    llm_model_gen: str = Field("qwen3:32b", description="LLM model for the generative step")
    embed_model: str = Field(
        "hiiamsid/sentence_similarity_spanish_es", description="Sentence-embedding model"
    )
    file_workers: int = Field(1, ge=1)
    row_workers: int = Field(4, ge=1)
    semantic_threshold: float = Field(0.6, ge=0.0, le=1.0)
    mallet: str = Field("/opt/mallet/bin/mallet", description="Path to mallet binary")
    ollama_host: str = Field("http://0.0.0.0:11434", description="Ollama host")

    class Config:
        json_schema_extra = {
            "example": {
                "base_dir": "/mnt/data_place/2025_26",
                "tipo": "insiders",
                "calculate_on": "texto_traducido",
                "llm_model_gen": "qwen3:32b",
                "embed_model": "hiiamsid/sentence_similarity_spanish_es",
                "file_workers": 1,
                "row_workers": 4,
                "semantic_threshold": 0.6,
                "mallet": "/opt/bin/mallet",
                "ollama-host": "http://0.0.0.0:11434"
            }
        }


class TrainPipelineRequest(BaseModel):
    """Parameters for the train pipeline mode."""
    base_dir: str = Field(..., description="Base directory, e.g. /mnt/data_place/2025_26")
    train_tipos: str = Field(
        ...,
        description='Colon-separated tipos to train, e.g. "minors:outsiders:insiders"',
    )
    ntopics: int = Field(25, ge=1)
    num_iterations: int = Field(1000, ge=1)
    mallet: str = Field("/opt/mallet/bin/mallet")

    class Config:
        json_schema_extra = {
            "example": {
                "base_dir": "/mnt/data_place/2025_26",
                "train_tipos": "minors:outsiders:insiders",
                "ntopics": 25,
                "num_iterations": 1000,
                "mallet": "/opt/bin/mallet",
            }
        }


class PipelineJobStatus(BaseModel):
    job_id: str
    mode: str
    tipo: Optional[str]
    status: str  # running:<step> | completed | failed
    started_at: str
    finished_at: Optional[str] = None
    returncode: Optional[int] = None
    data_dir: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "extract_a1b2c3d4",
                "mode": "extract",
                "tipo": "insiders",
                "status": "running:nlp-process",
                "started_at": "2026-04-20T10:00:00",
                "finished_at": None,
                "returncode": None,
                "data_dir": "/mnt/data_place/2025_26/metadata/test_transalte_14_abril/insiders",
            }
        }


class PipelineProgressResponse(BaseModel):
    job_id: str
    mode: str
    tipo: Optional[str]
    status: str
    started_at: str
    data_dir: Optional[str]
    files_total: int
    files_modified: int
    by_day: List[dict]

    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "extract_a1b2c3d4",
                "mode": "extract",
                "tipo": "insiders",
                "status": "running:nlp-process",
                "started_at": "2026-04-20T10:00:00",
                "data_dir": "/mnt/data_place/2025_26/metadata/test_transalte_14_abril/insiders",
                "files_total": 120,
                "files_modified": 45,
                "by_day": [
                    {"fecha": "2026-04-20", "modificados_ese_dia": 45, "acumulado": 45, "pct_sobre_total": 37.5}
                ],
            }
        }