"""
Exploitation services (Service APIs)

Set of services for public and mass consumption by the PortalIA.

It integrates the following functionalities:

- Multimodal search: exact metadata queries, thematic search, semantic search
- Calculation of indicators
- Recommendation services

Response conventions:
- All responses extend ResponseBase (success + message).
- Query endpoints use DataResponse (success + message + data).
- Error responses use ErrorResponse.

Author: Lorena Calvo-Bartolome
Date: 27/03/2023
Modified: 04/02/2026 (Migrated to FastAPI and reorganized)
"""

from fastapi import APIRouter, Body, Path, Request  # type: ignore

from src.api.schemas import (
    DataResponse,
    IndicatorRequest,
    # Search request schemas
    SemanticSearchByTextRequest,
    ThematicSearchByTextRequest,
    SimilarByDocumentRequest,
    #TemporalSearchRequest,
    MetadataFilter,
)
from src.api.exceptions import (
    APIException,
    SolrException,
    NotFoundException,
    ValidationException,
    error_responses,
)

router = APIRouter(
    prefix="/exploitation",
    tags=["3. Exploitation Services"],
)


# ======================================================
# Source restrictions
# ======================================================
# Some indicators rely on fields that are only populated for certain data
# sources. These constants declare the allowed tender_type values.
# None (= all sources) is never valid for restricted indicators.

_INSIDERS_ONLY: frozenset = frozenset({"insiders"})


def _require_tender_type(
    body: IndicatorRequest,
    allowed: frozenset,
    indicator: str,
) -> None:
    """
    Raise ValidationException if body.tender_type is not in `allowed`.

    Parameters
    ----------
    body      : incoming request body
    allowed   : set of valid tender_type strings (e.g. _INSIDERS_ONLY)
    indicator : human-readable indicator name used in the error message
    """
    if body.tender_type not in allowed:
        allowed_str = ", ".join(f'"{v}"' for v in sorted(allowed))
        raise ValidationException(
            f"Indicator '{indicator}' is only available for "
            f"tender_type in [{allowed_str}]. "
            f"Received: {body.tender_type!r}."
        )


# ======================================================
# Helper: build Solr filter query from MetadataFilter
# ======================================================
def _date_to_fq(date_value: str) -> str:
    """
    Convert a date value to a Solr ``updated`` fq clause.

    Accepted formats:
    - 4-digit year (``"2024"``) → range covering the full year.
    - Any other string (ISO-8601 timestamp, Solr date-math expression,
      explicit range ``[... TO ...]``) → passed through as-is.
    """
    if date_value.isdigit() and len(date_value) == 4:
        return (
            f"updated:[{date_value}-01-01T00:00:00Z"
            f" TO {date_value}-12-31T23:59:59Z]"
        )
    return f"updated:{date_value}"


def _build_filter_query(
    filters: MetadataFilter | None,
) -> str | None:
    """
    Build a Solr ``fq`` string from a structured ``MetadataFilter``.

    Field mapping:
    - ``date``  → ``updated`` (see ``_date_to_fq`` for accepted formats)
    - ``cpv``   → ``cpv_list``
    - ``extra`` → arbitrary indexed fields (passed through as-is)

    Returns ``None`` if no filters are active.
    """
    parts: list[str] = []

    if filters is not None:
        if filters.date is not None:
            parts.append(_date_to_fq(filters.date))
        if filters.cpv is not None:
            parts.append(f"cpv_list:{filters.cpv}")
        if filters.extra:
            for key, value in filters.extra.items():
                parts.append(f"{key}:{value}")

    return " AND ".join(parts) if parts else None


def _search_examples(*examples: tuple[str, str, dict]) -> dict:
    """Return an openapi_extra dict with named request body examples."""
    return {
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        name: {
                            "summary": summary,
                            "value": value,
                        }
                        for name, summary, value in examples
                    }
                }
            }
        }
    }


def _semantic_by_text_examples() -> dict:
    """Examples for semantic search by free text."""
    return _search_examples(
        (
            "AI procurement with filters",
            "Semantic search with date, CPV and extra metadata filters",
            {
                "query_text": "inteligencia artificial en contratacion publica",
                "filters": {
                    "date": "2025",
                    "cpv": "72*",
                    "extra": {"tender_type": "insiders"},
                },
                "pagination": {"start": 0, "rows": 10},
            },
        ),
        (
            "Cybersecurity without filters",
            "Semantic search using only the query text",
            {
                "query_text": "servicios de ciberseguridad y monitorizacion",
                "pagination": {"start": 0, "rows": 5},
            },
        ),
    )


def _thematic_by_text_examples() -> dict:
    """Examples for thematic similarity by free text."""
    return _search_examples(
        (
            "IT supplies with model",
            "Thematic similarity using a topic model and CPV/date filters",
            {
                "query_text": "suministro de equipos informaticos para centros publicos",
                "model_name": "topic_model_v1",
                "filters": {
                    "date": "2024",
                    "cpv": "30*",
                },
                "pagination": {"start": 0, "rows": 10},
            },
        ),
    )


def _semantic_by_document_examples() -> dict:
    """Examples for semantic similarity by existing document IDs."""
    return _search_examples(
        (
            "Two reference documents",
            "Semantic similarity aggregated from two indexed documents",
            {
                "doc_ids": ["DOC-2025-001", "DOC-2025-042"],
                "filters": {
                    "date": "2025",
                    "cpv": "72*",
                },
                "pagination": {"start": 0, "rows": 10},
            },
        ),
    )


def _thematic_by_document_examples() -> dict:
    """Examples for thematic similarity by existing document IDs."""
    return _search_examples(
        (
            "Thematic by document IDs",
            "Thematic similarity using a topic model and two reference documents",
            {
                "doc_ids": ["DOC-2025-001", "DOC-2025-042"],
                "model_name": "topic_model_v1",
                "filters": {
                    "date": "2025",
                    "cpv": "72*",
                    "extra": {"tender_type": "insiders"},
                },
                "pagination": {"start": 0, "rows": 10},
            },
        ),
    )


# ======================================================
# Metadata queries
# ======================================================
@router.get(
    "/corpora/{corpus_collection}/documents/{doc_id}",
    response_model=DataResponse,
    summary="Get document metadata",
    description="Retrieve all metadata associated with a specific document.",
    responses=error_responses(
        NotFoundException, SolrException,
        NotFoundException="Document or corpus not found",
    ),
)
async def get_document_metadata(
    request: Request,
    corpus_collection: str = Path(..., description="Corpus collection name"),
    doc_id: str = Path(..., description="Document ID"),
) -> DataResponse:
    """Get document metadata by ID."""
    sc = request.app.state.solr_client
    try:
        result = sc.do_Q6(corpus_col=corpus_collection, doc_id=doc_id)
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


@router.get(
    "/corpora/{corpus_collection}/metadata-fields",
    response_model=DataResponse,
    summary="Get corpus metadata fields",
    description="Returns the list of metadata fields available in a corpus.",
    responses=error_responses(
        NotFoundException, SolrException,
        NotFoundException="Corpus not found",
    ),
)
async def get_corpus_metadata_fields(
    request: Request,
    corpus_collection: str = Path(..., description="Corpus collection name"),
) -> DataResponse:
    """Get metadata fields of a corpus."""
    sc = request.app.state.solr_client
    try:
        result = sc.do_Q2(corpus_col=corpus_collection)
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


# ======================================================
# Semantic Search
# ======================================================
@router.post(
    "/corpora/{corpus_collection}/semantic/by-text",
    response_model=DataResponse,
    summary="Semantic search by text",
    description=(
        "Semantic search for documents similar to a given text. Uses BERT "
        "embeddings to find documents semantically related to the query, "
        "regardless of exact word matches. Results can be filtered by year, "
        "CPV code, and additional metadata."
    ),
    responses=error_responses(
        NotFoundException, SolrException,
        NotFoundException="Corpus not found",
    ),
    openapi_extra=_semantic_by_text_examples(),
)
async def semantic_search_by_text(
    request: Request,
    corpus_collection: str = Path(..., description="Corpus collection name"),
    body: SemanticSearchByTextRequest = Body(...),
) -> DataResponse:
    """Semantic search using BERT embeddings."""
    sc = request.app.state.solr_client
    try:
        result = sc.do_Q21(
            corpus_col=corpus_collection,
            search_doc=body.query_text,
            filter_query=_build_filter_query(body.filters),
            start=body.pagination.start,
            rows=body.pagination.rows,
        )
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


@router.post(
    "/corpora/{corpus_collection}/thematic/by-text",
    response_model=DataResponse,
    summary="Thematic similarity by text",
    description=(
        "Find thematically similar documents to a given text. Uses topic "
        "model inference to find documents with similar thematic content."
    ),
    responses=error_responses(
        NotFoundException, SolrException,
        NotFoundException="Corpus or model not found",
    ),
    openapi_extra=_thematic_by_text_examples(),
)
async def similar_docs_by_text_tm(
    request: Request,
    corpus_collection: str = Path(..., description="Corpus collection name"),
    body: ThematicSearchByTextRequest = Body(...),
) -> DataResponse:
    """Find similar documents using topic model inference."""
    sc = request.app.state.solr_client
    try:
        result = sc.do_Q14(
            corpus_col=corpus_collection,
            model_name=body.model_name,
            text_to_infer=body.query_text,
            filter_query=_build_filter_query(body.filters),
            start=body.pagination.start,
            rows=body.pagination.rows,
        )
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))

# ======================================================
# Similarity by Document ID(s)
# ======================================================
@router.post(
    "/corpora/{corpus_collection}/semantic/by-document",
    response_model=DataResponse,
    summary="Semantically similar documents by document ID(s)",
    description=(
        "Find documents semantically similar to one or more existing indexed "
        "documents. Accepts a list of document IDs and returns results "
        "aggregated across all reference documents."
    ),
    responses=error_responses(
        NotFoundException, SolrException,
        NotFoundException="Document, corpus or model not found",
    ),
    openapi_extra=_semantic_by_document_examples(),
)
async def similar_documents_by_id(
    request: Request,
    corpus_collection: str = Path(..., description="Corpus collection name"),
    body: SimilarByDocumentRequest = Body(...),
) -> DataResponse:
    """Find documents semantically similar to one or more existing documents."""
    sc = request.app.state.solr_client
    try:
        # TODO: implement multi-doc aggregation; for now process each ID
        all_results = []
        for doc_id in body.doc_ids:
            result = sc.do_Q21_by_doc(
                corpus_col=corpus_collection,
                doc_id=doc_id,
                filter_query=_build_filter_query(body.filters),
                start=body.pagination.start,
                rows=body.pagination.rows,
            )
            all_results.extend(result if isinstance(result, list) else [result])
        return DataResponse(success=True, data=all_results)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


@router.post(
    "/corpora/{corpus_collection}/thematic/by-document",
    response_model=DataResponse,
    summary="Thematically similar documents by document ID(s)",
    description=(
        "Find thematically similar documents to one or more existing indexed "
        "documents using topic model distributions."
    ),
    responses=error_responses(
        ValidationException, NotFoundException, SolrException,
        ValidationException="model_name is required for thematic similarity",
        NotFoundException="Document, corpus or model not found",
    ),
    openapi_extra=_thematic_by_document_examples(),
)
async def similar_docs_by_doc_tm(
    request: Request,
    corpus_collection: str = Path(..., description="Corpus collection name"),
    body: SimilarByDocumentRequest = Body(...),
) -> DataResponse:
    """Find thematically similar documents to one or more existing documents."""
    if not body.model_name:
        raise ValidationException("model_name is required for thematic similarity")

    sc = request.app.state.solr_client
    try:
        all_results = []
        for doc_id in body.doc_ids:
            result = sc.do_Q15(
                corpus_col=corpus_collection,
                model_name=body.model_name,
                doc_id=doc_id,
                filter_query=_build_filter_query(body.filters),
                start=body.pagination.start,
                rows=body.pagination.rows,
            )
            all_results.extend(result if isinstance(result, list) else [result])
        return DataResponse(success=True, data=all_results)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


# ======================================================
# Temporal Search
# ======================================================
#@router.post(
#    "/corpora/{corpus_collection}/temporal/by-year",
#    response_model=DataResponse,
#    summary="Documents by year",
#    description="Retrieve documents filtered by publication year with optional metadata filters.",
#    responses=error_responses(
#        NotFoundException, SolrException,
#        NotFoundException="Corpus not found",
#    ),
#)
#async def get_documents_by_year(
#    request: Request,
#    corpus_collection: str = Path(..., description="Corpus collection name"),
#    body: TemporalSearchRequest = Body(...),
#) -> DataResponse:
#    """Get documents from a specific year."""
#    sc = request.app.state.solr_client
#    try:
#        result = sc.do_Q30(
#            corpus_col=corpus_collection,
#            year=body.year,
#            filter_query=_build_filter_query(body.filters),
#            start=body.pagination.start,
#            rows=body.pagination.rows,
#        )
#        return DataResponse(success=True, data=result)
#    except APIException:
#        raise
#    except Exception as e:
#        raise SolrException(str(e))
    
# ======================================================
# Indicators and Statistics
# ======================================================
@router.get(
    "/collections/{collection}/count",
    response_model=DataResponse,
    summary="Count documents",
    description="Get the total number of documents in a collection.",
    responses=error_responses(
        NotFoundException, SolrException,
        NotFoundException="Collection not found",
    ),
)
async def get_document_count(
    request: Request,
    collection: str = Path(..., description="Collection name"),
) -> DataResponse:
    """Get document count."""
    sc = request.app.state.solr_client
    try:
        result = sc.do_Q3(col=collection)
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


@router.get(
    "/corpora/{corpus_collection}/years",
    response_model=DataResponse,
    summary="Get available years",
    description="List all years with documents in the corpus.",
    responses=error_responses(
        NotFoundException, SolrException,
        NotFoundException="Corpus not found",
    ),
)
async def get_available_years(
    request: Request,
    corpus_collection: str = Path(..., description="Corpus collection name"),
) -> DataResponse:
    """Get list of available years."""
    sc = request.app.state.solr_client
    try:
        result = sc.do_Q31(corpus_col=corpus_collection)
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))
    

# ======================================================
# OpenAPI request body examples for indicator endpoints
# These mirror the payload used by the plot_indicators.py script
# (CPVs 48/72/73, full 2025-Feb 2026 range, one per tender_type).
# ======================================================

def _indicator_examples(extra_note: str = "") -> dict:
    """
    Return an openapi_extra dict that injects three named examples
    into the Swagger UI for every indicator endpoint.
    extra_note: optional text appended to the example summary line,
                e.g. to flag insiders-only restrictions.
    """
    suffix = f" {extra_note}" if extra_note else ""
    _BASE = {
        "date_start":       "2025-01-01T00:00:00Z",
        "date_end":         "2026-03-01T00:00:00Z",
        "date_field":       "updated",
        "cpv_prefixes":     ["48", "72", "73"],
        "budget_min":       None,
        "budget_max":       None,
        "subentidad":       None,
        "cod_subentidad":   None,
        "organo_id":        None,
        "topic_model":      None,
        "topic_id":         None,
        "topic_min_weight": None,
    }
    return {
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "insiders (CPV 48/72/73, 2025-Feb 2026)": {
                            "summary": f"insiders - CPV 48/72/73, Jan 2025-Feb 2026{suffix}",
                            "value": {**_BASE, "tender_type": "insiders"},
                        },
                        "outsiders (CPV 48/72/73, 2025-Feb 2026)": {
                            "summary": f"outsiders - CPV 48/72/73, Jan 2025-Feb 2026{suffix}",
                            "value": {**_BASE, "tender_type": "outsiders"},
                        },
                        "minors (CPV 48/72/73, 2025-Feb 2026)": {
                            "summary": f"minors - CPV 48/72/73, Jan 2025-Feb 2026{suffix}",
                            "value": {**_BASE, "tender_type": "minors"},
                        },
                    }
                }
            }
        }
    }


def _indicator_examples_insiders_only() -> dict:
    """Variant for endpoints restricted to tender_type=insiders."""
    _BASE = {
        "date_start":       "2025-01-01T00:00:00Z",
        "date_end":         "2026-03-01T00:00:00Z",
        "date_field":       "updated",
        "tender_type":      "insiders",
        "cpv_prefixes":     ["48", "72", "73"],
        "budget_min":       None,
        "budget_max":       None,
        "subentidad":       None,
        "cod_subentidad":   None,
        "organo_id":        None,
        "topic_model":      None,
        "topic_id":         None,
        "topic_min_weight": None,
    }
    return {
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "insiders (CPV 48/72/73, 2025-Feb 2026) [insiders only]": {
                            "summary": "insiders - CPV 48/72/73, Jan 2025-Feb 2026 [insiders only]",
                            "value": _BASE,
                        },
                    }
                }
            }
        }
    }


@router.post(                          
    "/indicators/total-procurement",
    response_model=DataResponse,
    summary="Total procurement indicator",
    description=(
        "Count of tenders and aggregated budget per bimester. "
        "Filter by source, CPV, date range, geography or contracting authority."
    ),
    responses=error_responses(
        ValidationException, SolrException,
    ),
    openapi_extra=_indicator_examples(),
)
async def calculate_indicator_total_procurement(
    request: Request,
    body: IndicatorRequest = Body(...),
) -> DataResponse:
    sc = request.app.state.solr_client
    try:
        result, status = sc.do_Q40(
            date_start       = body.date_start,
            date_end         = body.date_end,
            date_field       = body.date_field,
            tender_type      = body.tender_type,
            cpv_prefixes     = body.cpv_prefixes,
            budget_min       = body.budget_min,
            budget_max       = body.budget_max,
            subentidad       = body.subentidad,
            cod_subentidad   = body.cod_subentidad,
            organo_id        = body.organo_id,
            topic_model      = body.topic_model,
            topic_id         = body.topic_id,
            topic_min_weight = body.topic_min_weight,
        )
        if status != 200:
            raise SolrException(result.get("error", "Solr query failed"))
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))

@router.post(
    "/indicators/single-bidder",
    response_model=DataResponse,
    summary="Single bidder indicator",
    description=(
        "Percentage of lots with exactly one offer received, per bimester. "
        "Includes field coverage statistics. "
        "Filter by source, CPV, date range, geography or contracting authority."
    ),
    responses=error_responses(ValidationException, SolrException),
    openapi_extra=_indicator_examples(),
)
async def calculate_indicator_single_bidder(
    request: Request,
    body: IndicatorRequest = Body(...),
) -> DataResponse:
    sc = request.app.state.solr_client
    try:
        result, status = sc.do_Q41(
            date_start       = body.date_start,
            date_end         = body.date_end,
            date_field       = body.date_field,
            tender_type      = body.tender_type,
            cpv_prefixes     = body.cpv_prefixes,
            budget_min       = body.budget_min,
            budget_max       = body.budget_max,
            subentidad       = body.subentidad,
            cod_subentidad   = body.cod_subentidad,
            organo_id        = body.organo_id,
            topic_model      = body.topic_model,
            topic_id         = body.topic_id,
            topic_min_weight = body.topic_min_weight,
        )
        if status != 200:
            raise SolrException(result.get("error", "Solr query failed"))
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


@router.post(
    "/indicators/decision-speed",
    response_model=DataResponse,
    summary="Decision speed indicator",
    description=(
        "Average days between submission deadline and award decision, per bimester. "
        "Filter by source, CPV, date range, geography or contracting authority."
    ),
    responses=error_responses(ValidationException, SolrException),
    openapi_extra=_indicator_examples_insiders_only(),
)
async def calculate_indicator_decision_speed(
    request: Request,
    body: IndicatorRequest = Body(...),
) -> DataResponse:
    _require_tender_type(body, _INSIDERS_ONLY, "decision-speed")
    sc = request.app.state.solr_client
    try:
        result, status = sc.do_Q42(
            date_start       = body.date_start,
            date_end         = body.date_end,
            date_field       = body.date_field,
            tender_type      = body.tender_type,
            cpv_prefixes     = body.cpv_prefixes,
            budget_min       = body.budget_min,
            budget_max       = body.budget_max,
            subentidad       = body.subentidad,
            cod_subentidad   = body.cod_subentidad,
            organo_id        = body.organo_id,
            topic_model      = body.topic_model,
            topic_id         = body.topic_id,
            topic_min_weight = body.topic_min_weight,
        )
        if status != 200:
            raise SolrException(result.get("error", "Solr query failed"))
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


@router.post(
    "/indicators/direct-awards",
    response_model=DataResponse,
    summary="Direct awards indicator",
    description=(
        "Percentage of procedures awarded via 'Negociado sin publicidad', per bimester. "
        "Includes field coverage statistics. "
        "Filter by source, CPV, date range, geography or contracting authority."
    ),
    responses=error_responses(ValidationException, SolrException),
    openapi_extra=_indicator_examples(),
)
async def calculate_indicator_direct_awards(
    request: Request,
    body: IndicatorRequest = Body(...),
) -> DataResponse:
    sc = request.app.state.solr_client
    try:
        result, status = sc.do_Q43(
            date_start       = body.date_start,
            date_end         = body.date_end,
            date_field       = body.date_field,
            tender_type      = body.tender_type,
            cpv_prefixes     = body.cpv_prefixes,
            budget_min       = body.budget_min,
            budget_max       = body.budget_max,
            subentidad       = body.subentidad,
            cod_subentidad   = body.cod_subentidad,
            organo_id        = body.organo_id,
            topic_model      = body.topic_model,
            topic_id         = body.topic_id,
            topic_min_weight = body.topic_min_weight,
        )
        if status != 200:
            raise SolrException(result.get("error", "Solr query failed"))
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


@router.post(
    "/indicators/ted-publication",
    response_model=DataResponse,
    summary="TED publication indicator",
    description=(
        "Percentage of procedures published in the EU TED portal, per bimester. "
        "Filter by source, CPV, date range, geography or contracting authority."
    ),
    responses=error_responses(ValidationException, SolrException),
    openapi_extra=_indicator_examples(),
)
async def calculate_indicator_ted_publication(
    request: Request,
    body: IndicatorRequest = Body(...),
) -> DataResponse:
    sc = request.app.state.solr_client
    try:
        result, status = sc.do_Q44(
            date_start       = body.date_start,
            date_end         = body.date_end,
            date_field       = body.date_field,
            tender_type      = body.tender_type,
            cpv_prefixes     = body.cpv_prefixes,
            budget_min       = body.budget_min,
            budget_max       = body.budget_max,
            subentidad       = body.subentidad,
            cod_subentidad   = body.cod_subentidad,
            organo_id        = body.organo_id,
            topic_model      = body.topic_model,
            topic_id         = body.topic_id,
            topic_min_weight = body.topic_min_weight,
        )
        if status != 200:
            raise SolrException(result.get("error", "Solr query failed"))
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


@router.post(
    "/indicators/sme-participation",
    response_model=DataResponse,
    summary="SME participation indicator",
    description=(
        "Percentage of lots with at least one SME offer, per bimester. "
        "Includes field coverage statistics. "
        "Filter by source, CPV, date range, geography or contracting authority."
    ),
    responses=error_responses(ValidationException, SolrException),
    openapi_extra=_indicator_examples_insiders_only(),
)
async def calculate_indicator_sme_participation(
    request: Request,
    body: IndicatorRequest = Body(...),
) -> DataResponse:
    _require_tender_type(body, _INSIDERS_ONLY, "sme-participation")
    sc = request.app.state.solr_client
    try:
        result, status = sc.do_Q45(
            date_start       = body.date_start,
            date_end         = body.date_end,
            date_field       = body.date_field,
            tender_type      = body.tender_type,
            cpv_prefixes     = body.cpv_prefixes,
            budget_min       = body.budget_min,
            budget_max       = body.budget_max,
            subentidad       = body.subentidad,
            cod_subentidad   = body.cod_subentidad,
            organo_id        = body.organo_id,
            topic_model      = body.topic_model,
            topic_id         = body.topic_id,
            topic_min_weight = body.topic_min_weight,
        )
        if status != 200:
            raise SolrException(result.get("error", "Solr query failed"))
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


@router.post(
    "/indicators/sme-offer-ratio",
    response_model=DataResponse,
    summary="SME offer ratio indicator",
    description=(
        "Percentage of all offers submitted by SMEs, per bimester. "
        "Includes field coverage statistics. "
        "Filter by source, CPV, date range, geography or contracting authority."
    ),
    responses=error_responses(ValidationException, SolrException),
    openapi_extra=_indicator_examples_insiders_only(),
)
async def calculate_indicator_sme_offer_ratio(
    request: Request,
    body: IndicatorRequest = Body(...),
) -> DataResponse:
    _require_tender_type(body, _INSIDERS_ONLY, "sme-offer-ratio")
    sc = request.app.state.solr_client
    try:
        result, status = sc.do_Q46(
            date_start       = body.date_start,
            date_end         = body.date_end,
            date_field       = body.date_field,
            tender_type      = body.tender_type,
            cpv_prefixes     = body.cpv_prefixes,
            budget_min       = body.budget_min,
            budget_max       = body.budget_max,
            subentidad       = body.subentidad,
            cod_subentidad   = body.cod_subentidad,
            organo_id        = body.organo_id,
            topic_model      = body.topic_model,
            topic_id         = body.topic_id,
            topic_min_weight = body.topic_min_weight,
        )
        if status != 200:
            raise SolrException(result.get("error", "Solr query failed"))
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


@router.post(
    "/indicators/lots-division",
    response_model=DataResponse,
    summary="Lots division indicator",
    description=(
        "Percentage of procedures divided into more than one lot, per bimester. "
        "Includes field coverage statistics. "
        "Filter by source, CPV, date range, geography or contracting authority."
    ),
    responses=error_responses(ValidationException, SolrException),
    openapi_extra=_indicator_examples(),
)
async def calculate_indicator_lots_division(
    request: Request,
    body: IndicatorRequest = Body(...),
) -> DataResponse:
    sc = request.app.state.solr_client
    try:
        result, status = sc.do_Q47(
            date_start       = body.date_start,
            date_end         = body.date_end,
            date_field       = body.date_field,
            tender_type      = body.tender_type,
            cpv_prefixes     = body.cpv_prefixes,
            budget_min       = body.budget_min,
            budget_max       = body.budget_max,
            subentidad       = body.subentidad,
            cod_subentidad   = body.cod_subentidad,
            organo_id        = body.organo_id,
            topic_model      = body.topic_model,
            topic_id         = body.topic_id,
            topic_min_weight = body.topic_min_weight,
        )
        if status != 200:
            raise SolrException(result.get("error", "Solr query failed"))
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


@router.post(
    "/indicators/missing-supplier-id",
    response_model=DataResponse,
    summary="Missing supplier ID indicator",
    description=(
        "Percentage of awarded lots where the supplier identifier is absent, "
        "per bimester. "
        "Filter by source, CPV, date range, geography or contracting authority."
    ),
    responses=error_responses(ValidationException, SolrException),
    openapi_extra=_indicator_examples(),
)
async def calculate_indicator_missing_supplier_id(
    request: Request,
    body: IndicatorRequest = Body(...),
) -> DataResponse:
    sc = request.app.state.solr_client
    try:
        result, status = sc.do_Q48(
            date_start       = body.date_start,
            date_end         = body.date_end,
            date_field       = body.date_field,
            tender_type      = body.tender_type,
            cpv_prefixes     = body.cpv_prefixes,
            budget_min       = body.budget_min,
            budget_max       = body.budget_max,
            subentidad       = body.subentidad,
            cod_subentidad   = body.cod_subentidad,
            organo_id        = body.organo_id,
            topic_model      = body.topic_model,
            topic_id         = body.topic_id,
            topic_min_weight = body.topic_min_weight,
        )
        if status != 200:
            raise SolrException(result.get("error", "Solr query failed"))
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))


@router.post(
    "/indicators/missing-buyer-id",
    response_model=DataResponse,
    summary="Missing buyer ID indicator",
    description=(
        "Percentage of procedures where the contracting authority identifier "
        "is absent, per bimester. "
        "Filter by source, CPV, date range, geography or contracting authority."
    ),
    responses=error_responses(ValidationException, SolrException),
    openapi_extra=_indicator_examples(),
)
async def calculate_indicator_missing_buyer_id(
    request: Request,
    body: IndicatorRequest = Body(...),
) -> DataResponse:
    sc = request.app.state.solr_client
    try:
        result, status = sc.do_Q49(
            date_start       = body.date_start,
            date_end         = body.date_end,
            date_field       = body.date_field,
            tender_type      = body.tender_type,
            cpv_prefixes     = body.cpv_prefixes,
            budget_min       = body.budget_min,
            budget_max       = body.budget_max,
            subentidad       = body.subentidad,
            cod_subentidad   = body.cod_subentidad,
            organo_id        = body.organo_id,
            topic_model      = body.topic_model,
            topic_id         = body.topic_id,
            topic_min_weight = body.topic_min_weight,
        )
        if status != 200:
            raise SolrException(result.get("error", "Solr query failed"))
        return DataResponse(success=True, data=result)
    except APIException:
        raise
    except Exception as e:
        raise SolrException(str(e))