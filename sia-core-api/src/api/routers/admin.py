"""
Infrastructure Administration (Configuration APIs)

This module groups low-level operations reserved for technical management
and dynamic system configuration:

- Solr collections management: creation, listing, reloading and deletion of indexes
- Display configuration: metadata, visible fields, active filters
- API key management: generation, listing, revocation

Response conventions:
- All responses extend ResponseBase (success + message).
- Error responses use ErrorResponse.

Author: Lorena Calvo-Bartolome
Date: 27/03/2023
Modified: 04/02/2026 (Migrated to FastAPI and reorganized)
"""

import logging

from fastapi import APIRouter, Depends, Request, Path, Body # type: ignore
from src.api.schemas import (
    CollectionCreateRequest,
    ResponseBase,
    CollectionResponse,
    CollectionListResponse,
    SolrQueryParams,
    SolrQueryResponse
)
from src.api.exceptions import (
    APIException,
    ConflictException,
    NotFoundException,
    SolrException,
    UnauthorizedException,
    ValidationException,
    error_responses,
    raise_internal_solr,
)
from src.api.auth import (
    api_key_manager,
    verify_master_key,
    APIKeyCreate,
    APIKeyResponse,
    APIKeyListResponse,
    APIKeyInfo,
)

# ======================================================
# Router
# ======================================================
logger = logging.getLogger("SIA-Core-API.admin")

router = APIRouter(
    prefix="/admin",
    tags=["1. Infrastructure Administration"],
)


# ======================================================
# Management of Solr Collections
# ======================================================
@router.post(
    "/collections",
    response_model=CollectionResponse,
    status_code=201,
    summary="Create Solr collection",
    description="Creates a new collection in Apache Solr.",
    responses=error_responses(
        ConflictException, SolrException,
        ConflictException="Collection already exists",
    ),
)
async def create_collection(
    request: Request,
    body: CollectionCreateRequest = Body(...),
) -> CollectionResponse:
    """Create a new Solr collection."""
    sc = request.app.state.solr_client
    collection = body.collection
    try:
        _, status_code = sc.create_collection(col_name=collection)
        
        if status_code == 409:
            raise ConflictException("Collection", collection)
        
        return CollectionResponse(
            success=True,
            message=f"Collection '{collection}' created successfully",
            collection=collection
        )
    except APIException:
        raise
    except Exception as e:
        raise_internal_solr(e, log=logger)


@router.delete(
    "/collections/{collection}",
    response_model=CollectionResponse,
    summary="Delete Solr collection",
    description="Deletes an existing collection from Apache Solr.",
    responses=error_responses(
        NotFoundException, SolrException,
        NotFoundException="Collection not found",
    ),
)
async def delete_collection(
    request: Request,
    collection: str = Path(..., description="Name of the collection to delete"),
) -> CollectionResponse:
    """Delete an existing Solr collection."""
    sc = request.app.state.solr_client
    try:
        sc.delete_collection(col_name=collection)
        return CollectionResponse(
            success=True,
            message=f"Collection '{collection}' deleted successfully",
            collection=collection
        )
    except APIException:
        raise
    except Exception as e:
        raise_internal_solr(e, log=logger)


@router.get(
    "/collections",
    response_model=CollectionListResponse,
    summary="List Solr collections",
    description="Returns the list of all available collections in Solr.",
    responses=error_responses(SolrException),
)
async def list_collections(
    request: Request
) -> CollectionListResponse:
    """List all available Solr collections."""
    sc = request.app.state.solr_client
    try:
        collections, code = sc.list_collections()
        if code != 200:
            raise SolrException(f"Error listing collections (code: {code})")
        return CollectionListResponse(
            success=True,
            collections=collections
        )
    except APIException:
        raise
    except Exception as e:
        raise_internal_solr(e, log=logger)


@router.get(
    "/collections/{collection}/query",
    response_model=SolrQueryResponse,
    summary="Execute Solr query",
    description="Executes a direct query against a Solr collection using standard syntax.",
    responses=error_responses(
        ValidationException, NotFoundException, SolrException,
        ValidationException="Invalid query syntax",
        NotFoundException="Collection not found",
    ),
)
async def execute_raw_query(
    request: Request,
    collection: str = Path(..., description="Collection to query"),
    params: SolrQueryParams = Depends(),
) -> SolrQueryResponse:
    """Execute Solr query with full parameters."""
    sc = request.app.state.solr_client
    query_values = {
        "q_op": params.q_op.value if params.q_op else None,
        "fq": params.fq,
        "sort": params.sort,
        "start": params.start,
        "rows": params.rows,
        "fl": params.fl,
        "df": params.df,
    }
    query_values = {k: v for k, v in query_values.items() if v is not None}
    
    try:
        code, results = sc.execute_query(q=params.q, col_name=collection, **query_values)
        return SolrQueryResponse(
            success=True,
            data=results.docs,
            num_found=getattr(results, 'num_found', len(results.docs))
        )
    except APIException:
        raise
    except Exception as e:
        raise_internal_solr(e, log=logger)


# ======================================================
# API Key Management (requires master key)
# ======================================================
@router.post(
    "/api-keys",
    response_model=APIKeyResponse,
    status_code=201,
    summary="Generate new API key",
    description="Generate a new API key. **Requires master key authentication.** The generated API key will only be shown once in the response. Store it securely as it cannot be retrieved later.",
    dependencies=[Depends(verify_master_key)],
    responses=error_responses(
        UnauthorizedException,
        UnauthorizedException="Invalid or missing master key",
    ),
)
async def create_api_key(
    request: Request,
    key_request: APIKeyCreate = Body(...),
) -> APIKeyResponse:
    """Generate a new API key."""
    return api_key_manager.generate_key(key_request.name)


@router.get(
    "/api-keys",
    response_model=APIKeyListResponse,
    summary="List all API keys",
    description="List all API keys with their metadata. **Requires master key authentication.** Note: The actual key values are not returned for security reasons.",
    dependencies=[Depends(verify_master_key)],
    responses=error_responses(
        UnauthorizedException,
        UnauthorizedException="Invalid or missing master key",
    ),
)
async def list_api_keys(
    request: Request,
) -> APIKeyListResponse:
    """List all API keys."""
    keys = api_key_manager.list_keys()
    return APIKeyListResponse(keys=keys, total=len(keys))


@router.get(
    "/api-keys/{key_id}",
    response_model=APIKeyInfo,
    summary="Get API key info",
    description="Get information about a specific API key. **Requires master key authentication.**",
    dependencies=[Depends(verify_master_key)],
    responses=error_responses(
        UnauthorizedException, NotFoundException,
        UnauthorizedException="Invalid or missing master key",
        NotFoundException="API key not found",
    ),
)
async def get_api_key(
    request: Request,
    key_id: str = Path(..., description="ID of the API key"),
) -> APIKeyInfo:
    """Get info about a specific API key."""
    key_info = api_key_manager.get_key_info(key_id)
    if not key_info:
        raise NotFoundException(f"API key with ID '{key_id}' not found")
    return key_info


@router.post(
    "/api-keys/{key_id}/revoke",
    response_model=ResponseBase,
    summary="Revoke API key",
    description="Revoke an API key (soft delete). **Requires master key authentication.** The key will be deactivated but kept in records.",
    dependencies=[Depends(verify_master_key)],
    responses=error_responses(
        UnauthorizedException, NotFoundException,
        UnauthorizedException="Invalid or missing master key",
        NotFoundException="API key not found",
    ),
)
async def revoke_api_key(
    request: Request,
    key_id: str = Path(..., description="ID of the API key to revoke"),
) -> ResponseBase:
    """Revoke an API key."""
    if api_key_manager.revoke_key(key_id):
        return ResponseBase(success=True, message=f"API key '{key_id}' revoked successfully")
    raise NotFoundException(f"API key with ID '{key_id}' not found")


@router.delete(
    "/api-keys/{key_id}",
    response_model=ResponseBase,
    summary="Delete API key",
    description="Permanently delete an API key. **Requires master key authentication.**",
    dependencies=[Depends(verify_master_key)],
    responses=error_responses(
        UnauthorizedException, NotFoundException,
        UnauthorizedException="Invalid or missing master key",
        NotFoundException="API key not found",
    ),
)
async def delete_api_key(
    request: Request,
    key_id: str = Path(..., description="ID of the API key to delete"),
) -> ResponseBase:
    """Permanently delete an API key."""
    if api_key_manager.delete_key(key_id):
        return ResponseBase(success=True, message=f"API key '{key_id}' deleted successfully")
    raise NotFoundException(f"API key with ID '{key_id}' not found")