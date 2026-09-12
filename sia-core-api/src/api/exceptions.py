"""
Custom exceptions and error handling for the API.

Author: Lorena Calvo-Bartolomé
Date: 04/02/2026 (Migrated to FastAPI)
"""

import logging
import uuid

from fastapi import FastAPI, HTTPException, Request # type: ignore
from fastapi.responses import JSONResponse # type: ignore
from typing import Any, Dict, Optional, Type

logger = logging.getLogger("SIA-Core-API.errors")

_INTERNAL_ERROR_MESSAGE = "An internal error occurred. Contact support with the correlation id."


def _sanitized_500(correlation_id: str, error_code: str = "INTERNAL_ERROR") -> Dict[str, Any]:
    """Body returned to the client for server-side (5xx) failures."""
    return {
        "success": False,
        "error": _INTERNAL_ERROR_MESSAGE,
        "error_code": error_code,
        "details": {"correlation_id": correlation_id},
    }


# ======================================================
# Base Exception
# ======================================================
class APIException(HTTPException):
    """
    Base class for every custom API exception.

    Subclasses fix "status_code" and "error_code" as class-level defaults callers only provide the human-readable message and, optionally, a details dict.

    The "detail" field (inherited from HTTPException) is set to the full ErrorResponse (shaped dict so that even without custom exception handlers FastAPI's default HTTPException handler produces the correct JSON body::

        {
            "success": false,
            "error": "...",
            "error_code": "...",
            "details": { ... }
        }
    """

    status_code: int = 500
    error_code: str = "INTERNAL_ERROR"

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.error = message
        self.error_code_instance = self.error_code  # for handler access
        self.details = details

        # Build the ErrorResponse-shaped payload and store it in
        # "detail" so that FastAPI's default HTTPException handler
        # returns it as-is when custom handlers are not registered.
        body: Dict[str, Any] = {
            "success": False,
            "error": message,
            "error_code": self.error_code,
            "details": details,
        }
        super().__init__(status_code=self.status_code, detail=body)

    @classmethod
    def response_spec(cls, description: Optional[str] = None) -> dict:
        """
        Generate a Swagger/OpenAPI response spec from this exception class.

        Returns a dict suitable for use as a value in FastAPI's
        responses parameter::

            @router.get("/...", responses={
                **NotFoundException.response_spec("Corpus not found"),
            })

        The key is the HTTP status code; the value includes a
        representative example derived from error_code.
        """
        desc = description or cls.error_code.replace("_", " ").title()
        return {
            cls.status_code: {
                "description": desc,
                "content": {
                    "application/json": {
                        "example": {
                            "success": False,
                            "error": f"{desc}",
                            "error_code": cls.error_code,
                            "details": None,
                        }
                    }
                },
            }
        }


# ======================================================
# 400 - Bad Request / Validation
# ======================================================
class ValidationException(APIException):
    """
    Request payload fails business-logic validation.

    Examples: mutually-exclusive fields both missing, invalid file type,
    malformed base64 content.

    HTTP 400 - BAD_REQUEST
    """

    status_code = 400
    error_code = "BAD_REQUEST"


# ======================================================
# 401 - Unauthorized
# ======================================================
class UnauthorizedException(APIException):
    """
    Authentication failed or credentials are missing/invalid.

    HTTP 401 - UNAUTHORIZED
    """

    status_code = 401
    error_code = "UNAUTHORIZED"


# ======================================================
# 404 - Not Found
# ======================================================
class NotFoundException(APIException):
    """
    A referenced resource does not exist.

    Supports **two call signatures** so that callers can use whichever
    fits the context:

    1. Structured - NotFoundException("Corpus", "procurement_2024")
       - error: "Corpus not found: procurement_2024"
       - details: {"resource": "Corpus", "identifier": "procurement_2024"}

    2. Free-form - NotFoundException("PDF file not found: /path/to/f.pdf")
       - error: "PDF file not found: /path/to/f.pdf"
       - details: None

    HTTP 404 - NOT_FOUND
    """

    status_code = 404
    error_code = "NOT_FOUND"

    def __init__(
        self,
        resource_or_message: str,
        identifier: Optional[str] = None,
    ) -> None:
        if identifier is not None:
            message = f"{resource_or_message} not found: {identifier}"
            details: Optional[Dict[str, Any]] = {
                "resource": resource_or_message,
                "identifier": identifier,
            }
        else:
            message = resource_or_message
            details = None
        super().__init__(message=message, details=details)


# ======================================================
# 409 - Conflict
# ======================================================
class ConflictException(APIException):
    """
    The operation conflicts with the current state of the resource.

    Typical case: trying to create a resource that already exists.

    HTTP 409 - CONFLICT
    """

    status_code = 409
    error_code = "CONFLICT"

    def __init__(self, resource: str, identifier: str) -> None:
        message = f"{resource} already exists: {identifier}"
        details = {"resource": resource, "identifier": identifier}
        super().__init__(message=message, details=details)


# ======================================================
# 500 - Processing Error
# ======================================================
class ProcessingException(APIException):
    """
    An internal processing-pipeline step failed.

    Used by enrichment and ingestion endpoints (text extraction,
    summarisation, topic modelling, embeddings, etc.).

    HTTP 500 - PROCESSING_ERROR
    """

    status_code = 500
    error_code = "PROCESSING_ERROR"

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message=f"Processing error: {message}", details=details)


# ======================================================
# 500 - Solr Error
# ======================================================
class SolrException(APIException):
    """
    Communication with Apache Solr failed.

    Used by administration and exploitation endpoints.

    HTTP 500 - SOLR_ERROR
    """

    status_code = 500
    error_code = "SOLR_ERROR"

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message=f"Solr error: {message}", details=details)


# ======================================================
# Sanitized re-raise helpers (IDX-001)
# ======================================================
# Call these from an except block: the active exception (message, stack,
# any internal paths/identifiers it carries) is written to the server log only,
# and a fixed, non-revealing message is raised toward the client.

def raise_internal_solr(
    exc: BaseException, context: str = "", log: Optional[logging.Logger] = None
) -> "None":
    """Log the real Solr failure server-side, raise a sanitized SolrException."""
    (log or logger).exception(
        "Solr operation failed%s", f": {context}" if context else ""
    )
    raise SolrException("Solr operation failed") from exc


def raise_internal_processing(
    exc: BaseException, context: str = "", log: Optional[logging.Logger] = None
) -> "None":
    """Log the real processing failure server-side, raise a sanitized ProcessingException."""
    (log or logger).exception(
        "Processing operation failed%s", f": {context}" if context else ""
    )
    raise ProcessingException("operation failed") from exc


# ======================================================
# Response-spec helper
# ======================================================
def error_responses(
    *exc_classes: Type[APIException],
    **descriptions: str,
) -> Dict[int, dict]:
    """
    Build a combined responses dict from one or more exception classes.

    Usage::

        @router.get(
            "/items/{id}",
            responses=error_responses(NotFoundException, SolrException),
        )

    You can override the default description for any exception by
    passing its class name as a keyword argument::

        responses=error_responses(
            NotFoundException, SolrException,
            NotFoundException="Corpus not found",
        )

    When two exception classes share the same status code (e.g.
    SolrException and ProcessingException are both 500),
    their descriptions are joined with " | ".
    """
    result: Dict[int, dict] = {}
    for cls in exc_classes:
        desc = descriptions.get(cls.__name__)
        spec = cls.response_spec(description=desc)
        code = cls.status_code
        if code in result:
            existing = result[code]["description"]
            new_desc = spec[code]["description"]
            result[code]["description"] = f"{existing} | {new_desc}"
        else:
            result[code] = spec[code]
    return result


# ======================================================
# Exception Handlers
# ======================================================
async def _api_exception_handler(request: Request, exc: APIException) -> JSONResponse:
    """
    Handler for custom APIException subclasses.

    4xx responses keep their (controlled, business-level) message. For 5xx the
    message and details are logged server-side only and the client gets a
    generic body plus a correlation id (IDX-001).
    """
    if exc.status_code >= 500:
        correlation_id = str(uuid.uuid4())
        logger.error(
            "[%s] %s %s -> %s: %s | details=%s",
            correlation_id, request.method, request.url.path,
            exc.error_code_instance, exc.error, exc.details,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=_sanitized_500(correlation_id, exc.error_code_instance),
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "error": exc.error,
            "error_code": exc.error_code_instance,
            "details": exc.details,
        },
    )


async def _http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """
    Catch-all for any HTTPException that is not an
    APIException (e.g. FastAPI's own 422 validation errors).
    """
    if exc.status_code >= 500:
        correlation_id = str(uuid.uuid4())
        logger.error(
            "[%s] %s %s -> HTTP %s: %s",
            correlation_id, request.method, request.url.path,
            exc.status_code, exc.detail,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=_sanitized_500(correlation_id),
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "error": str(exc.detail),
            "error_code": "BAD_REQUEST",
            "details": None,
        },
    )


async def _generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Last-resort handler for completely unhandled exceptions. The full traceback
    is logged server-side; the client only receives a correlation id (IDX-001).
    """
    correlation_id = str(uuid.uuid4())
    logger.exception(
        "[%s] unhandled exception on %s %s",
        correlation_id, request.method, request.url.path,
    )
    return JSONResponse(
        status_code=500,
        content=_sanitized_500(correlation_id),
    )


def register_exception_handlers(app: FastAPI) -> None:
    """
    Register all custom exception handlers on the FastAPI application. Priority is handled by the order of registration, so more specific handlers should be registered before more generic ones: (1) APIException, (2) HTTPException, (3) Exception.
    """
    app.add_exception_handler(APIException, _api_exception_handler)
    app.add_exception_handler(HTTPException, _http_exception_handler)
    app.add_exception_handler(Exception, _generic_exception_handler)