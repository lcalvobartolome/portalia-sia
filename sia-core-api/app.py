"""
SIA-Core API: RESTful API of the SIA-Core system, which is organized into three functional blocks:
1. Infrastructure Administration - Technical management and configuration
2. Data Enrichment and Ingestion - Processing pipeline orchestration
3. Exploitation Services - Public consumption by the AI Portal

Author: Lorena Calvo-Bartolomé
Date: 27/03/2023
Modified: 04/02/2026 (Migrated to FastAPI and reorganized)
"""

import logging
import os
import pathlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator

from fastapi import Depends, FastAPI, HTTPException, Request  # type: ignore
from fastapi.middleware.cors import CORSMiddleware  # type: ignore
from fastapi.openapi.utils import get_openapi  # type: ignore
from src.api.exceptions import register_exception_handlers
from src.api.routers.admin import router as admin_router
from src.api.routers.processing import router as processing_router
from src.api.routers.services import router as exploitation_router
from src.api.schemas import HealthResponse
from src.core.clients.sia_solr_client import SIASolrClient
from src.api.auth import verify_api_key, API_KEY_HEADER_NAME


# ======================================================
# Loaders (version, description, tags)
# ======================================================
def load_version() -> str:
    """Load application version from file."""
    version_path = pathlib.Path(__file__).parent / "docs" / "version.txt"
    try:
        return version_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return "1.0.0"


def load_api_description() -> str:
    """Load API description from markdown file."""
    docs_path = pathlib.Path(__file__).parent / "docs" / "api_description.md"
    try:
        return docs_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return "SIA-Core API - Sistema de Inteligencia y Análisis"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SIA-Core-API")

# get version
VERSION = load_version()

# ======================================================
# Application lifespan
# ======================================================
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager for startup and shutdown events."""
    logger.info("Starting SIA-Core API...")

    try:
        config_path = pathlib.Path(__file__).parent / "config" / "config.cf"
        app.state.solr_client = SIASolrClient(
            logger, config_file=str(config_path))
        logger.info("Solr client initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing Solr client: {e}")
        raise

    yield

    logger.info("Closing SIA-Core API...")


# ======================================================
# Application Configuration
# ======================================================
app = FastAPI(
    title="SIA-Core API",
    description=load_api_description(),
    version=VERSION,
    contact={
        "name": "Lorena Calvo-Bartolomé",
        "email": "lcalvo@pa.uc3m.es",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)


# ======================================================
# CORS Middleware
# ======================================================
# Define allowed origins (add your frontend URLs here)
ALLOWED_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:8080").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,  # Specific origins instead of "*"
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*", API_KEY_HEADER_NAME],  # Allow API key header
    expose_headers=["X-Request-ID"],
)


# ======================================================
# Exception Handlers
# ======================================================
register_exception_handlers(app)


# ======================================================
# Routers (with API key authentication)
# ======================================================
# All routers require API key authentication
# Note: Master key also works as a valid API key
app.include_router(admin_router, dependencies=[Depends(verify_api_key)])
app.include_router(processing_router, dependencies=[Depends(verify_api_key)])
app.include_router(exploitation_router, dependencies=[Depends(verify_api_key)])

# ======================================================
# Root and Health Endpoints
# ======================================================
@app.get(
    "/",
    summary="API Root",
    description="Basic API information and documentation links.",
    tags=["Health"],
)
async def root():
    """Root endpoint with basic API information and documentation links."""
    return {
        "name": "SIA-Core API",
        "version": VERSION,
        "description": "API RESTful of the  'Sistema de Inteligencia y Análisis de Contratación y Ayudas Públicas (SIA)'.",
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc",
            "openapi": "/openapi.json"
        },
        "blocks": {
            "1_admin": "/admin - Infrastructure Administration",
            "2_processing": "/processing - Enrichment and Ingestion",
            "3_exploitation": "/api - Exploitation Services"
        }
    }


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health Check",
    description="Health check including Solr connectivity status.",
    tags=["Health"],
)
async def health_check(request: Request) -> HealthResponse:
    """Health check with Solr status."""
    solr_connected = False
    try:
        sc = request.app.state.solr_client
        # Try to list collections as a connectivity test
        sc.list_collections()
        solr_connected = True
    except Exception:
        solr_connected = False

    return HealthResponse(
        status="healthy" if solr_connected else "unhealthy",
        timestamp=datetime.now(timezone.utc).isoformat(),
        solr_connected=solr_connected,
        version=VERSION
    )


# ======================================================
# Custom OpenAPI Schema
# ======================================================
def custom_openapi():
    """Generate custom OpenAPI schema with additional metadata."""
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        contact=app.contact,
        license_info=app.license_info,
    )
    
    # Add servers
    openapi_schema["servers"] = [
        {"url": "/", "description": "Current server"},
    ]

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


# ======================================================
# Main Entry Point
# ======================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=10083,
        reload=True,
        log_level="info",
    )
