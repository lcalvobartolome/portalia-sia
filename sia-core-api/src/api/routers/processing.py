"""
Enrichment and Data Ingestion (Processing APIs)

Set of services that orchestrate and trigger the processing pipeline:
- Ingestion and processing trigger: download, text extraction, normalization
- AI inference services: summaries, topic models, embeddings
- On-demand inference: real-time processing of external documents

Endpoints can be executed:
1. Sequentially within the ingestion pipeline (configuration controlled)
2. Independently for on-demand calculations

Response conventions:
- Single endpoints return a typed response extending ResponseBase (e.g. TextExtractionSingleResponse).
- Batch endpoints return BatchProcessingResponse (success/message + job_id).
- All error responses use ErrorResponse.

Author: Lorena Calvo-Bartolome
Date: 27/03/2023
Modified: 04/02/2026 (Migrated to FastAPI and reorganized)
"""

import asyncio
import logging
import os
import sqlite3
import uuid
import json
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, Body, Path as PathParam, Request  # type: ignore

from src.api.exceptions import (
    APIException,
    ConflictException,
    NotFoundException,
    ProcessingException,
    SolrException,
    ValidationException,
    error_responses,
    raise_internal_processing,
    raise_internal_solr,
)
from src.api.schemas import (
    BatchProcessingResponse,
    CorpusIndexRequest,
    CorpusListResponse,
    ExtractPipelineRequest,
    IndexingResponse,
    PipelineJobStatus,
    PipelineProgressResponse,
    ResponseBase,
    TrainPipelineRequest,
)

logger = logging.getLogger(__name__)

# ======================================================
# Router
# ======================================================
router = APIRouter(
    prefix="/processing",
    tags=["2. Data Enrichment and Ingestion"],
)

# ======================================================
# Constants
# ======================================================
VALID_TIPOS = {"minors", "outsiders", "insiders"}


# ======================================================
# SQLite job store
# ======================================================
_DB_PATH = Path(os.environ.get("PIPELINE_DB_PATH", "/mnt/data/pipeline_jobs.db"))


def _conn() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create the jobs table if it does not exist. Call this at app startup."""
    with _conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_jobs (
                job_id      TEXT PRIMARY KEY,
                mode        TEXT NOT NULL,
                tipo        TEXT,
                status      TEXT NOT NULL DEFAULT 'running',
                started_at  TEXT NOT NULL,
                finished_at TEXT,
                returncode  INTEGER,
                data_dir    TEXT,
                pid         INTEGER,
                params      TEXT
            )
        """)


def _upsert_job(job_id: str, **fields) -> None:
    """Insert a new job row or update specific fields on an existing one."""
    with _conn() as conn:
        exists = conn.execute(
            "SELECT 1 FROM pipeline_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if not exists:
            fields["job_id"] = job_id
            cols = ", ".join(fields.keys())
            placeholders = ", ".join("?" * len(fields))
            conn.execute(
                f"INSERT INTO pipeline_jobs ({cols}) VALUES ({placeholders})",
                list(fields.values()),
            )
        else:
            set_clause = ", ".join(f"{k}=?" for k in fields)
            conn.execute(
                f"UPDATE pipeline_jobs SET {set_clause} WHERE job_id=?",
                [*fields.values(), job_id],
            )


def _get_job(job_id: str) -> Optional[dict]:
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM pipeline_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        return dict(row) if row else None


def _list_jobs() -> List[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM pipeline_jobs ORDER BY started_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]


# ======================================================
# Internal pipeline helpers
# ======================================================
def _cpv_args(params: dict) -> List[str]:
    """Return --metadata-parquet args if the parquet file exists."""
    parquet = params.get("metadata_parquet")
    if parquet and Path(parquet).is_file():
        return ["--metadata-parquet", parquet]
    return []


def _metadata_parquet(base_dir: str, tipo: str) -> str:
    return f"{base_dir}/{tipo}_2526.parquet"


def _snapshot_mtimes(data_dir: str) -> dict:
    """Capture {filename: mtime} for all part*.parquet files at job start."""
    return {
        f.name: f.stat().st_mtime
        for f in Path(data_dir).glob("part*.parquet")
    }


async def _run_step(job_id: str, step_name: str, cmd: List[str]) -> bool:
    _upsert_job(job_id, status=f"running:{step_name}")
    logger.info(f"[{job_id}] Starting step '{step_name}': {' '.join(cmd)}")

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    # Read and log line by line in real time
    async for line in proc.stdout:
        decoded = line.decode(errors="replace").rstrip()
        if decoded:
            logger.info(f"[{job_id}][{step_name}] {decoded}")

    await proc.wait()

    if proc.returncode != 0:
        logger.error(f"[{job_id}] Step '{step_name}' failed (rc={proc.returncode})")
        _upsert_job(
            job_id,
            status="failed",
            finished_at=datetime.utcnow().isoformat(),
            returncode=proc.returncode,
        )
        return False

    logger.info(f"[{job_id}] Step '{step_name}' completed OK")
    return True


async def _run_extract_pipeline(job_id: str, params: dict) -> None:
    """extract mode: alia-extract-objectives → alia-nlp-process → alia-extract-relevance"""
    data_dir = params["data_dir"]
    cpv = _cpv_args(params)
    base = [
        "-p", data_dir + "/",
        "--file-workers", str(params["file_workers"]),
        "--row-workers", str(params["row_workers"]),
    ]

    steps = [
        ("extract-objectives", [
            "alia-extract-objectives", *base,
            "--calculate-on", params["calculate_on"],
            "--mode", "generative",
            "--llm-model-gen", params["llm_model_gen"],
            "--ollama_host", params["ollama_host"],
            *cpv,
        ]),
        ("nlp-process", [
            "alia-nlp-process", *base,
            "--text-cols", "generative_objective",
            "--embeddings-model", params["embed_model"],
            *cpv,
        ]),
        ("extract-relevance", [
            "alia-extract-relevance", *base,
            "--calculate-on", "generative_objective",
            "--semantic-threshold", str(params["semantic_threshold"]),
            "--embeddings-col", "generative_objective_embeddings",
            "--keywords",
            "--force",
            *cpv,
        ]),
    ]

    for step_name, cmd in steps:
        if not await _run_step(job_id, step_name, cmd):
            return

    _upsert_job(job_id, status="completed", finished_at=datetime.utcnow().isoformat(), returncode=0)


async def _run_train_pipeline(job_id: str, params: dict) -> None:
    """train mode: alia-tm-train for each requested tipo."""
    base_dir = params["base_dir"]
    tipos = [t.strip() for t in params["train_tipos"].split(":") if t.strip()]

    for tipo in tipos:
        tipo_dir = f"{base_dir}/metadata/{tipo}translate"
        model_path = f"{base_dir}/metadata/{tipo}translate/model"
        parquet = _metadata_parquet(base_dir, tipo)
        cpv = ["--metadata-parquet", parquet] if Path(parquet).is_file() else []

        cmd = [
            "alia-tm-train",
            "-p", tipo_dir + "/",
            "--model-path", model_path,
            "--mallet-path", params["mallet"],
            "--text-col", "generative_objective_lemmas",
            "--ntopics", str(params["ntopics"]),
            "--num-iterations", str(params["num_iterations"]),
            *cpv,
        ]

        if not await _run_step(job_id, f"tm-train:{tipo}", cmd):
            return

    _upsert_job(job_id, status="completed", finished_at=datetime.utcnow().isoformat(), returncode=0)


async def _run_infer_pipeline(job_id: str, params: dict) -> None:
    """infer mode: extract-objectives → nlp-process → extract-relevance → tm-infer"""
    data_dir = params["data_dir"]
    model_dir = f"{params['base_dir']}/metadata/{params['tipo']}translate/model"
    cpv = _cpv_args(params)
    base = [
        "-p", data_dir + "/",
        "--file-workers", str(params["file_workers"]),
        "--row-workers", str(params["row_workers"]),
    ]

    steps = [
        ("extract-objectives", [
            "alia-extract-objectives", *base,
            "--calculate-on", params["calculate_on"],
            "--mode", "generative",
            "--llm-model-gen", params["llm_model_gen"],
            *cpv,
        ]),
        ("nlp-process", [
            "alia-nlp-process", *base,
            "--text-cols", "generative_objective",
            "--embeddings-model", params["embed_model"],
            *cpv,
        ]),
        ("extract-relevance", [
            "alia-extract-relevance", *base,
            "--calculate-on", "generative_objective",
            "--semantic-threshold", str(params["semantic_threshold"]),
            "--embeddings-col", "generative_objective_embeddings",
            "--keywords",
            *cpv,
        ]),
        ("tm-infer", [
            "alia-tm-infer", *base,
            "--model-path", model_dir,
            "--mallet-path", params["mallet"],
            "--text-col", "generative_objective_lemmas",
            *cpv,
        ]),
    ]

    for step_name, cmd in steps:
        if not await _run_step(job_id, step_name, cmd):
            return

    _upsert_job(job_id, status="completed", finished_at=datetime.utcnow().isoformat(), returncode=0)


def _compute_progress(data_dir: str, mtime_snapshot: dict) -> dict:
    tgt_dir = Path(data_dir)
    files = list(tgt_dir.glob("part*.parquet"))
    total = len(files)

    if total == 0:
        return dict(files_total=0, files_modified=0, by_day=[])

    records = []
    for f in files:
        current_mtime = f.stat().st_mtime
        original_mtime = mtime_snapshot.get(f.name, None)
        # Only count as modified if we have a snapshot entry AND mtime changed
        if original_mtime is not None and current_mtime > original_mtime:
            records.append((f.name, date.fromtimestamp(current_mtime)))

    by_day_rows: List[dict] = []
    if records:
        df_done = pd.DataFrame(records, columns=["fichero", "fecha"])
        by_day = (
            df_done.groupby("fecha")
            .size()
            .reset_index(name="modificados_ese_dia")
            .sort_values("fecha")
        )
        by_day["acumulado"] = by_day["modificados_ese_dia"].cumsum()
        by_day["pct_sobre_total"] = (by_day["acumulado"] / total * 100).round(1)
        by_day["fecha"] = by_day["fecha"].astype(str)
        by_day_rows = by_day.to_dict(orient="records")

    return dict(
        files_total=total,
        files_modified=len(records),
        by_day=by_day_rows,
    )


def _job_to_status(job: dict) -> PipelineJobStatus:
    return PipelineJobStatus(
        job_id=job["job_id"],
        mode=job["mode"],
        tipo=job.get("tipo"),
        status=job["status"],
        started_at=job["started_at"],
        finished_at=job.get("finished_at"),
        returncode=job.get("returncode"),
        data_dir=job.get("data_dir"),
    )


# ======================================================
# Corpus Management
# ======================================================
@router.post(
    "/corpora",
    response_model=IndexingResponse,
    status_code=201,
    summary="Index corpus",
    description="Index a corpus from a parquet file.",
    responses=error_responses(
        ConflictException, ProcessingException,
        ConflictException="Corpus already exists",
    ),
)
async def index_corpus(
    request: Request,
    body: CorpusIndexRequest = Body(...),
) -> IndexingResponse:
    sc = request.app.state.solr_client
    try:
        sc.index_corpus(body.corpus_name)
        return IndexingResponse(
            success=True,
            message=f"Corpus '{body.corpus_name}' indexed successfully",
            status="completed",
        )
    except APIException:
        raise
    except Exception as e:
        raise_internal_processing(e, log=logger)


@router.delete(
    "/corpora/{corpus_name}",
    response_model=ResponseBase,
    summary="Delete corpus",
    description="Delete a corpus and all its associated data from the system.",
    responses=error_responses(
        NotFoundException, ProcessingException,
        NotFoundException="Corpus not found",
    ),
)
async def delete_corpus(
    request: Request,
    corpus_name: str = PathParam(..., description="Name of the corpus to delete"),
) -> ResponseBase:
    sc = request.app.state.solr_client
    try:
        sc.delete_corpus(corpus_name)
        return ResponseBase(success=True, message=f"Corpus '{corpus_name}' deleted successfully")
    except APIException:
        raise
    except Exception as e:
        raise_internal_processing(e, log=logger)


# ======================================================
# Model Management
# ======================================================
# @router.post(
#     "/models",
#     response_model=IndexingResponse,
#     status_code=201,
#     summary="Index topic model",
#     description="Index a trained topic model in Solr.",
#     responses=error_responses(
#         ConflictException, ProcessingException,
#         ConflictException="Model already exists",
#     ),
# )
# async def index_model(
#     request: Request,
#     body: ModelIndexRequest = Body(...),
# ) -> IndexingResponse:
#     sc = request.app.state.solr_client
#     try:
#         sc.index_model(body.model_name)
#         return IndexingResponse(
#             success=True,
#             message=f"Model '{body.model_name}' indexed successfully",
#             status="completed",
#         )
#     except APIException:
#         raise
#     except Exception as e:
#         raise ProcessingException(str(e))


# @router.delete(
#     "/models/{model_name}",
#     response_model=ResponseBase,
#     summary="Delete topic model",
#     description="Delete a topic model from the system.",
#     responses=error_responses(
#         NotFoundException, ProcessingException,
#         NotFoundException="Model not found",
#     ),
# )
# async def delete_model(
#     request: Request,
#     model_name: str = PathParam(..., description="Name of the model to delete"),
# ) -> ResponseBase:
#     sc = request.app.state.solr_client
#     try:
#         sc.delete_model(model_name)
#         return ResponseBase(success=True, message=f"Model '{model_name}' deleted successfully")
#     except APIException:
#         raise
#     except Exception as e:
#         raise ProcessingException(str(e))


# ======================================================
# Alia Pipeline: extract / train / infer
# ======================================================
@router.post(
    "/alia-pipeline/extract",
    response_model=BatchProcessingResponse,
    status_code=202,
    summary="Run alia extract pipeline",
    description=(
        "Launches **alia-extract-objectives → alia-nlp-process → alia-extract-relevance** "
        "in the background. Returns immediately with a `job_id`. "
        "Poll `/alia-pipeline/status/{job_id}` or `/alia-pipeline/progress/{job_id}` "
        "to track execution. Job state is persisted in SQLite."
    ),
    responses=error_responses(ValidationException, ProcessingException),
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "example": {
                        "base_dir": "/mnt/data/2025_26",
                        "tipo": "insiders",
                        "calculate_on": "texto_traducido",
                        "llm_model_gen": "qwen3:32b",
                        "embed_model": "hiiamsid/sentence_similarity_spanish_es",
                        "file_workers": 1,
                        "row_workers": 4,
                        "semantic_threshold": 0.6,
                        "mallet": "/opt/bin/mallet",
                    }
                }
            }
        }
    },
)
async def run_alia_extract(
    body: ExtractPipelineRequest = Body(...),
) -> BatchProcessingResponse:
    base_dir = body.base_dir.rstrip("/")
    data_dir = f"{base_dir}/metadata/{body.tipo}translate"
    job_id = f"extract_{uuid.uuid4().hex[:8]}"

    # Snapshot of mtimes BEFORE launching the pipeline
    snapshot = _snapshot_mtimes(data_dir)

    params = dict(
        base_dir=base_dir,
        tipo=body.tipo,
        data_dir=data_dir,
        metadata_parquet=_metadata_parquet(base_dir, body.tipo),
        calculate_on=body.calculate_on,
        llm_model_gen=body.llm_model_gen,
        embed_model=body.embed_model,
        file_workers=body.file_workers,
        row_workers=body.row_workers,
        semantic_threshold=body.semantic_threshold,
        mallet=body.mallet,
        ollama_host=body.ollama_host,
    )

    _upsert_job(
        job_id,
        mode="extract",
        tipo=body.tipo,
        status="running",
        started_at=datetime.utcnow().isoformat(),
        data_dir=data_dir,
        params=json.dumps({"mtime_snapshot": snapshot}),
    )
    asyncio.create_task(_run_extract_pipeline(job_id, params))

    return BatchProcessingResponse(
        success=True,
        message=f"Extract pipeline started for tipo='{body.tipo}' (data_dir={data_dir})",
        job_id=job_id,
        status="running",
    )


@router.post(
    "/alia-pipeline/train",
    response_model=BatchProcessingResponse,
    status_code=202,
    summary="Run alia train pipeline",
    description=(
        "Launches **alia-tm-train** for each requested tipo in the background. "
        "`train_tipos` accepts a colon-separated list, e.g. `minors:outsiders:insiders`. "
        "Returns immediately with a `job_id`."
    ),
    responses=error_responses(ValidationException, ProcessingException),
)
async def run_alia_train(
    body: TrainPipelineRequest = Body(...),
) -> BatchProcessingResponse:
    tipos_list = [t.strip() for t in body.train_tipos.split(":") if t.strip()]
    invalid = [t for t in tipos_list if t not in VALID_TIPOS]
    if invalid:
        raise ValidationException(
            f"Invalid tipo(s): {invalid}. Valid values: {sorted(VALID_TIPOS)}"
        )

    base_dir = body.base_dir.rstrip("/")
    job_id = f"train_{uuid.uuid4().hex[:8]}"

    params = dict(
        base_dir=base_dir,
        train_tipos=body.train_tipos,
        ntopics=body.ntopics,
        num_iterations=body.num_iterations,
        mallet=body.mallet,
    )

    _upsert_job(
        job_id,
        mode="train",
        tipo=body.train_tipos,
        status="running",
        started_at=datetime.utcnow().isoformat(),
    )
    asyncio.create_task(_run_train_pipeline(job_id, params))

    return BatchProcessingResponse(
        success=True,
        message=(
            f"Train pipeline started for tipos='{body.train_tipos}' "
            f"(ntopics={body.ntopics}, iterations={body.num_iterations})"
        ),
        job_id=job_id,
        status="running",
    )


@router.post(
    "/alia-pipeline/infer",
    response_model=BatchProcessingResponse,
    status_code=202,
    summary="Run alia infer pipeline",
    description=(
        "Launches **alia-extract-objectives → alia-nlp-process → "
        "alia-extract-relevance → alia-tm-infer** in the background. "
        "Returns immediately with a `job_id`."
    ),
    responses=error_responses(ValidationException, ProcessingException),
)
async def run_alia_infer(
    body: ExtractPipelineRequest = Body(...),
) -> BatchProcessingResponse:
    base_dir = body.base_dir.rstrip("/")
    data_dir = f"{base_dir}/metadata/{body.tipo}translate"
    job_id = f"infer_{uuid.uuid4().hex[:8]}"

    # Snapshot of mtimes BEFORE launching the pipeline
    snapshot = _snapshot_mtimes(data_dir)

    params = dict(
        base_dir=base_dir,
        tipo=body.tipo,
        data_dir=data_dir,
        metadata_parquet=_metadata_parquet(base_dir, body.tipo),
        calculate_on=body.calculate_on,
        llm_model_gen=body.llm_model_gen,
        embed_model=body.embed_model,
        file_workers=body.file_workers,
        row_workers=body.row_workers,
        semantic_threshold=body.semantic_threshold,
        mallet=body.mallet,
    )

    _upsert_job(
        job_id,
        mode="infer",
        tipo=body.tipo,
        status="running",
        started_at=datetime.utcnow().isoformat(),
        data_dir=data_dir,
        params=json.dumps({"mtime_snapshot": snapshot}),
    )
    asyncio.create_task(_run_infer_pipeline(job_id, params))

    return BatchProcessingResponse(
        success=True,
        message=f"Infer pipeline started for tipo='{body.tipo}' (data_dir={data_dir})",
        job_id=job_id,
        status="running",
    )


# ======================================================
# Job status & progress endpoints
# ======================================================
@router.get(
    "/alia-pipeline/jobs",
    response_model=List[PipelineJobStatus],
    summary="List all pipeline jobs",
    description=(
        "Returns all pipeline jobs ordered by start time descending. "
        "Persisted in SQLite — survives server restarts."
    ),
)
async def list_pipeline_jobs() -> List[PipelineJobStatus]:
    return [_job_to_status(j) for j in _list_jobs()]


@router.get(
    "/alia-pipeline/status/{job_id}",
    response_model=PipelineJobStatus,
    summary="Get pipeline job status",
    description=(
        "Returns the current status of a pipeline job. "
        "Possible values: `running:<step>` | `completed` | `failed`."
    ),
    responses=error_responses(NotFoundException),
)
async def get_pipeline_status(
    job_id: str = PathParam(..., description="Job ID returned when the pipeline was started"),
) -> PipelineJobStatus:
    job = _get_job(job_id)
    if job is None:
        raise NotFoundException(f"Job '{job_id}' not found")
    return _job_to_status(job)


@router.get(
    "/alia-pipeline/progress/{job_id}",
    response_model=PipelineProgressResponse,
    summary="Get pipeline processing progress",
    description=(
        "Returns processing progress by inspecting modification timestamps of "
        "`part*.parquet` files in the job's `data_dir` against a snapshot taken "
        "at job start. Only files whose mtime changed since the snapshot count as modified. "
        "Only available for `extract` and `infer` modes."
    ),
    responses=error_responses(NotFoundException, ProcessingException),
)
async def get_pipeline_progress(
    job_id: str = PathParam(..., description="Job ID returned when the pipeline was started"),
) -> PipelineProgressResponse:
    job = _get_job(job_id)
    if job is None:
        raise NotFoundException(f"Job '{job_id}' not found")

    data_dir = job.get("data_dir")
    if not data_dir:
        raise ProcessingException(
            f"Progress tracking is not available for mode='{job['mode']}'"
        )

    params = json.loads(job.get("params") or "{}")
    mtime_snapshot = params.get("mtime_snapshot", {})

    try:
        progress = _compute_progress(data_dir, mtime_snapshot)
    except Exception as e:
        raise_internal_processing(e, context="compute_progress", log=logger)

    return PipelineProgressResponse(
        job_id=job_id,
        mode=job["mode"],
        tipo=job.get("tipo"),
        status=job["status"],
        started_at=job["started_at"],
        data_dir=data_dir,
        **progress,
    )
    
# ======================================================
# Corpora Management
# ======================================================
@router.get(
    "/corpora",
    response_model=CorpusListResponse,
    summary="List all corpora",
    description="Returns the list of all corpora indexed in the system.",
    responses=error_responses(SolrException),
)
async def list_all_corpora(
    request: Request
) -> CorpusListResponse:
    """List all available corpora."""
    sc = request.app.state.solr_client
    try:
        corpus_lst, code = sc.list_corpus_collections()
        if code != 200:
            raise SolrException(f"Error listing corpora (code: {code})")
        return CorpusListResponse(
            success=True,
            corpora=corpus_lst
        )
    except APIException:
        raise
    except Exception as e:
        raise_internal_solr(e, log=logger)


# @router.get(
#     "/corpora/{corpus_col}/models",
#     response_model=CorpusModelsResponse,
#     summary="List topic models of a corpus",
#     description="Lists all topic models associated with a specific corpus.",
#     responses=error_responses(
#         NotFoundException, SolrException,
#         NotFoundException="Corpus not found",
#     ),
# )
# async def list_corpus_models(
#     request: Request,
#     corpus_col: str = Path(..., description="Name of the corpus collection"),
# ) -> CorpusModelsResponse:
#     """List models associated with a corpus."""
#     sc = request.app.state.solr_client
#     try:
#         models_lst, code = sc.get_corpus_models(corpus_col=corpus_col)
#         if code != 200:
#             raise SolrException(f"Error getting models (code: {code})")
                
#         return CorpusModelsResponse(
#             success=True,
#             models=models_lst
#         )
#     except APIException:
#         raise
#     except Exception as e:
#         raise SolrException(str(e))


# ======================================================
# Models Management
# ======================================================
# @router.get(
#     "/models",
#     response_model=ModelsListResponse,
#     summary="List all models",
#     description="Lists all topic models available in the system.",
#     responses=error_responses(SolrException),
# )
# async def list_all_models(
#     request: Request
# ) -> ModelsListResponse:
#     """List all topic models."""
#     sc = request.app.state.solr_client
#     try:
#         models_lst, code = sc.list_model_collections()
#         if code != 200:
#             raise SolrException(f"Error listing models (code: {code})")
        
#         return ModelsListResponse(
#             success=True,
#             models=models_lst
#         )
#     except APIException:
#         raise
#     except Exception as e:
#         raise SolrException(str(e))