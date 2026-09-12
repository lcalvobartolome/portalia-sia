"""
Loader for the BDNS (convocatorias de ayudas públicas / BDNS-SNPSAP) corpus.

Source layout: a flat directory of *.parquet files (one per monthly page of
the BDNS API pull), consolidated and deduplicated by codigo_bdns. Unlike
PLACE, the NLP enrichment (lemmas/embeddings/relevance) is already computed
upstream and shipped inside the same parquet files, under corpus-specific
column names (descripcion_norm_lemmas, descripcion_norm_embeddings, ...)
that this loader maps onto the corpus-agnostic Solr fields (lemmas, embeddings, ...) shared with PLACE.

Author: Lorena Calvo-Bartolomé
Date: 12/09/2026
"""

import json
from typing import Iterator

from .base import BaseCorpusLoader
from .utils import (
    alias_common_fields,
    build_searcheable_field,
    clean_record,
    is_valid_parquet,
    parse_embedding,
    parse_list_field,
    parse_time_instant,
)

import pandas as pd

LIST_FIELDS = ["sectores", "sector_seccion",
               "sector_detalle", "regiones", "tipos_beneficiarios"]
DATE_FIELDS = ["fecha_registro", "fecha_inicio_solicitud",
               "fecha_fin_solicitud", "plazo_fin_efectivo"]
# Columns straight from the BDNS parquet that are indexed as-is (see managed-schema.xml)
BASE_COLS = [
    "id", "codigo_bdns", "url_convocatoria",
    "descripcion", "descripcion_norm", "descripcion_leng",
    "bases_reguladoras", "url_bases_reguladoras", "sede_electronica",
    "organo_ambito", "organo_entidad", "organo_unidad",
    "finalidad", "tipo_convocatoria",
    *LIST_FIELDS,
    "mrr",
    *DATE_FIELDS,
    "texto_inicio", "texto_fin", "plazo_origen", "abierto",
    "presupuesto_total", "fuente", "_snapshot_date", "_query_params",
]
# Columns produced from the enrichment step already baked into the parquet
ENRICHED_COLS = [
    "lemmas", "embeddings", "nwords_per_doc",
    "semantic_score", "is_relevant", "keyword_counts", "total_keyword_count",
]
# Free-text columns known to carry occasional UTF-8/Latin-1 mojibake at the source
MOJIBAKE_COLS = ["url_bases_reguladoras", "descripcion"]


def _fix_mojibake(s):
    """Best-effort repair of UTF-8 bytes that were mis-decoded as Latin-1 upstream."""
    if not isinstance(s, str):
        return s
    try:
        return s.encode("latin1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return s


class BdnsCorpusLoader(BaseCorpusLoader):

    def get_docs_metadata(self) -> Iterator[dict]:
        """Reads every *.parquet file under path_source, consolidates them into a single deduplicated frame and yields the metadata of each convocatoria as a dictionary.
        """
        self.logger.info("Indexing corpus: bdns")

        corpus = self.corpus
        parquet_files = sorted(
            f for f in corpus.path_source.glob("*.parquet")
            if f.is_file() and is_valid_parquet(f)
        )
        skipped = sorted(
            f.name for f in corpus.path_source.glob("*.parquet")
            if f.name not in {p.name for p in parquet_files}
        )
        if skipped:
            self.logger.warning(f"Skipping corrupt parquet file(s): {skipped}")
        if not parquet_files:
            self.logger.warning(
                f"No parquet files found in {corpus.path_source}.")
            return

        df = pd.concat(
            (pd.read_parquet(f) for f in parquet_files), ignore_index=True)

        if "codigo_bdns" in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=["codigo_bdns"], keep="last")
            self.logger.info(
                f"Loaded {before} rows from {len(parquet_files)} parquet files, "
                f"{len(df)} unique after deduplication by codigo_bdns"
            )

        for col in LIST_FIELDS:
            if col in df.columns:
                df[col] = df[col].apply(parse_list_field)

        for col in DATE_FIELDS:
            if col in df.columns:
                df[col] = df[col].map(parse_time_instant)

        for col in MOJIBAKE_COLS:
            if col in df.columns:
                df[col] = df[col].apply(_fix_mojibake)

        # Enrichment already computed upstream: map corpus-specific source columns onto the corpus-agnostic Solr fields.
        if "descripcion_norm_lemmas" in df.columns:
            df["lemmas"] = df["descripcion_norm_lemmas"].apply(
                lambda x: x.split() if isinstance(x, str) else [])
            df["nwords_per_doc"] = df["lemmas"].apply(len)

        if "descripcion_norm_embeddings" in df.columns:
            df["embeddings"] = df["descripcion_norm_embeddings"].apply(
                parse_embedding)

        if "keyword_counts" in df.columns:
            df["keyword_counts"] = df["keyword_counts"].apply(
                lambda v: json.dumps(v) if isinstance(
                    v, (list, dict)) else (v if isinstance(v, str) else None)
            )

        df = alias_common_fields(df, corpus)

        cols_keep = [c for c in [*BASE_COLS, "title", "date", *ENRICHED_COLS]
                     if c in df.columns]
        df = df[cols_keep]
        self.logger.info(f"Columns: {list(df.columns)}")

        df["SearcheableField"] = build_searcheable_field(df, corpus)

        yield from (clean_record(rec) for rec in df.to_dict(orient="records"))
