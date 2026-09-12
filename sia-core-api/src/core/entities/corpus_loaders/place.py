"""
Loader for the PLACE (tenders) corpus.

Author: Lorena Calvo-Bartolomé
Date: 27/03/2023
Modified: 24/01/2024 (Updated for NP-Solr-Service (NextProcurement Project))
Modified: 13/04/2026 (Updated for SIA-Core-API (ALIA Project))
Modified: 12/09/2026 (Extracted into corpus_loaders.place as part of the multi-corpus refactor)

Hay un fichero de metadatos por tipo de tenders (insiders/outsiders/minors) en
"/export/data_ml4ds/alia/place/2025_26/{tender_type}_2526.parquet" de ahí hay
que coger el df["pliegos"], para cada fila sacar el "id_tecnico" (e.g.,
df["pliegos"].iloc[0]["id_tecnico"]).
"""

import gc
import json
from typing import Iterator

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from gensim.corpora import Dictionary

from alia_pipeline.utils.filter_utils import get_cpv_filtered_ids

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


class PlaceCorpusLoader(BaseCorpusLoader):

    def _load_enriched_data(self, tender_type: str) -> pd.DataFrame:
        """
        Reads and concatenates all enriched parquet files from the directory corresponding to tender_type. The join key 'id' is renamed to 'id_tecnico' for the subsequent merge with the base metadata.

        Expected structure:
            <path_source>/metadata/<tender_type>translate/*.parquet
        """
        enriched_dir = self.corpus.path_source / \
            "2025_26/metadata" / f"{tender_type}translate"

        # Read only needed columns to save memory
        COLS_NEEDED = {
            'id', 'id_tecnico',
            'generative_objective_lemmas', 'generative_objective_embeddings',
            'generative_objective', 'semantic_score', 'is_relevant',
            'keyword_counts', 'total_keyword_count',
        }
        BATCH_SIZE = 50

        parquet_files = sorted(
            f for f in enriched_dir.glob("*.parquet")
            if f.is_file() and is_valid_parquet(f)
        )
        if not parquet_files:
            self.logger.warning(
                f"No parquet files found in {enriched_dir}. "
                "Continuing without enriched data for this tender_type."
            )
            return pd.DataFrame()

        self.logger.info(
            f"  Loading {len(parquet_files)} parquet files from '{enriched_dir}' "
            f"in batches of {BATCH_SIZE}..."
        )

        chunks = []
        for batch_start in range(0, len(parquet_files), BATCH_SIZE):
            batch = parquet_files[batch_start:batch_start + BATCH_SIZE]
            batch_dfs = []
            for f in batch:
                try:
                    schema_names = set(pq.read_schema(f).names)
                    cols_to_read = list(COLS_NEEDED & schema_names)
                    batch_dfs.append(pd.read_parquet(f, columns=cols_to_read))
                except Exception:
                    # Log the file name only, not the exception text (IDX-001);
                    # keep the parser detail at DEBUG for troubleshooting.
                    self.logger.warning("Skipping corrupt parquet file '%s'", f.name)
                    self.logger.debug("parquet read error detail", exc_info=True)
            if batch_dfs:
                chunks.append(pd.concat(batch_dfs, ignore_index=True))
                del batch_dfs
                gc.collect()

        if not chunks:
            self.logger.error(f"  All parquet files in {enriched_dir} failed to read.")
            return pd.DataFrame()

        df_enriched = pd.concat(chunks, ignore_index=True)
        del chunks
        gc.collect()

        self.logger.info(
            f"  Loaded enriched parquets from '{enriched_dir}': {len(df_enriched)} rows"
        )

        if "id" in df_enriched.columns:
            df_enriched = df_enriched.rename(columns={"id": "id_tecnico"})

        # Parquet files are incremental: if the same id_tecnico appears in multiple files, we keep the most recent version (last one in sorted order)
        if "id_tecnico" in df_enriched.columns:
            df_enriched = df_enriched.drop_duplicates(
                subset=["id_tecnico"], keep="last")

        #  Procesing of lemmas, embeddings, and computation of BoW and n_words
        if "generative_objective_lemmas" in df_enriched.columns:

            df_enriched["nwords_per_doc"] = df_enriched["generative_objective_lemmas"].apply(
                lambda x: len(x.split()) if isinstance(x, str) else 0
            )
            lemmas_ = df_enriched["generative_objective_lemmas"].apply(
                lambda x: x.split() if isinstance(x, str) else []
            )

            df_enriched["lemmas"] = lemmas_

            dictionary = Dictionary(lemmas_)

            bow = lemmas_.apply(lambda x: dictionary.doc2bow(
                x, allow_update=True) if x else [])
            df_enriched["bow"] = bow.apply(
                lambda x: " ".join(
                    f"{dictionary[wid]}|{count}" for wid, count in x) if x else None
            )

        # Embeddings: transform from space-separated string to list of floats, or from ndarray to list
        if "generative_objective_embeddings" in df_enriched.columns:
            df_enriched["embeddings"] = df_enriched["generative_objective_embeddings"].apply(
                parse_embedding
            )

        # Any other columns that are still np.ndarray
        for col in df_enriched.columns:
            if col != "embeddings":
                df_enriched[col] = df_enriched[col].apply(
                    lambda x: x.tolist() if isinstance(x, np.ndarray) else x
                )

        cols_keep = ['id_tecnico', 'generative_objective', 'lemmas', 'bow', 'nwords_per_doc', 'embeddings', 'semantic_score', 'is_relevant', 'keyword_counts', 'total_keyword_count']

        df_enriched = df_enriched[[col for col in cols_keep if col in df_enriched.columns]]

        self.logger.info(
            f"  Enriched data for '{tender_type}': {len(df_enriched)} unique records"
        )
        return df_enriched

    def get_docs_metadata(self) -> Iterator[dict]:
        """
        Reads the raw corpus file and yields the metadata of each document as a dictionary.
        """
        self.logger.info("Indexing corpus: place")

        corpus = self.corpus
        df_tenders = []
        for tender_type in ["minors", "insiders", "outsiders"]:

            # to index: minors / insiders / outsiders
            dir_meta = corpus.path_source / \
                f"2025_26/{tender_type}_2526.parquet"

            df = pd.read_parquet(dir_meta)
            df["tender_type"] = tender_type

            valid_ids = get_cpv_filtered_ids(
                metadata_parquet=dir_meta, id_field="id")

            self.logger.info(
                f"Filtering corpus to {len(valid_ids)} valid IDs (out of {len(df)})")
            df = df[df[corpus.id_field].isin(valid_ids)].fillna("")

            # Extract id_tecnico from pliegos dict
            df["id_tecnico"] = df["pliegos"].apply(
                lambda x: x.get("id_tecnico") if isinstance(x, dict) else None
            )

            df_enriched = self._load_enriched_data(tender_type)
            if not df_enriched.empty and "id_tecnico" in df_enriched.columns:
                # avoid collisions
                enriched_cols = [
                    c for c in df_enriched.columns
                    if c not in df.columns or c == "id_tecnico"
                ]
                df = df.merge(df_enriched[enriched_cols], on="id_tecnico", how="left")
                self.logger.info(
                    f"  Merged enriched data for '{tender_type}': "
                    f"{df_enriched['id_tecnico'].nunique()} enriched IDs available"
                )

            df_tenders.append(df)

        df = pd.concat(df_tenders, ignore_index=True)

        # Rename fields to canonical names before filtering
        df = df.rename(columns={"TED id": "ted_id"})
        df = alias_common_fields(df, corpus)

        # Keep only the necessary fields
        cols_keep = [
            'id', 'ted_id', 'tender_type', 'id_tecnico', 'title', 'date', 'summary', 'updated', 'link',
            'estado', 'expediente', 'objeto', 'valor_estimado', 'presupuesto_sin_iva',
            'presupuesto_con_iva', 'duracion_dias', 'cpv_list',
            'subentidad_nacional', 'codigo_subentidad_territorial', 'lotes',
            'tipo_procedimiento', 'tramitacion', 'over_threshold', 'organo_nombre',
            'organo_id', 'plazo_presentacion', 'resultado', 'fecha_acuerdo',
            'ofertas_recibidas', 'ofertas_pymes', 'adjudicatario_nombre',
            'identificador',  # formato [["-1", "NIF", "B87222006"]]
            'adjudicatario_pyme', 'adjudicatario_ute',
            'importe_total_sin_iva', 'importe_total_con_iva',
            # enriched fields from df_enriched
            'generative_objective', 'lemmas', 'bow', 'nwords_per_doc',
            'embeddings', 'semantic_score', 'is_relevant',
            'keyword_counts', 'total_keyword_count',
        ]
        cols_keep = [col for col in cols_keep if col in df.columns]
        df = df[cols_keep]
        self.logger.info(f"Columns: {list(df.columns)}")

        # keyword_counts is a list of [keyword, count] pairs; serialize to JSON string for Solr
        if 'keyword_counts' in df.columns:
            df['keyword_counts'] = df['keyword_counts'].apply(
                lambda v: json.dumps(v) if isinstance(v, (list, dict)) else (v if isinstance(v, str) else None)
            )

        # Campos con estructura [["-1", "tipo", "valor"], ...] → "-1|tipo|valor" por elemento
        nested_cols = [
            'resultado', 'fecha_acuerdo', 'ofertas_recibidas', 'ofertas_pymes',
            'adjudicatario_nombre', 'identificador', 'adjudicatario_pyme', 'adjudicatario_ute',
            'importe_total_sin_iva', 'importe_total_con_iva',
        ]
        for col in nested_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda v: parse_list_field(
                    v, serialize_elements=True, sep="|"))

        # lotes: sublistas pueden ser dicts → serializar como JSON strings
        if 'lotes' in df.columns:
            df['lotes'] = df['lotes'].apply(
                lambda v: parse_list_field(v, serialize_elements=True))

        if 'cpv_list' in df.columns:
            df['cpv_list'] = df['cpv_list'].apply(parse_list_field)
            # each element in cpv_list should be a string
            df['cpv_list'] = df['cpv_list'].apply(
                lambda x: [str(i) for i in x] if isinstance(x, list) else x)

        # Parse date columns
        date_cols = [col for col in ["updated",
                                     "plazo_presentacion", "date"] if col in df.columns]
        for col in date_cols:
            df[col] = df[col].map(parse_time_instant)

        # Create SearcheableField
        df["SearcheableField"] = build_searcheable_field(df, corpus)

        yield from (clean_record(rec) for rec in df.to_dict(orient="records"))
