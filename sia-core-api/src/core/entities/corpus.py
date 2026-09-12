"""
This module is a class implementation to manage and hold all the information associated with a logical corpus.

Author: Lorena Calvo-Bartolomé
Date: 27/03/2023
Modified: 24/01/2024 (Updated for NP-Solr-Service (NextProcurement Project))
Modified: 13/04/2026 (Updated for SIA-Core-API (ALIA Project))

Hay un fichero de metadatos por tipo de tenders (insiders/outsiders/minors) en ""/export/data_ml4ds/alia/place/2025_26/{tender_type}_2526.parquet"
de ahí hay que coger el df["pliegos"], para cada fila sacar el "id_tecnico" (e.g., df["pliegos"].iloc[0]["id_tecnico"]).

Entonces, para cada licitación tenemos:
'id'
'id_tecnico'
'title'
'summary'
'updated'
'link'
'estado'
'expediente',
'objeto'
'valor_estimado'
'presupuesto_sin_iva',
'presupuesto_con_iva'
'duracion_dias', 
'cpv_list'
'pliegos', 
'TED id',
'subentidad_nacional'
'codigo_subentidad_territorial'
'lotes'
'tipo_procedimiento'
'tramitacion'
'over_threshold'
'organo_nombre'
'organo_id'
'plazo_presentacion'
'resultado'
'fecha_acuerdo'
'ofertas_recibidas'
'ofertas_pymes'
'adjudicatario_nombre',
'identificador' (formato [["-1", "NIF", "B87222006"]])
'adjudicatario_pyme'
'adjudicatario_ute',
'importe_total_sin_iva'
'importe_total_con_iva'
"""

import ast
import configparser
import gc
import math
from datetime import datetime
import json
from typing import List
from gensim.corpora import Dictionary
import pathlib

import pyarrow.parquet as pq
import pandas as pd
import numpy as np
#from src.core.entities.utils import parseTimeINSTANT


def is_valid_xml_char_ordinal(i):
    """
    Defines whether char is valid to use in xml document
    XML standard defines a valid char as::
    Char ::= #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
    """
    # conditions ordered by presumed frequency
    return (
        0x20 <= i <= 0xD7FF
        or i in (0x9, 0xA, 0xD)
        or 0xE000 <= i <= 0xFFFD
        or 0x10000 <= i <= 0x10FFFF
    )

import pytz
import math
def clean_xml_string(s):
    """
    Cleans string from invalid xml chars
    Solution was found there::
    http://stackoverflow.com/questions/8733233/filtering-out-certain-bytes-in-python
    """
    return "".join(c for c in s if is_valid_xml_char_ordinal(ord(c)))


def parseTimeINSTANT(time):
    """
    Parses a string or datetime-like value representing an instant in time.
    Supports ISO 8601 with timezone (e.g. '2024-12-30T13:52:11.444+01:00'),
    '%Y-%m-%d %H:%M:%S' string formats, and pandas Timestamp / datetime objects.
    """
    import pandas as pd
    # Handle pandas NaT
    if time is pd.NaT or (isinstance(time, float) and math.isnan(time)):
        return clean_xml_string("")
    # Handle pandas Timestamp or Python datetime objects
    if isinstance(time, (pd.Timestamp, datetime)):
        dt = time.to_pydatetime() if isinstance(time, pd.Timestamp) else time
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
        dt_utc = dt.astimezone(pytz.UTC)
        return clean_xml_string(dt_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
    if isinstance(time, str) and time not in ("", "foo"):
        try:
            # ISO 8601 with optional timezone — works for both naive and aware
            dt = datetime.fromisoformat(time)
        except ValueError:
            dt = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
        dt_utc = dt.astimezone(pytz.UTC)
        return clean_xml_string(dt_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
    return clean_xml_string("")

from alia_pipeline.utils.filter_utils import get_cpv_filtered_ids


class Corpus(object):
    """
    A class to manage and hold all the information associated with a logical corpus.
    """

    def __init__(
        self,
        corpus_name: str,
        logger=None,
        config_file: str = "/config/config.cf"
    ) -> None:
        """Init method.

        Parameters
        ----------
        corpus_name: str
            Name of the corpus.
            One out of ["ted", "place", "bdns"].
        logger : logging.Logger
            The logger object to log messages and errors.
        config_file: str
            Path to the configuration file.
        """

        if logger:
            self._logger = logger
        else:
            import logging
            logging.basicConfig(level='INFO')
            self._logger = logging.getLogger('Entity Corpus')

        if corpus_name not in ["ted", "place", "bdns"]:
            self._logger.error(
                f"Corpus name {corpus_name} not in allowed list (ted, place, bdns).")
            raise ValueError(
                f"Corpus name {corpus_name} not in allowed list (ted, place, bdns).")

        self.corpus_name = corpus_name
        self.fields = None

        # Read configuration from config file
        cf = configparser.ConfigParser()
        cf.read(config_file)
        self._logger.info(f"Sections {cf.sections()}")
        if "place" + "-config" in cf.sections():
            section = "place" + "-config"
        else:
            self._logger.error(
                f"Corpus configuration {'place'} not found in config file.")

        self.path_source = pathlib.Path(
            #"/export/data_ml4ds/alia/place"
            cf.get('restapi', 'path_source')
        )
        self.id_field = cf.get(section, "id_field")
        self.title_field = cf.get(section, "title_field")
        # updated o plazo_presentacion
        self.date_field = cf.get(section, "date_field")
        self.MetadataDisplayed = cf.get(
            section, "MetadataDisplayed").split(",")
        self.SearcheableField = cf.get(section, "SearcheableField").split(",")
        if self.title_field in self.SearcheableField:
            self.SearcheableField.remove(self.title_field)
            self.SearcheableField.append("title")
        if self.date_field in self.SearcheableField:
            self.SearcheableField.remove(self.date_field)
            self.SearcheableField.append("date")

        return

    @staticmethod
    def _extract_nested(val, idx=1):
        """Extract a value from a nested [[key, value, ...]] structure."""
        if not val or val == "":
            return None
        try:
            parsed = json.loads(val) if isinstance(val, str) else val
            return parsed[0][idx]
        except (ValueError, IndexError, TypeError):
            return None

    @staticmethod
    def _serialize_element(x):
        """Convert a scalar element to string, formatting dates with parseTimeINSTANT."""
        if isinstance(x, (pd.Timestamp, datetime)):
            return parseTimeINSTANT(x)
        if isinstance(x, str):
            try:
                datetime.fromisoformat(x)
                formatted = parseTimeINSTANT(x)
                return formatted if formatted != "" else x
            except ValueError:
                pass
        return str(x)

    @staticmethod
    def _parse_list_field(val, serialize_elements=False, sep=None):
        """Parse a field that may come as a string repr of a list or already as a list.

        - serialize_elements=True, sep=None  → each sub-element serialized as JSON string
        - serialize_elements=True, sep='|'   → each sub-list joined with sep (no escaping)

        Date-like elements (datetime, pd.Timestamp, or ISO 8601 strings) are formatted
        with parseTimeINSTANT.
        """
        if isinstance(val, (list, np.ndarray)):
            result = list(val)
        elif not isinstance(val, str) or val.strip() in ("", "[]"):
            return []
        else:
            try:
                result = json.loads(val)
            except (ValueError, json.JSONDecodeError):
                try:
                    result = ast.literal_eval(val)
                except (ValueError, SyntaxError):
                    return []
            if not isinstance(result, list):
                return []
        if serialize_elements:
            if sep is not None:
                return [
                    sep.join(Corpus._serialize_element(x) for x in v)
                    if isinstance(v, list) else Corpus._serialize_element(v)
                    for v in result
                ]
            return [json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else Corpus._serialize_element(v) for v in result]
        return [str(v) for v in result]

    def _load_enriched_data(self, tender_type: str) -> pd.DataFrame:
        """
        Reads and concatenates all enriched parquet files from the directory corresponding to tender_type. The join key 'id' is renamed to 'id_tecnico' for the subsequent merge with the base metadata.

        Expected structure:
            <path_source>/metadata/<tender_type>translate/*.parquet
        """
        enriched_dir = self.path_source / \
            "2025_26/metadata" / f"{tender_type}translate"
        def _is_valid_parquet(f) -> bool:
            try:
                with f.open("rb") as fh:
                    header = fh.read(4)
                    if header != b"PAR1":
                        return False
                    fh.seek(-4, 2)
                    footer = fh.read(4)
                    return footer == b"PAR1"
            except Exception:
                return False

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
            if f.is_file() and _is_valid_parquet(f)
        )
        if not parquet_files:
            self._logger.warning(
                f"No parquet files found in {enriched_dir}. "
                "Continuing without enriched data for this tender_type."
            )
            return pd.DataFrame()

        self._logger.info(
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
                    self._logger.warning("Skipping corrupt parquet file '%s'", f.name)
                    self._logger.debug("parquet read error detail", exc_info=True)
            if batch_dfs:
                chunks.append(pd.concat(batch_dfs, ignore_index=True))
                del batch_dfs
                gc.collect()

        if not chunks:
            self._logger.error(f"  All parquet files in {enriched_dir} failed to read.")
            return pd.DataFrame()

        df_enriched = pd.concat(chunks, ignore_index=True)
        del chunks
        gc.collect()

        self._logger.info(
            f"  Loaded enriched parquets from '{enriched_dir}': {len(df_enriched)} rows"
        )

        if "id" in df_enriched.columns:
            df_enriched = df_enriched.rename(columns={"id": "id_tecnico"})

        # Parquet files are incremental: if the same id_tecnico appears in multiple files, we keep the most recent version (last one in sorted order)
        if "id_tecnico" in df_enriched.columns:
            df_enriched = df_enriched.drop_duplicates(
                subset=["id_tecnico"], keep="last")

        #  Procesing of lemmas, embeddings, and computation of BoW and n_words
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
                lambda x: x.tolist() if isinstance(x, np.ndarray)
                else [float(v) for v in x.split()] if isinstance(x, str)
                else x
            )

        # Any other columns that are still np.ndarray
        for col in df_enriched.columns:
            if col != "embeddings":
                df_enriched[col] = df_enriched[col].apply(
                    lambda x: x.tolist() if isinstance(x, np.ndarray) else x
                )

        cols_keep = ['id_tecnico', 'generative_objective', 'lemmas', 'bow', 'nwords_per_doc', 'embeddings', 'semantic_score', 'is_relevant', 'keyword_counts', 'total_keyword_count']
        
        df_enriched = df_enriched[[col for col in cols_keep if col in df_enriched.columns]]
        
        self._logger.info(
            f"  Enriched data for '{tender_type}': {len(df_enriched)} unique records"
        )
        return df_enriched

    def get_docs_metadata(self):
        """
        Reads the raw corpus file and yields the metadata of each document as a dictionary.
        """

        if self.corpus_name == "place":
            self._logger.info("Indexing corpus: place")

            df_tenders = []
            for tender_type in ["minors", "insiders", "outsiders"]:

                # to index: minors / insiders / outsiders
                dir_meta = self.path_source / \
                    f"2025_26/{tender_type}_2526.parquet"

                df = pd.read_parquet(dir_meta)
                df["tender_type"] = tender_type

                valid_ids = get_cpv_filtered_ids(
                    metadata_parquet=dir_meta, id_field="id")

                self._logger.info(
                    f"Filtering corpus to {len(valid_ids)} valid IDs (out of {len(df)})")
                df = df[df[self.id_field].isin(valid_ids)].fillna("")
                
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
                    self._logger.info(
                        f"  Merged enriched data for '{tender_type}': "
                        f"{df_enriched['id_tecnico'].nunique()} enriched IDs available"
                    )                
                    
                df_tenders.append(df)

            df = pd.concat(df_tenders, ignore_index=True)

            # Rename fields to canonical names before filtering
            if "id" in df.columns and "id" != self.id_field:
                df = df.rename(columns={"id": "id_"})
            df = df.rename(columns={self.id_field: "id", "TED id": "ted_id"})
            if self.title_field != "title":
                df["title"] = df[self.title_field]
            if self.date_field != "date":
                df["date"] = df[self.date_field]

            # Build a local copy to avoid mutating instance state
            searcheable = list(self.SearcheableField)
            if self.id_field in searcheable:
                searcheable.remove(self.id_field)
                searcheable.append("id")
            self._logger.info(f"SearcheableField {searcheable}")

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
            self._logger.info(f"Columns: {list(df.columns)}")

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
                    df[col] = df[col].apply(lambda v: self._parse_list_field(
                        v, serialize_elements=True, sep="|"))

            # lotes: sublistas pueden ser dicts → serializar como JSON strings
            if 'lotes' in df.columns:
                df['lotes'] = df['lotes'].apply(
                    lambda v: self._parse_list_field(v, serialize_elements=True))

            if 'cpv_list' in df.columns:
                df['cpv_list'] = df['cpv_list'].apply(self._parse_list_field)
                # each element in cpv_list should be a string
                df['cpv_list'] = df['cpv_list'].apply(
                    lambda x: [str(i) for i in x] if isinstance(x, list) else x)

            # Parse date columns
            date_cols = [col for col in ["updated",
                                         "plazo_presentacion", "date"] if col in df.columns]
            for col in date_cols:
                df[col] = df[col].map(parseTimeINSTANT)

            # Create SearcheableField
            df["SearcheableField"] = df[searcheable].apply(
                lambda x: " ".join(x.astype(str)), axis=1
            )

            def _clean(v):
                if isinstance(v, (float, np.floating)) and (math.isnan(v) or math.isinf(v)):
                    return None
                if isinstance(v, list) and len(v) == 0:
                    return None
                if isinstance(v, dict):
                    return {kk: _clean(vv) for kk, vv in v.items()}
                if isinstance(v, list):
                    return [_clean(i) for i in v]
                return v

            yield from (
                {k: _clean(v) for k, v in rec.items()}
                for rec in df.to_dict(orient="records")
            )

        elif self.corpus_name == "ted":
            raise NotImplementedError(
                "TED corpus processing not implemented yet.")

        elif self.corpus_name == "bdns":
            raise NotImplementedError(
                "BDNS corpus processing not implemented yet.")

    def get_corpora_update(
        self,
        id: int
    ) -> List[dict]:
        """Creates the json to update the 'corpora' collection in Solr with the new logical corpus information.
        """

        fields_dict = [{"id": id,
                        "corpus_name": self.corpus_name,
                        "fields": self.fields,
                        "MetadataDisplayed": self.MetadataDisplayed,
                        "SearcheableFields": self.SearcheableField}]

        return fields_dict


if __name__ == "__main__":
    # Example usage
    corpus = Corpus(corpus_name="place", config_file="/export/usuarios_ml4ds/lbartolome/Repos/alia/alia-sia/sia-config/config.cf")
    # run for all documents
    for doc in corpus.get_docs_metadata():
        print(doc)
        import pdb; pdb.set_trace()
