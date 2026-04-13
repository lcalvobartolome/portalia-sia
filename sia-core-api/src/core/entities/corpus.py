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

# def process_partition(partition):
#     """Processes a single partition of the dataframe"""
#     partition["nwords_per_doc"] = partition["lemmas"].apply(lambda x: len(x.split()))
#     partition["lemmas_"] = partition["lemmas"].apply(lambda x: x.split() if isinstance(x, str) else [])
    
#     # Convert to BoW representation
#     partition['bow'] = partition["lemmas_"].apply(
#         lambda x: dictionary.doc2bow(x, allow_update=True) if x else []
#     )
#     partition['bow'] = partition['bow'].apply(
#         lambda x: [(dictionary[id], count) for id, count in x] if x else []
#     )
#     partition['bow'] = partition['bow'].apply(lambda x: ' '.join([f'{word}|{count}' for word, count in x]) if x else None)
    
#     partition = partition.drop(['lemmas_'], axis=1)

#     # Convert embeddings (assume space-separated numbers)
#     """
#     partition["embeddings"] = partition["embeddings"].apply(
#         lambda x: [float(val) for val in x.split()] if isinstance(x, str) else []
#     )
#     """
#     #partition["embeddings"] =  partition["embeddings"].apply(lambda x: [float(val) for _, val in enumerate(x.split())])
    
#     for col in partition.columns:
#         partition[col] = partition[col].apply(
#             lambda x: x.tolist() if isinstance(x, np.ndarray) else x
#     )

#     # Convert date fields
#     partition, cols = convert_datetime_to_strftime(partition)
#     partition[cols] = partition[cols].applymap(parseTimeINSTANT)

#     # Create SearcheableField
#     partition['SearcheableField'] = partition[self.SearcheableField].apply(
#         lambda x: ' '.join(x.astype(str)), axis=1
#     )
    
#     for record in partition.to_dict(orient="records"):
#         yield record

import ast
import configparser
from datetime import datetime
import json
from typing import List
from gensim.corpora import Dictionary
import pathlib

import pandas as pd
import numpy as np
from src.core.entities.utils import parseTimeINSTANT


# def is_valid_xml_char_ordinal(i):
#     """
#     Defines whether char is valid to use in xml document
#     XML standard defines a valid char as::
#     Char ::= #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
#     """
#     # conditions ordered by presumed frequency
#     return (
#         0x20 <= i <= 0xD7FF
#         or i in (0x9, 0xA, 0xD)
#         or 0xE000 <= i <= 0xFFFD
#         or 0x10000 <= i <= 0x10FFFF
#     )

# import pytz
# import math
# def clean_xml_string(s):
#     """
#     Cleans string from invalid xml chars
#     Solution was found there::
#     http://stackoverflow.com/questions/8733233/filtering-out-certain-bytes-in-python
#     """
#     return "".join(c for c in s if is_valid_xml_char_ordinal(ord(c)))


# def parseTimeINSTANT(time):
#     """
#     Parses a string representing an instant in time and returns it as an Instant object.
#     Supports ISO 8601 with timezone (e.g. '2024-12-30T13:52:11.444+01:00') and
#     '%Y-%m-%d %H:%M:%S' formats.
#     """
#     if isinstance(time, str) and time not in ("", "foo"):
#         try:
#             # ISO 8601 with optional timezone — works for both naive and aware
#             dt = datetime.fromisoformat(time)
#         except ValueError:
#             dt = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
#         if dt.tzinfo is None:
#             dt = dt.replace(tzinfo=pytz.UTC)
#         dt_utc = dt.astimezone(pytz.UTC)
#         return clean_xml_string(dt_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
#     else:
#         return clean_xml_string("")

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
                f"Corpus configuration {"place"} not found in config file.")

        self.id_field = cf.get(section, "id_field")
        self.title_field = cf.get(section, "title_field")
        self.date_field = cf.get(section, "date_field") #updated o plazo_presentacion
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
    def _parse_list_field(val, serialize_elements=False):
        """Parse a field that may come as a string repr of a list or already as a list."""
        if isinstance(val, (list, np.ndarray)):
            result = list(val)
        elif not isinstance(val, str) or val.strip() in ("", "[]"):
            return []
        else:
            try:
                result = ast.literal_eval(val)
                if not isinstance(result, list):
                    return []
            except (ValueError, SyntaxError):
                return []
        if serialize_elements:
            return [json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v) for v in result]
        return [str(v) for v in result]

    def get_docs_metadata(self):
        """
        Reads the raw corpus file and yields the metadata of each document as a dictionary.
        """
        
        if self.corpus_name == "place":
            self._logger.info("Indexing corpus: place")
            
            df_tenders = []
            for tender_type in ["minors", "insiders", "outsiders"]:
            
                # to index: minors / insiders / outsiders
                dir_meta = self.path_source / f"2025_26/{tender_type}_2526.parquet"
                
                df = pd.read_parquet(dir_meta)
                df["tender_type"] = tender_type

                valid_ids = get_cpv_filtered_ids(metadata_parquet=dir_meta, id_field="id")

                self._logger.info(f"Filtering corpus to {len(valid_ids)} valid IDs (out of {len(df)})")
                df = df[df[self.id_field].isin(valid_ids)].fillna("")

                df_tenders.append(df)
            
            df = pd.concat(df_tenders, ignore_index=True)

            # Extract id_tecnico from pliegos dict
            df["id_tecnico"] = df["pliegos"].apply(
                lambda x: x.get("id_tecnico") if isinstance(x, dict) else None
            )

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
                'id', 'tender_type', 'id_tecnico', 'title', 'date', 'summary', 'updated', 'link',
                'estado', 'expediente', 'objeto', 'valor_estimado', 'presupuesto_sin_iva',
                'presupuesto_con_iva', 'duracion_dias', 'cpv_list', 'ted_id',
                'subentidad_nacional', 'codigo_subentidad_territorial', 'lotes',
                'tipo_procedimiento', 'tramitacion', 'over_threshold', 'organo_nombre',
                'organo_id', 'plazo_presentacion', 'resultado', 'fecha_acuerdo',
                'ofertas_recibidas', 'ofertas_pymes', 'adjudicatario_nombre',
                'identificador',  # formato [["-1", "NIF", "B87222006"]]
                'adjudicatario_pyme', 'adjudicatario_ute',
                'importe_total_sin_iva', 'importe_total_con_iva',
            ]
            cols_keep = [col for col in cols_keep if col in df.columns]
            df = df[cols_keep]
            self._logger.info(f"Columns: {list(df.columns)}")

            # Parse nested [["-1", value]] fields
            nested_cols = [
                'resultado', 'fecha_acuerdo', 'ofertas_recibidas', 'ofertas_pymes',
                'adjudicatario_nombre', 'adjudicatario_pyme', 'adjudicatario_ute',
                'importe_total_sin_iva', 'importe_total_con_iva',
            ]
            for col in nested_cols:
                if col in df.columns:
                    df[col] = df[col].apply(self._extract_nested)

            # identificador tiene formato [["-1", "NIF", "valor"]] → extraer índice 2
            if 'identificador' in df.columns:
                df['identificador'] = df['identificador'].apply(lambda x: self._extract_nested(x, idx=2))

            if 'cpv_list' in df.columns:
                df['cpv_list'] = df['cpv_list'].apply(self._parse_list_field)

            # lotes: lista de [id_lote, descripcion] → lista de JSON strings
            if 'lotes' in df.columns:
                df['lotes'] = df['lotes'].apply(lambda v: self._parse_list_field(v, serialize_elements=True))

            # Parse date columns
            date_cols = [col for col in ["updated", "plazo_presentacion", "date"] if col in df.columns]
            for col in date_cols:
                df[col] = df[col].map(parseTimeINSTANT)

            # Create SearcheableField
            df["SearcheableField"] = df[searcheable].apply(
                lambda x: " ".join(x.astype(str)), axis=1
            )

            # Replace inf/-inf with NaN, then NaN with None in float columns (vectorised)
            float_cols = df.select_dtypes(include="float").columns
            df[float_cols] = df[float_cols].replace([np.inf, -np.inf], np.nan)
            df[float_cols] = df[float_cols].where(df[float_cols].notna(), other=None)

            yield from df.to_dict(orient="records")
        
        elif self.corpus_name == "ted":
            raise NotImplementedError("TED corpus processing not implemented yet.")
        
        elif self.corpus_name == "bdns":
            raise NotImplementedError("BDNS corpus processing not implemented yet.")

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
    
    def get_enrich_update(self):
        
        # read DATA_DIR="${BASE_DIR}/metadata/${TIPO}translate"
        pass
    
# if __name__ == "__main__":
#     # Example usage
#     corpus = Corpus(path_to_raw=pathlib.Path("/export/data_ml4ds/alia/place/2025_26/minors_2526.parquet"), config_file="/export/usuarios_ml4ds/lbartolome/Repos/alia/alia-sia/sia-config/config.cf")
#     # run for all documents
#     for doc in corpus.get_docs_metadata():
#         print(doc)
    