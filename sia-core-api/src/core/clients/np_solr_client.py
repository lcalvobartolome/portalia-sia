"""
This module provides a specific class for handeling the Solr API responses and requests of the NP-Solr-Service.

Author: Lorena Calvo-Bartolomé
Date: 17/04/2023
Modifed: 24/01/2024 (Updated for NP-Solr-Service (NextProcurement Proyect))
"""

from __future__ import annotations
import configparser
import logging
import math
import pathlib
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Union
from src.core.clients.external.np_tools_client import NPToolsClient
from src.core.clients.base.solr_client import SolrClient
from src.core.entities.corpus import Corpus
from src.core.entities.model import Model
from src.core.entities.queries import Queries

_PLACE_COL = "place"
_DATE_FMT  = "%Y-%m-%dT%H:%M:%SZ"

def _safe(val):
    """Return None instead of NaN / Inf, and round to 4 decimal places."""
    if val is None:
        return None
    try:
        if math.isnan(val) or math.isinf(val):
            return None
    except TypeError:
        pass
    return round(val, 4)
 
 
def _parse_date_flexible(raw: str) -> Optional[datetime]:
    """
    Parse a date string in ISO-8601 format (with or without time/timezone).
    Returns a timezone-aware datetime or None on failure.
    """
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw).astimezone(timezone.utc)
    except ValueError:
        return None
 
 
def _date_diff_days(d1: str, d2: str) -> Optional[float]:
    """
    Return max(d2 - d1, 0) in days, or None if either value is missing
    or cannot be parsed.  Accepts both full ISO-8601 and date-only strings.
    """
    if not d1 or not d2:
        return None
    t1 = _parse_date_flexible(d1)
    t2 = _parse_date_flexible(d2)
    if t1 is None or t2 is None:
        return None
    return max((t2 - t1).total_seconds() / 86_400.0, 0.0)
 
def _parse_date_field(raw) -> List[Optional[str]]:
    """
    Parse a date field stored as a list of "lot_id|date_value" strings,
    e.g. ["-1|2024-09-14"]. Returns a flat list of date strings.
    A plain ISO string (not the lot|date format) is returned as-is.
    """
    if not raw:
        return []
    if isinstance(raw, str):
        return [raw]
    return [item.split("|", 1)[1] if "|" in item else item for item in raw if item]
 
def _parse_lot_offers(raw) -> List[Optional[int]]:
    """
    Parse the ofertas_recibida  field.
 
    Solr stores this field as a list of strings with format "lot_id|n_offers",
    e.g. ["-1|1", "2|3"].  Returns a flat list of int-or-None values,
    one per lot.
    """
    if not isinstance(raw, (list, tuple)) or not raw:
        return []
    result: List[Optional[int]] = []
    for item in raw:
        try:
            # Expected format: "lot_id|n_offers"
            n_offers = item.split("|")[1]
            result.append(int(n_offers))
        except (AttributeError, IndexError, ValueError):
            result.append(None)
    return result
 
 
def _parse_lot_int_values(raw) -> List[Optional[int]]:
    """
    Generic parser for lot-level integer fields stored as "lot_id|value" strings.
    Returns a flat list of int-or-None values, one per lot entry.
    """
    if not isinstance(raw, (list, tuple)) or not raw:
        return []
    result: List[Optional[int]] = []
    for item in raw:
        try:
            val = item.split("|")[1]
            result.append(int(val))
        except (AttributeError, IndexError, ValueError):
            result.append(None)
    return result

class SIASolrClient(SolrClient):

    def __init__(
        self,
        logger: logging.Logger,
        config_file: str = "/config/config.cf"
    ) -> None:
        super().__init__(logger)

        # Read configuration from config file
        cf = configparser.ConfigParser()
        cf.read(config_file)
        self.solr_config = "sia_config"
        self.batch_size = int(cf.get('restapi', 'batch_size'))
        self.corpus_col = cf.get('restapi', 'corpus_col')
        self.no_meta_fields = cf.get('restapi', 'no_meta_fields').split(",")
        self.thetas_max_sum = int(cf.get('restapi', 'thetas_max_sum'))
        self.betas_max_sum = int(cf.get('restapi', 'betas_max_sum'))
        self.searchable_fields = cf.get('restapi', 'searchable_fields')
        self.date_field = cf.get('restapi', 'date_field')

        # Create Queries object for managing queries
        self.querier = Queries()

        # Create NPToolsClient to send requests to the NPTools API
        self.nptooler = NPToolsClient(logger)

        return

    # ======================================================
    # CORPUS-RELATED OPERATIONS
    # ======================================================

    def index_corpus(
        self,
        corpus_name: str
    ) -> None:
        """
        This method takes the name of a corpus raw file as input. It creates a Solr collection with the stem name of the file, which is obtained by converting the file name to lowercase (for example, if the input is 'Cordis', the stem would be 'cordis'). However, this process occurs only if the directory structure (self.path_source / corpus_raw / parquet) exists.

        After creating the Solr collection, the method reads the corpus file, extracting the raw information of each document. Subsequently, it sends a POST request to the Solr server to index the documents in batches.

        Parameters
        ----------
        corpus_raw : str
            The string name of the corpus raw file to be indexed.

        """

        # 1. Get full path and stem of the logical corpus
        #/mnt/data/2025_26
        #DATA_DIR="${BASE_DIR}/metadata/${TIPO}translate"
        #MODEL_DIR="${BASE_DIR}/metadata/${TIPO}/model"
        #METADATA_PARQUET="${BASE_DIR}/${TIPO}_2526.parquet"
        #corpus_to_index = self.path_source / (corpus_raw + ".parquet")
        
            
        self.logger.info(f"Corpus to index: {corpus_name}")

        # 2. Create collection
        corpus, err = self.create_collection(
            col_name=corpus_name, config=self.solr_config)
        if err == 409:
            self.logger.info(
                f"-- -- Collection {corpus_name} already exists.")
            return
        else:
            self.logger.info(
                f"-- -- Collection {corpus_name} successfully created.")

        # 3. Add corpus collection to self.corpus_col. If Corpora has not been created already, create it
        corpus, err = self.create_collection(
            col_name=self.corpus_col, config=self.solr_config)
        self.logger.info(f"-- -- Collection {self.corpus_col} successfully created.")
        if err == 409:
            self.logger.info(
                f"-- -- Collection {self.corpus_col} already exists.")

            # 3.1. Do query to retrieve last id in self.corpus_col
            # http://localhost:8983/solr/#/{self.corpus_col}/query?q=*:*&q.op=OR&indent=true&sort=id desc&fl=id&rows=1&useParams=
            sc, results = self.execute_query(q='*:*',
                                             col_name=self.corpus_col,
                                             sort="id desc",
                                             rows="1",
                                             fl="id")
            if sc != 200:
                self.logger.error(
                    f"-- -- Error getting latest used ID. Aborting operation...")
                return
            # Increment corpus_id for next corpus to be indexed
            corpus_id = int(results.docs[0]["id"]) + 1
        else:
            self.logger.info(
                f"Collection {self.corpus_col} successfully created.")
            corpus_id = 1

        # 4. Create Corpus object and extract info from the corpus to index
        corpus = Corpus(corpus_name)
        corpus_col_upt = corpus.get_corpora_update(id=corpus_id)
        self.logger.info(f"-- -- corpus_col_upt extracted")
        self.logger.info(f"{corpus_col_upt}")

        # 5. Index corpus and its fields in CORPUS_COL
        self.logger.info(
            f"-- -- Indexing of {corpus_name} info in {self.corpus_col} starts.")
        self.index_documents(corpus_col_upt, self.corpus_col, self.batch_size)
        self.logger.info(
            f"-- -- Indexing of {corpus_name} info in {self.corpus_col} completed.")
        
        self.logger.info(f"this is the corpus_col_upt: {corpus_col_upt}")

        # 6. Index documents in corpus collection
        self.logger.info(
            f"-- -- Indexing of {corpus_name} in {corpus_name} starts.")
        batch = []
        for doc in corpus.get_docs_metadata():
            batch.append(doc)
            
            if len(batch) >= self.batch_size:
                
                self.index_documents(batch, corpus_name, self.batch_size)
                batch = []  # Clear batch to free memory

        # Index remaining documents
        if batch:
            self.index_documents(batch, corpus_name, self.batch_size)
        self.logger.info(f"-- -- Indexing of {corpus_name} info in {corpus_name} completed.")


        return

    def list_corpus_collections(self) -> Union[List, int]:
        """Returns a list of the names of the corpus collections that have been created in the Solr server.

        Returns
        -------
        corpus_lst: List
            List of the names of the corpus collections that have been created in the Solr server.
        """

        sc, results = self.execute_query(q='*:*',
                                         col_name=self.corpus_col,
                                         fl="corpus_name")
        if sc != 200:
            self.logger.error(
                f"-- -- Error getting corpus collections in {self.corpus_col}. Aborting operation...")
            return [], sc

        corpus_lst = [doc["corpus_name"] for doc in results.docs]

        return corpus_lst, sc

    def get_corpus_coll_fields(self, corpus_col: str) -> Union[List, int]:
        """Returns a list of the fields of the corpus collection given by 'corpus_col' that have been defined in the Solr server.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection whose fields are to be retrieved.

        Returns
        -------
        models: list
            List of fields of the corpus collection
        sc: int
            Status code of the request
        """
        sc, results = self.execute_query(q='corpus_name:"'+corpus_col+'"',
                                         col_name=self.corpus_col,
                                         fl="fields")

        if sc != 200:
            self.logger.error(
                f"-- -- Error getting fields of {corpus_col}. Aborting operation...")
            return

        return results.docs[0]["fields"], sc

    def get_corpus_raw_path(self, corpus_col: str) -> Union[pathlib.Path, int]:
        """Returns the path of the logical corpus file associated with the corpus collection given by 'corpus_col'.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection whose path is to be retrieved.

        Returns
        -------
        path: pathlib.Path
            Path of the logical corpus file associated with the corpus collection given by 'corpus_col'.
        sc: int
            Status code of the request
        """

        sc, results = self.execute_query(q='corpus_name:"'+corpus_col+'"',
                                         col_name=self.corpus_col,
                                         fl="corpus_path")
        if sc != 200:
            self.logger.error(
                f"-- -- Error getting corpus path of {corpus_col}. Aborting operation...")
            return

        self.logger.info(results.docs[0]["corpus_path"])
        return pathlib.Path(results.docs[0]["corpus_path"]), sc

    def get_id_corpus_in_corpora(self, corpus_col: str) -> Union[int, int]:
        """Returns the ID of the corpus collection given by 'corpus_col' in the self.corpus_col collection.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection whose ID is to be retrieved.

        Returns
        -------
        id: int
            ID of the corpus collection given by 'corpus_col' in the self.corpus_col collection.
        """

        sc, results = self.execute_query(q='corpus_name:"'+corpus_col+'"',
                                         col_name=self.corpus_col,
                                         fl="id")
        if sc != 200:
            self.logger.error(
                f"-- -- Error getting corpus ID. Aborting operation...")
            return

        return results.docs[0]["id"], sc

    def get_corpus_MetadataDisplayed(self, corpus_col: str) -> Union[List, int]:
        """Returns a list of the fileds of the corpus collection indicating what metadata will be displayed in the NP front upon user request.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection whose MetadataDisplayed are to be retrieved.
        sc: int
            Status code of the request
        """

        sc, results = self.execute_query(q='corpus_name:"'+corpus_col+'"',
                                         col_name=self.corpus_col,
                                         fl="MetadataDisplayed")

        if sc != 200:
            self.logger.error(
                f"-- -- Error getting MetadataDisplayed of {corpus_col}. Aborting operation...")
            return

        return results.docs[0]["MetadataDisplayed"], sc

    def get_corpus_SearcheableField(self, corpus_col: str) -> Union[List, int]:
        """Returns a list of the fields used for autocompletion in the document search in the similarities function and in the document search function.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection whose SearcheableField are to be retrieved.
        sc: int
            Status code of the request
        """

        sc, results = self.execute_query(q='corpus_name:"'+corpus_col+'"',
                                         col_name=self.corpus_col,
                                         fl="SearcheableFields")

        if sc != 200:
            self.logger.error(
                f"-- -- Error getting SearcheableField of {corpus_col}. Aborting operation...")
            return

        return results.docs[0]["SearcheableFields"], sc

    def get_corpus_models(self, corpus_col: str) -> Union[List, int]:
        """Returns a list with the models associated with the corpus given by 'corpus_col'

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection whose models are to be retrieved.

        Returns
        -------
        models: list
            List of models associated with the corpus
        sc: int
            Status code of the request
        """

        sc, results = self.execute_query(q='corpus_name:"'+corpus_col+'"',
                                         col_name=self.corpus_col,
                                         fl="models")

        if sc != 200:
            self.logger.error(
                f"-- -- Error getting models of {corpus_col}. Aborting operation...")
            return

        return results.docs[0]["models"], sc

    def delete_corpus(self, corpus_raw: str) -> None:
        """Given the name of a corpus raw file as input, it deletes the Solr collection associated with it. Additionally, it removes the document entry of the corpus in the self.corpus_col collection and all the models that have been trained with such a corpus.

        Parameters
        ----------
        corpus_raw : str
            The string name of the corpus raw file to be deleted.
        """

        # 1. Get stem of the logical corpus        
        corpus_to_delete = self.path_source / (corpus_raw + ".parquet")
        corpus_logical_name = corpus_to_delete.stem.lower()

        # 2. Delete corpus collection
        _, sc = self.delete_collection(col_name=corpus_logical_name)
        if sc != 200:
            self.logger.error(
                f"-- -- Error deleting corpus collection {corpus_logical_name}")
            return

        # 3. Get ID and associated models of corpus collection in self.corpus_col
        sc, results = self.execute_query(q='corpus_name:'+corpus_logical_name,
                                         col_name=self.corpus_col,
                                         fl="id,models")
        if sc != 200:
            self.logger.error(
                f"-- -- Error getting corpus ID. Aborting operation...")
            return

        # 4. Delete all models associated with the corpus if any
        if "models" in results.docs[0].keys():
            for model in results.docs[0]["models"]:
                _, sc = self.delete_collection(col_name=model)
                if sc != 200:
                    self.logger.error(
                        f"-- -- Error deleting model collection {model}")
                    return

        # 5. Remove corpus from self.corpus_col
        sc = self.delete_doc_by_id(
            col_name=self.corpus_col, id=results.docs[0]["id"])
        if sc != 200:
            self.logger.error(
                f"-- -- Error deleting corpus from {self.corpus_col}")
        return

    def check_is_corpus(self, corpus_col) -> bool:
        """Checks if the collection given by 'corpus_col' is a corpus collection.

        Parameters
        ----------
        corpus_col : str
            Name of the collection to be checked.

        Returns
        -------
        is_corpus: bool
            True if the collection is a corpus collection, False otherwise.
        """

        corpus_colls, sc = self.list_corpus_collections()
        if corpus_col not in corpus_colls:
            self.logger.error(
                f"-- -- {corpus_col} is not a corpus collection. Aborting operation...")
            return False

        return True

    def check_corpus_has_model(self, corpus_col, model_name) -> bool:
        """Checks if the collection given by 'corpus_col' has a model with name 'model_name'.

        Parameters
        ----------
        corpus_col : str
            Name of the collection to be checked.
        model_name : str
            Name of the model to be checked.

        Returns
        -------
        has_model: bool
            True if the collection has the model, False otherwise.
        """

        corpus_fields, sc = self.get_corpus_coll_fields(corpus_col)
        if 'doctpc_' + model_name not in corpus_fields:
            self.logger.error(
                f"-- -- {corpus_col} does not have the field doctpc_{model_name}. Aborting operation...")
            return False
        return True

    def modify_corpus_SearcheableFields(
        self,
        SearcheableFields: str,
        corpus_col: str,
        action: str
    ) -> None:
        """
        Given a list of fields, it adds them to the SearcheableFields field of the corpus collection given by 'corpus_col' if action is 'add', or it deletes them from the SearcheableFields field of the corpus collection given by 'corpus_col' if action is 'delete'.

        Parameters
        ----------
        SearcheableFields : str
            List of fields to be added to the SearcheableFields field of the corpus collection given by 'corpus_col'.
        corpus_col : str
            Name of the corpus collection whose SearcheableFields field is to be updated.
        action : str
            Action to be performed. It can be 'add' or 'delete'.
        """

        # 1. Get full path
        corpus_path, _ = self.get_corpus_raw_path(corpus_col)

        SearcheableFields = SearcheableFields.split(",")

        # 2. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 3. Create Corpus object, get SearcheableField and index information in corpus collection
        corpus = Corpus(corpus_path)
        corpus_update, new_SearcheableFields = corpus.get_corpus_SearcheableField_update(
            new_SearcheableFields=SearcheableFields,
            action=action)
        self.logger.info(
            f"-- -- Indexing new SearcheableField information in {corpus_col} collection")
        self.index_documents(corpus_update, corpus_col, self.batch_size)
        self.logger.info(
            f"-- -- Indexing new SearcheableField information in {self.corpus_col} completed.")

        # 4. Get self.corpus_col update
        corpora_id, _ = self.get_id_corpus_in_corpora(corpus_col)
        corpora_update = corpus.get_corpora_SearcheableField_update(
            id=corpora_id,
            field_update=new_SearcheableFields,
            action="set")
        self.logger.info(
            f"-- -- Indexing new SearcheableField information in {self.corpus_col} starts.")
        self.index_documents(corpora_update, self.corpus_col, self.batch_size)
        self.logger.info(
            f"-- -- Indexing new SearcheableField information in {self.corpus_col} completed.")

        return

    # ======================================================
    # MODEL-RELATED OPERATIONS
    # ======================================================

    def index_model(self, model_path: str) -> None:
        """
        Given the string path of a model created with the ITMT (i.e., the name of one of the folders representing a model within the TMmodels folder), it extracts the model information and that of the corpus used for its generation. It then adds a new field in the corpus collection of type 'VectorField' and name 'doctpc_{model_name}, and index the document-topic proportions in it. At last, it index the rest of the model information in the model collection.

        Parameters
        ----------
        model_path : str
            Path to the folder of the model to be indexed.
        """

        # 1. Get stem of the model folder
        model_to_index =  self.path_source / model_path
        model_name = model_to_index.stem.lower()

        # 2. Create collection
        _, err = self.create_collection(
            col_name=model_name, config=self.solr_config)
        if err == 409:
            self.logger.info(
                f"-- -- Collection {model_name} already exists.")
            return
        else:
            self.logger.info(
                f"-- -- Collection {model_name} successfully created.")

        # 3. Create Model object and extract info from the corpus to index
        model = Model(model_to_index)
        json_docs, corpus_name = model.get_model_info_update(action='set')
        if not self.check_is_corpus(corpus_name):
            return
        corpora_id, _ = self.get_id_corpus_in_corpora(corpus_name)
        field_update = model.get_corpora_model_update(
            id=corpora_id, action='add')

        # 4. Add field for the doc-tpc distribution associated with the model being indexed in the document associated with the corpus
        self.logger.info(
            f"-- -- Indexing model information of {model_name} in {self.corpus_col} starts.")

        self.index_documents(field_update, self.corpus_col, self.batch_size)
        self.logger.info(
            f"-- -- Indexing of model information of {model_name} info in {self.corpus_col} completed.")

        # 5. Modify schema in corpus collection to add field for the doc-tpc distribution and the similarities associated with the model being indexed
        model_key = 'doctpc_' + model_name
        sim_model_key = 'sim_' + model_name
        self.logger.info(
            f"-- -- Adding field {model_key} in {corpus_name} collection")
        _, err = self.add_field_to_schema(
            col_name=corpus_name, field_name=model_key, field_type='VectorField')
        self.logger.info(
            f"-- -- Adding field {sim_model_key} in {corpus_name} collection")
        _, err = self.add_field_to_schema(
            col_name=corpus_name, field_name=sim_model_key, field_type='VectorFloatField')

        # 6. Index doc-tpc information in corpus collection
        self.logger.info(
            f"-- -- Indexing model information in {corpus_name} collection")
        self.index_documents(json_docs, corpus_name, self.batch_size)

        self.logger.info(
            f"-- -- Indexing model information in {model_name} collection")
        json_tpcs = model.get_model_info()

        self.index_documents(json_tpcs, model_name, self.batch_size)

        return

    def list_model_collections(self) -> Union[List[str], int]:
        """Returns a list of the names of the model collections that have been created in the Solr server.

        Returns
        -------
        models_lst: List[str]
            List of the names of the model collections that have been created in the Solr server.
        sc: int
            Status code of the request.
        """
        sc, results = self.execute_query(q='*:*',
                                         col_name=self.corpus_col,
                                         fl="models")
        if sc != 200:
            self.logger.error(
                f"-- -- Error getting corpus collections in {self.corpus_col}. Aborting operation...")
            return

        models_lst = [model for doc in results.docs if bool(
            doc) for model in doc["models"]]
        self.logger.info(f"-- -- Models found: {models_lst}")

        return models_lst, sc

    def delete_model(self, model_path: str) -> None:
        """
        Given the string path of a model created with the ITMT (i.e., the name of one of the folders representing a model within the TMmodels folder), 
        it deletes the model collection associated with it. Additionally, it removes the document-topic proportions field in the corpus collection and removes the fields associated with the model and the model from the list of models in the corpus document from the self.corpus_col collection.

        Parameters
        ----------
        model_path : str
            Path to the folder of the model to be indexed.
        """

        # 1. Get stem of the model folder
        model_to_index =  self.path_source / model_path
        model_name = model_to_index.stem.lower()

        # 2. Delete model collection
        _, sc = self.delete_collection(col_name=model_name)
        if sc != 200:
            self.logger.error(
                f"-- -- Error occurred while deleting model collection {model_name}. Stopping...")
            return
        else:
            self.logger.info(
                f"-- -- Model collection {model_name} successfully deleted.")

        # 3. Create Model object and extract info from the corpus associated with the model
        model = Model(model_to_index)
        json_docs, corpus_name = model.get_model_info_update(action='remove')
        sc, results = self.execute_query(q='corpus_name:'+corpus_name,
                                         col_name=self.corpus_col,
                                         fl="id")
        if sc != 200:
            self.logger.error(
                f"-- -- Corpus collection not found in {self.corpus_col}")
            return
        field_update = model.get_corpora_model_update(
            id=results.docs[0]["id"], action='remove')

        # 4. Remove field for the doc-tpc distribution associated with the model being deleted in the document associated with the corpus
        self.logger.info(
            f"-- -- Deleting model information of {model_name} in {self.corpus_col} starts.")
        self.index_documents(field_update, self.corpus_col, self.batch_size)
        self.logger.info(
            f"-- -- Deleting model information of {model_name} info in {self.corpus_col} completed.")

        # 5. Delete doc-tpc information from corpus collection
        self.logger.info(
            f"-- -- Deleting model information from {corpus_name} collection")
        self.index_documents(json_docs, corpus_name, self.batch_size)

        # 6. Modify schema in corpus collection to delete field for the doc-tpc distribution and similarities associated with the model being indexed
        model_key = 'doctpc_' + model_name
        sim_model_key = 'sim_' + model_name
        self.logger.info(
            f"-- -- Deleting field {model_key} in {corpus_name} collection")
        _, err = self.delete_field_from_schema(
            col_name=corpus_name, field_name=model_key)
        self.logger.info(
            f"-- -- Deleting field {sim_model_key} in {corpus_name} collection")
        _, err = self.delete_field_from_schema(
            col_name=corpus_name, field_name=sim_model_key)

        return

    def check_is_model(self, model_col) -> bool:
        """Checks if the model_col is a model collection. If not, it aborts the operation.

        Parameters
        ----------
        model_col : str
            Name of the model collection.

        Returns
        -------
        is_model : bool
            True if the model_col is a model collection, False otherwise.
        """

        model_colls, sc = self.list_model_collections()
        if model_col not in model_colls:
            self.logger.error(
                f"-- -- {model_col} is not a model collection. Aborting operation...")
            return False
        return True
    
    def get_all_searchable_fields(self) -> List[str]:
        """Returns the list of fields used for searching documents in the similarities and document search functions.

        Returns
        -------
        searchable_fields : List[str]
            List of fields used for searching documents.
        """

        return self.searchable_fields.split(","), 200

    # ======================================================
    # AUXILIARY FUNCTIONS
    # ======================================================
    def custom_start_and_rows(self, start, rows, col) -> Union[str, str]:
        """Checks if start and rows are None. If so, it returns the number of documents in the collection as the value for rows and 0 as the value for start.

        Parameters
        ----------
        start : str
            Start parameter of the query.
        rows : str
            Rows parameter of the query.
        col : str
            Name of the collection.

        Returns
        -------
        start : str
            Final start parameter of the query.
        rows : str
            Final rows parameter of the query.
        """
        if start is None:
            start = str(0)
            
        if rows is None:
            numFound_dict, sc = self.do_Q3(col)
            rows = str(numFound_dict['ndocs'])

            if sc != 200:
                self.logger.error(
                    f"-- -- Error executing query Q3. Aborting operation...")
                return "0", "100"
        self.logger.info(f"-- -- Start: {start}, Rows: {rows} from custom_start_and_rows")
        return start, rows

    # ======================================================
    # QUERIES
    # ======================================================

    def do_Q1(
        self,
        corpus_col: str,
        doc_id: str,
        model_name: str
    ) -> Union[dict, int]:
        """Executes query Q1.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection.
        id : str
            ID of the document to be retrieved.
        model_name : str
            Name of the model to be used for the retrieval.

        Returns
        -------
        thetas: dict
            JSON object with the document-topic proportions (thetas)
        sc : int
            The status code of the response.  
        """

        # 0. Convert corpus and model names to lowercase
        corpus_col = corpus_col.lower()
        model_name = model_name.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 2. Check that corpus_col has the model_name field
        if not self.check_corpus_has_model(corpus_col, model_name):
            return

        # 3. Execute query
        q1 = self.querier.customize_Q1(id=doc_id, model_name=model_name)
        params = {k: v for k, v in q1.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q1['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q1. Aborting operation...")
            return

        # 4. Return -1 if thetas field is not found (it could happen that a document in a collection has not thetas representation since it was not keeped within the corpus used for training the model)
        if 'doctpc_' + model_name in results.docs[0].keys():
            resp = {'thetas': results.docs[0]['doctpc_' + model_name]}
        else:
            resp = {'thetas': -1}

        return resp, sc

    def do_Q2(
        self,
        corpus_col: str
    ) -> Union[dict, int]:
        """
        Executes query Q2.

        Parameters
        ----------
        corpus_col: str
            Name of the corpus collection

        Returns
        -------
        json_object: dict
            JSON object with the metadata fields of the corpus collection in the form: {'metadata_fields': [field1, field2, ...]}
        sc: int
            The status code of the response
        """

        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 2. Execute query (to self.corpus_col)
        q2 = self.querier.customize_Q2(corpus_name=corpus_col)
        params = {k: v for k, v in q2.items() if k != 'q'}
        sc, results = self.execute_query(
            q=q2['q'], col_name=self.corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q2. Aborting operation...")
            return

        # 3. Get Metadatadisplayed fields of corpus_col
        Metadatadisplayed, sc = self.get_corpus_MetadataDisplayed(corpus_col)
        if sc != 200:
            self.logger.error(
                f"-- -- Error getting Metadatadisplayed of {corpus_col}. Aborting operation...")
            return

        # 4. Filter metadata fields to be displayed in the NP
        # meta_fields = [field for field in results.docs[0]
        #               ['fields'] if field in Metadatadisplayed]

        return {'metadata_fields': Metadatadisplayed}, sc

    def do_Q3(
        self,
        col: str
    ) -> Union[dict, int]:
        """Executes query Q3.

        Parameters
        ----------
        col : str
            Name of the collection

        Returns
        -------
        json_object : dict
            JSON object with the number of documents in the corpus collection
        sc : int
            The status code of the response
        """

        # 0. Convert collection name to lowercase
        col = col.lower()

        # 1. Check that col is either a corpus or a model collection
        if not self.check_is_corpus(col) and not self.check_is_model(col):
            return

        # 2. Execute query
        q3 = self.querier.customize_Q3()
        params = {k: v for k, v in q3.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q3['q'], col_name=col, **params)

        # 3. Filter results
        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q3. Aborting operation...")
            return

        return {'ndocs': int(results.hits)}, sc

    def do_Q5(
        self,
        corpus_col: str,
        model_name: str,
        doc_id: str,
        start: str,
        rows: str
    ) -> Union[dict, int]:
        """Executes query Q5.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection
        model_name: str
            Name of the model to be used for the retrieval
        doc_id: str
            ID of the document whose similarity is going to be checked against all other documents in 'corpus_col'
         start: str
            Offset into the responses at which Solr should begin displaying content
        rows: str
            How many rows of responses are displayed at a time

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert corpus and model names to lowercase
        corpus_col = corpus_col.lower()
        model_col = model_name.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 2. Check that corpus_col has the model_col field
        if not self.check_corpus_has_model(corpus_col, model_col):
            return

        # 3. Execute Q1 to get thetas of document given by doc_id
        thetas_dict, sc = self.do_Q1(
            corpus_col=corpus_col, model_name=model_col, doc_id=doc_id)
        thetas = thetas_dict['thetas']

        # 4. Check that thetas are available on the document given by doc_id. If not, infer them
        if thetas == -1:
            # Get text (lemmas) of the document so its thetas can be inferred
            lemmas_dict, sc = self.do_Q15(
                corpus_col=corpus_col, doc_id=doc_id)
            lemmas = lemmas_dict['lemmas']
            
            inf_resp = self.nptooler.get_thetas(text_to_infer=lemmas,
                                                model_for_infer=model_name)
        
            if inf_resp.status_code != 200:
                self.logger.error(
                    f"-- -- Error attaining thetas from {lemmas} while executing query Q5. Aborting operation...")
                return

            thetas = inf_resp.results[0]['thetas']

            self.logger.info(
                f"-- -- Thetas attained in {inf_resp.time} seconds: {thetas}")

        # 4. Customize start and rows
        start, rows = self.custom_start_and_rows(start, rows, corpus_col)
        
        # 5. Execute query
        distance = "bhattacharyya"
        q5 = self.querier.customize_Q5(
            model_name=model_col, thetas=thetas, distance=distance,
            start=start, rows=rows)
        params = {k: v for k, v in q5.items() if k != 'q'}
        
        sc, results = self.execute_query(
            q=q5['q'], col_name=corpus_col, **params)
        
        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q5. Aborting operation...")
            return
        
        # 6. Normalize scores
        for el in results.docs:
            el['score'] *= (100/(self.thetas_max_sum ^ 2))

        return results.docs, sc

    def do_Q6(
        self,
        corpus_col: str,
        doc_id: str
    ) -> Union[dict, int]:
        """Executes query Q6.

        Parameters
        ----------
        corpus_col: str
            Name of the corpus collection
        doc_id: str
            ID of the document whose metadata is going to be retrieved

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 2. Get meta fields
        #meta_fields_dict, sc = self.do_Q2(corpus_col)
        #meta_fields = ','.join(meta_fields_dict['metadata_fields'])

        #self.logger.info("-- -- These are the meta fields: " + meta_fields)

        # 3. Execute query
        q6 = self.querier.customize_Q6(id=doc_id, meta_fields=self.searchable_fields)
        params = {k: v for k, v in q6.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q6['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q6. Aborting operation...")
            return

        return results.docs, sc

    def do_Q7(
        self,
        corpus_col: str,
        string: str,
        start: str,
        rows: str
    ) -> Union[dict, int]:
        """Executes query Q7.

        Parameters
        ----------
        corpus_col: str
            Name of the corpus collection
        string: str
            String to be searched in the title of the documents

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 2. Get number of docs in the collection (it will be the maximum number of docs to be retireved) if rows is not specified
        if rows is None:
            q3 = self.querier.customize_Q3()
            params = {k: v for k, v in q3.items() if k != 'q'}

            sc, results = self.execute_query(
                q=q3['q'], col_name=corpus_col, **params)

            if sc != 200:
                self.logger.error(
                    f"-- -- Error executing query Q3. Aborting operation...")
                return
            rows = results.hits
        if start is None:
            start = str(0)

        # 2. Execute query
        q7 = self.querier.customize_Q7(
            title_field='SearcheableField',
            string=string,
            start=start,
            rows=rows)
        params = {k: v for k, v in q7.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q7['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q7. Aborting operation...")
            return

        return results.docs, sc

    def do_Q8(self,
        model_col: str,
        start: str,
        rows: str
    ) -> Union[dict, int]:
        """Executes query Q8.

        Parameters
        ----------
        model_col: str
            Name of the model collection
        start: str
            Index of the first document to be retrieved
        rows: str
            Number of documents to be retrieved

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert model name to lowercase
        model_col = model_col.lower()

        # 1. Check that model_col is indeed a model collection
        if not self.check_is_model(model_col):
            return

        # 3. Customize start and rows
        start, rows = self.custom_start_and_rows(start, rows, model_col)

        # 4. Execute query
        q8 = self.querier.customize_Q8(start=start, rows=rows)
        params = {k: v for k, v in q8.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q8['q'], col_name=model_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q8. Aborting operation...")
            return

        return results.docs, sc

    def do_Q9(self,
        corpus_col: str,
        model_name: str,
        topic_id: str,
        start: str,
        rows: str
    ) -> Union[dict, int]:
        """Executes query Q9.

        Parameters
        ----------
        corpus_col: str
            Name of the corpus collection on which the query will be carried out
        model_name: str
            Name of the model collection on which the search will be based
        topic_id: str
            ID of the topic whose top-documents will be retrieved
        start: str
            Index of the first document to be retrieved
        rows: str
            Number of documents to be retrieved

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert corpus and model names to lowercase
        corpus_col = corpus_col.lower()
        model_name = model_name.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 2. Check that corpus_col has the model_name field
        if not self.check_corpus_has_model(corpus_col, model_name):
            return

        # 3. Customize start and rows
        start, rows = self.custom_start_and_rows(start, rows, corpus_col)
        # We limit the maximum number of results since they are top-documnts
        # If more results are needed pagination should be used

        if int(rows) > 100:
            rows = "100"

        # 5. Execute query
        q9 = self.querier.customize_Q9(
            model_name=model_name,
            topic_id=topic_id,
            start=start,
            rows=rows)
        params = {k: v for k, v in q9.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q9['q'], col_name=corpus_col, **params)
        
        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q9. Aborting operation...")
            return

        # 6. Return a dictionary with names more understandable to the end user
        proportion_key = "payload(doctpc_{},t{})".format(model_name, topic_id)
        for dict in results.docs:
            if proportion_key in dict.keys():
                dict["topic_relevance"] = dict.pop(proportion_key)*0.1
            dict["num_words_per_doc"] = dict.pop("nwords_per_doc")

        return results.docs, sc

    def do_Q10(self,
        model_col: str,
        start: str,
        rows: str,
        only_id: bool
    ) -> Union[dict, int]:
        """Executes query Q10.

        Parameters
        ----------
        model_col: str
            Name of the model collection whose information is being retrieved
        start: str
            Index of the first document to be retrieved
        rows: str
            Number of documents to be retrieved

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert model name to lowercase
        model_col = model_col.lower()

        # 1. Check that model_col is indeed a model collection
        if not self.check_is_model(model_col):
            return

        # 3. Customize start and rows
        start, rows = self.custom_start_and_rows(start, rows, model_col)

        # 4. Execute query
        q10 = self.querier.customize_Q10(
            start=start, rows=rows, only_id=only_id)
        params = {k: v for k, v in q10.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q10['q'], col_name=model_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q10. Aborting operation...")
            return

        return results.docs, sc

    def do_Q14(self,
        corpus_col: str,
        model_name: str,
        text_to_infer: str,
        start: str,
        rows: str
    ) -> Union[dict, int]:
        """Executes query Q14.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection
        model_name: str
            Name of the topic model to be used for the retrieval
        text_to_infer: str
            Text to be inferred
         start: str
            Offset into the responses at which Solr should begin displaying content
        rows: str
            How many rows of responses are displayed at a time

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert corpus and model names to lowercase
        corpus_col = corpus_col.lower()
        model_col = model_name.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 2. Check that corpus_col has the model_col field
        if not self.check_corpus_has_model(corpus_col, model_col):
            return

        # 3. Make request to NPTools API to get thetas of text_to_infer
        # Get text (lemmas) of the document so its thetas can be inferred
        lemmas_resp = self.nptooler.get_lemmas(text_to_lemmatize=text_to_infer, lang="es")
        lemmas = lemmas_resp.results[0]['lemmas']
        
        self.logger.info(
            f"-- -- Lemas attained in {lemmas_resp.time} seconds: {lemmas}")
        
        inf_resp = self.nptooler.get_thetas(text_to_infer=lemmas,
                                    model_for_infer=model_name)

        if inf_resp.status_code != 200:
            self.logger.error(
                f"-- -- Error attaining thetas from {lemmas} while executing query Q5. Aborting operation...")
            return

        thetas = inf_resp.results[0]['thetas']
        
        self.logger.info(
            f"-- -- Thetas attained in {inf_resp.time} seconds: {thetas}")

        # 4. Customize start and rows
        start, rows = self.custom_start_and_rows(start, rows, corpus_col)
        
        # 5. Execute query
        distance = "bhattacharyya"
        q14 = self.querier.customize_Q14(
            model_name=model_col, thetas=thetas, distance=distance,
            start=start, rows=rows)
        params = {k: v for k, v in q14.items() if k != 'q'}
        
        sc, results = self.execute_query(
            q=q14['q'], col_name=corpus_col, **params)
        
        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q14. Aborting operation...")
            return

        # 6. Normalize scores
        for el in results.docs:
            el['score'] *= (100/(self.thetas_max_sum ^ 2))
            
        # return dictionary with keys topics and mostSimilar
        return {'topics': thetas, 'mostSimilar': results.docs}, sc

    def do_Q15(self,
        corpus_col: str,
        doc_id: str
    ) -> Union[dict, int]:
        """Executes query Q15.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection.
        id : str
            ID of the document to be retrieved.

        Returns
        -------
        lemmas: dict
            JSON object with the document's lemmas.
        sc : int
            The status code of the response.  
        """

        # 0. Convert corpus and model names to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 2. Execute query
        q15 = self.querier.customize_Q15(id=doc_id)
        params = {k: v for k, v in q15.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q15['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q15. Aborting operation...")
            return

        return {'lemmas': results.docs[0]['lemmas']}, sc
    
    def do_Q20(
        self,
        corpus_col:str,
        model_name:str,
        search_word:str,
        start:int,
        rows:int,
        embedding_model:str = "word2vec",
        lang:str = "es",
    ) -> Union[dict,int]:
        """Executes query Q20. 
        
        Parameters
        ----------
        corpus_col: str
            Name of the corpus collection
        model_name: str
            Name of the topic model to be used for the retrieval
        search_word: str
            Word to look documents similar to
        start: int
            Index of the first document to be retrieved
        rows: int
            Number of documents to be retrieved
        
        Returns
        -------
        response: dict
            JSON object with the results of the query.
        """
        
        # 0. Convert corpus and model names to lowercase
        corpus_col = corpus_col.lower()
        model_col = model_name.lower()
        
        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return
        
        # 2. Check that corpus_col has the model_name field
        if not self.check_corpus_has_model(corpus_col, model_col):
            default_model = f"default_{model_col.split('_')[-1]}"
            if not self.check_corpus_has_model(corpus_col, default_model):
                return
            model_col = default_model
            model_name = model_col
            self.logger.info(
                f"-- -- Model {model_col} not found in {corpus_col}. Using {default_model} instead.")

        # 3. Lemmatize and get embedding from search_word
        resp = self.nptooler.get_embedding(
            text_to_embed=search_word,
            embedding_model=embedding_model,
            model_for_embedding=model_name,
            lang=lang
        )
        
        if resp.status_code != 200:
            self.logger.error(
                f"-- -- Error attaining embeddings from {search_word} while executing query Q20. Aborting operation...")
            return

        embs = resp.results
        self.logger.info(
            f"-- -- Embbedings for word {search_word} attained in {resp.time} seconds: {embs}")
         
        # 4. Customize start and rows
        start, rows = self.custom_start_and_rows(start, rows, model_col)
        self.logger.info(f"-- -- Start: {start}, Rows: {rows}")
        
        # 5. Execute query
        q20 = self.querier.customize_Q20(
            wd_embeddings=embs,
            start=start,
            rows=rows
        )
        params = {k: v for k, v in q20.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q20['q'], col_name=model_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q20. Aborting operation...")
            return
        
        # 5. Find the topic that is most similar to the search_word
        #self.logger.info(f"-- -- Results: {results.docs}")
        #self.logger.info(f"-- -- Results: {results.docs[0]}")
        closest_tpc = results.docs[0]["id"].split("t")[1]
        sim_score = results.docs[0]["score"]
        self.logger.info(f"-- -- Closest topic: {closest_tpc}")
        self.logger.info(f"-- -- Similarity score: {sim_score}")
        
        # 6. Get top documents for that topic
        start, rows = self.custom_start_and_rows(start, rows, corpus_col)
        docs, sc = self.do_Q9(
            corpus_col=corpus_col,
            model_name=model_col,
            topic_id=closest_tpc,
            start=start,
            rows=rows
        )
        
        self.logger.info(f"-- -- Docs: {docs}")
        
        
        # 7. Return the id of the topic, the similarity score, and the top documents for that topic
        response = {
            "topic_id": closest_tpc,
            "topic_str": "t" + closest_tpc,
            "similarity_score": sim_score,
            "docs": docs
        }
        
        return response, sc
    
    def do_Q21(
        self,
        corpus_col:str,
        search_doc:str,
        start:int,
        rows:int,
        embedding_model:str = "bert",
        keyword:str = None,
        query_fields:str="raw_text", #"tile objective"
        lang:str = "es",
    ) -> Union[dict,int]:
        """
        Executes query Q21.
        
        Parameters
        ----------
        corpus_col: str
            Name of the corpus collection
        search_doc: str
            Document to look documents similar to
        start: int
            Index of the first document to be retrieved
        rows: int
            Number of documents to be retrieved
        embedding_model: str
            Name of the embedding model to be used
        lang: str
            Language of the text to be embedded
        
        Returns
        -------
        response: dict
            JSON object with the results of the query.
        """
        
        # 0. Convert corpus to lowercase
        corpus_col = corpus_col.lower()
        
        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return
        
        # 3. Get embedding from search_doc
        resp = self.nptooler.get_embedding(
            text_to_embed=search_doc,
            embedding_model=embedding_model,
            lang=lang
        )
        
        if resp.status_code != 200:
            self.logger.error(
                f"-- -- Error attaining embeddings from {search_doc} while executing query Q21. Aborting operation...")
            return

        embs = resp.results
        self.logger.info(
            f"-- -- Embbedings for doc {search_doc} attained in {resp.time} seconds.")
         
        # 4. Customize start and rows
        start, rows = self.custom_start_and_rows(start, rows, corpus_col)
        
        # 5. Calculate cosine similarity between the embedding of search_doc and the embeddings of the documents in the corpus
        if keyword is None:
            q21 = self.querier.customize_Q21(
                doc_embeddings=embs,
                start=start,
                rows=rows
            )
        else:
            q21 = self.querier.customize_Q21_e(
            doc_embeddings=embs,
            keyword=keyword,
            query_fields=query_fields,
            start=start,
            rows=rows
        )
        params = {k: v for k, v in q21.items() if k != 'q'}

        sc, results = self.execute_query(
            q=q21['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q21. Aborting operation...")
            return

        return results.docs, sc
    
    def do_Q22( # this is not a predefined query, but a wrapper over the inferencer that gets the information for the predicted topic
        self,
        model_name: str,
        text_to_infer: str
    ) -> Union[dict, int]:
        """Executes query Q22.

        Parameters
        ----------
        corpus_col : str
            Name of the corpus collection
        model_name: str
            Name of the topic model to be used for the retrieval
        text_to_infer: str
            Text to be inferred
        start: str
            Offset into the responses at which Solr should begin displaying content
        rows: str
            How many rows of responses are displayed at a time

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert model names to lowercase
        model_col = model_name.lower()

        if not self.check_is_model(model_col):
            return

        # 1. Make request to NPTools API to get thetas of text_to_infer
        # Get text (lemmas) of the document so its thetas can be inferred
        lemmas_resp = self.nptooler.get_lemmas(text_to_lemmatize=text_to_infer, lang="es")
        lemmas = lemmas_resp.results[0]['lemmas']
        
        self.logger.info(
            f"-- -- Lemmas attained in {lemmas_resp.time} seconds: {lemmas}")
        
        inf_resp = self.nptooler.get_thetas(
            text_to_infer=lemmas,
            model_for_infer=model_name)

        if inf_resp.status_code != 200:
            self.logger.error(
                f"-- -- Error attaining thetas from {lemmas} while executing query Q5. Aborting operation...")
            return

        thetas = inf_resp.results[0]['thetas']
        
        self.logger.info(
            f"-- -- Thetas attained in {inf_resp.time} seconds: {thetas}")

        # thetas is something like "t0|26 t1|21 t2|61 t3|77 t4|55 t5|34 t6|127 t7|97 t8|46 t9|154 t10|179 t11|123"
        # get topics and their weights as a dictionary
        topics = {tpc.split("|")[0]: int(tpc.split("|")[1]) for tpc in thetas.split(" ")}
        
        # 3. Get model info for the topics in the text
        # execute Q10 to get model info
        start, rows = self.custom_start_and_rows(0, None, model_col)
        model_info, sc = self.do_Q10(model_col, start=start, rows=rows, only_id=False)
        # model info is a list of dictionaries, each dictionary has the id of the topic in the form "t0", "t1", etc.
        # keep only info from the topics that are in topics, the keys "id", "tpc_descriptions" and "tpc_labels", and add "weight" to each dictionary
        # the result is a list of dictionaries with the keys "id", "tpc_descriptions", "tpc_labels", and "weight"
        upd_model_info = [{"id": tpc["id"], "tpc_descriptions": tpc["tpc_descriptions"], "tpc_labels": tpc["tpc_labels"], "weight": topics[tpc["id"]]} for tpc in model_info if tpc["id"] in topics]
        
        self.logger.info(f"-- -- Model info: {upd_model_info}")
        
        return upd_model_info, sc
    
    def do_Q30(
        self,
        corpus_col: str,
        year: int,
        start: str,
        rows: str
    ) -> Union[dict, int]:
        """Executes query Q30.

        Parameters
        ----------
        corpus_col: str
            Name of the corpus collection
        year: int
            Year for which the documents are going to be retrieved

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 2. Get number of docs in the collection (it will be the maximum number of docs to be retireved) if rows is not specified
        if rows is None:
            q3 = self.querier.customize_Q3()
            params = {k: v for k, v in q3.items() if k != 'q'}

            sc, results = self.execute_query(
                q=q3['q'], col_name=corpus_col, **params)

            if sc != 200:
                self.logger.error(
                    f"-- -- Error executing query Q3. Aborting operation...")
                return
            rows = results.hits
        if start is None:
            start = str(0)

        # 2. Execute query
        q30 = self.querier.customize_Q30(
            year=year,
            start=start,
            rows=rows)
        params = {k: v for k, v in q30.items() if k != 'q'}
        
        self.logger.info(f"-- -- Q30 params: {params}")
        self.logger.info(f"-- -- Q30 q: {q30['q']}")

        sc, results = self.execute_query(
            q=q30['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q30. Aborting operation...")
            return

        return results.docs, sc
    
    def do_Q31(
        self,
        corpus_col: str,
    ) -> Union[dict, int]:
        """Executes query Q31.

        Parameters
        ----------
        corpus_col: str
            Name of the corpus collection

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """

        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 3. Execute query
        q31 = self.querier.customize_Q31()
        params = {k: v for k, v in q31.items() if k != 'q'}

        self.logger.info(f"-- -- Q31 params: {params}")
        self.logger.info(f"-- -- Q31 q: {q31['q']}")

        sc, results = self.execute_query(
            q=q31['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q31. Aborting operation...")
            return
        
        self.logger.info(f"facets: {results.facets}")
        
        buckets = (
            results.facets.get("years", {}).get("buckets", [])
        )

        items = []
        for b in buckets:
            val = b.get("val", "")
            # val is like "2024-01-01T00:00:00Z" → take first 4 chars as year
            try:
                year = int(val[:4])
            except Exception:
                continue
            count = int(b.get("count", 0))
            items.append((year, count))
        items.sort(key=lambda x: x[0])
        
        # convert into a dictionary
        items = [{"year": year, "count": count} for year, count in items]

        return items, sc
    
    def do_Q32(
        self,
        corpus_col: str,
        start: str,
        rows: str,
        sort_by_order: List[Tuple[str, str]] = [("date", "desc")],
        start_year: int = None,
        end_year: int = None,
        keyword: str = "*",
        searchable_field: str = '*',
    ) -> Union[dict, int]:
        """Executes query Q32.

        Parameters
        ----------
        corpus_col: str
            Name of the corpus collection
        start: str
            Index of the first document to be retrieved
        rows: str
            Number of documents to be retrieved
        sort_by_order: List[Tuple[str, str]]
            List of tuples with the field to sort by and the order (asc or desc)
        start_year: int
            Start year for filtering documents by date
        end_year: int
            End year for filtering documents by date
        keyword: str
            Keyword to search in the searchable_field
        searchable_field: str
            Field to search the keyword in
        date_field: str
            Field containing the date information
        display_fields: List[str]
            List of fields to be displayed in the results

        Returns
        -------
        json_object: dict
            JSON object with the results of the query.
        sc : int
            The status code of the response.
        """
        # 0. Convert corpus name to lowercase
        corpus_col = corpus_col.lower()

        # 1. Check that corpus_col is indeed a corpus collection
        if not self.check_is_corpus(corpus_col):
            return

        # 2. Get number of docs in the collection (it will be the maximum number of docs to be retireved) if rows is not specified
        if rows is None:
            q3 = self.querier.customize_Q3()
            params = {k: v for k, v in q3.items() if k != 'q'}

            sc, results = self.execute_query(
                q=q3['q'], col_name=corpus_col, **params)

            if sc != 200:
                self.logger.error(
                    f"-- -- Error executing query Q3. Aborting operation...")
                return
            rows = results.hits
        if start is None:
            start = str(0)

        # 2. Execute query
        q32 = self.querier.customize_Q32(
            sort_by_order=sort_by_order,
            start_year=start_year,
            end_year=end_year,
            keyword=keyword,
            searchable_field=searchable_field,
            date_field=self.date_field,
            display_fields=self.searchable_fields,
            start=start,
            rows=rows
        )
        params = {k: v for k, v in q32.items() if k != 'q'}
        
        self.logger.info(f"-- -- Q32 params: {params}")
        self.logger.info(f"-- -- Q32 q: {q32['q']}")

        sc, results = self.execute_query(
            q=q32['q'], col_name=corpus_col, **params)

        if sc != 200:
            self.logger.error(
                f"-- -- Error executing query Q32. Aborting operation...")
            return

        return results.docs, sc

    
    # =======================================================================
    # do_Q40 – Total procurement
    # =======================================================================
    def do_Q40(
        self,
        date_start:       str,
        date_end:         str,
        date_field:       str                 = "updated",
        tender_type:      Optional[str]       = None,
        cpv_prefixes:     Optional[List[str]] = None,
        cpv_field:        str                 = "cpv_list",
        budget_min:       Optional[float]     = None,
        budget_max:       Optional[float]     = None,
        budget_field:     str                 = "presupuesto_sin_iva",
        value_field:      str                 = "valor_estimado",
        id_field:         str                 = "id",
        subentidad:       Optional[str]       = None,
        cod_subentidad:   Optional[str]       = None,
        organo_id:        Optional[str]       = None,
        topic_model:      Optional[str]       = None,
        topic_id:         Optional[str]       = None,
        topic_min_weight: Optional[float]     = None,
        extra_fq:         Optional[List[str]] = None,
    ) -> Tuple[dict, int]:
        """
        Executes Q40 – Total procurement.
    
        Issues a single Solr request against the "place" collection using
        JSON Facets (one query-facet per bimester). No documents are
        transferred; all aggregation is server-side (rows=0).
    
        Parameters
        ----------
        date_start       : ISO-8601 UTC lower bound, e.g. "2025-01-01T00:00:00Z"
        date_end         : ISO-8601 UTC upper bound (exclusive)
        date_field       : temporal filter field ("updated" or "plazo_presentacion")
        tender_type      : "insiders" | "outsiders" | "minors" | None (all sources)
        cpv_prefixes     : CPV prefix list, e.g. ["48", "72", "73"]
        budget_min/max   : range filter on `presupuesto_sin_iva`
        subentidad       : region name filter
        cod_subentidad   : territorial code filter
        organo_id        : contracting authority id filter
        topic_model / topic_id / topic_min_weight : topic model filter
        extra_fq         : additional raw Solr fq clauses
    
        Returns
        -------
        (result, status_code)
    
        result = {
            "id":              "total_procurement",
            "bimester_labels": ["Ene–Feb 2025", ...],
            "by_count":        [142, 310, ...],     # unique tenders per bimester
            "by_budget":       [1.2e8, 3.4e8, ...], # sum(valor_estimado)/bimester
            "total_tenders":   1234,
            "total_budget":    9.87e9,
        }
        """
        if not self.check_is_corpus(_PLACE_COL):
            return {"error": f'"{_PLACE_COL}" is not a valid corpus collection'}, 400
    
        q  = self.querier.customize_Q40(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            value_field=value_field, id_field=id_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )
        ranges = q.pop("_meta")["ranges"]
        params = {k: v for k, v in q.items() if k != "q"}
    
        self.logger.info(
            f"do_Q40 | tender_type={tender_type} | fq={q['fq']}"
        )
    
        sc, results = self.execute_query(q=q["q"], col_name=_PLACE_COL, **params)
        if sc != 200:
            self.logger.error(f"do_Q40: Solr returned {sc}")
            return {"error": "Solr query failed"}, sc
    
        facets     = results.facets or {}
        by_count:  List = []
        by_budget: List = []
    
        for r in ranges:
            key = r["label"].replace(" ", "_").replace("–", "_")
            bim = facets.get(key, {})
            by_count.append(_safe(bim.get("n_tenders",    0)))
            by_budget.append(_safe(bim.get("total_budget", 0.0)))
    
        result = {
            "id":              "total_procurement",
            "bimester_labels": [r["label"] for r in ranges],
            "by_count":        by_count,
            "by_budget":       by_budget,
            "total_tenders":   sum(v for v in by_count  if v is not None),
            "total_budget":    sum(v for v in by_budget if v is not None),
        }
        return result, sc
 
 
    # =======================================================================
    # do_Q41 – Single bidder
    # =======================================================================
    def do_Q41(
        self,
        date_start:       str,
        date_end:         str,
        date_field:       str                 = "updated",
        tender_type:      Optional[str]       = None,
        cpv_prefixes:     Optional[List[str]] = None,
        cpv_field:        str                 = "cpv_list",
        budget_min:       Optional[float]     = None,
        budget_max:       Optional[float]     = None,
        budget_field:     str                 = "presupuesto_sin_iva",
        offers_field:     str                 = "ofertas_recibidas",
        id_field:         str                 = "id",
        subentidad:       Optional[str]       = None,
        cod_subentidad:   Optional[str]       = None,
        organo_id:        Optional[str]       = None,
        topic_model:      Optional[str]       = None,
        topic_id:         Optional[str]       = None,
        topic_min_weight: Optional[float]     = None,
        extra_fq:         Optional[List[str]] = None,
    ) -> Tuple[dict, int]:
        """
        Executes Q41 – Single bidder.
    
        Issues one Solr request per bimester against the "place" collection,
        fetching only id + ofertas_recibidas. The single-bidder ratio is
        computed in Python because `ofertas_recibidas` is a nested structure
        that Solr cannot aggregate natively.
    
        Returns
        -------
        (result, status_code)
    
        result = {
            "id":              "single_bidder",
            "bimester_labels": ["Ene–Feb 2025", ...],
            "pct_single_bid":  [73.1, 71.5, ...],  # % lots with exactly 1 offer
            "coverage":        [82.3, 90.1, ...],  # % lots where field is present
            "n_lots_total":    [4200, 5100, ...],   # total lots evaluated
        }
        """
        if not self.check_is_corpus(_PLACE_COL):
            return {"error": f'"{_PLACE_COL}" is not a valid corpus collection'}, 400
    
        queries = self.querier.customize_Q41(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            offers_field=offers_field, id_field=id_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )
    
        labels:       List = []
        pct_single:   List = []
        cov_list:     List = []
        n_lots_total: List = []
        last_sc = 200
    
        for q in queries:
            label  = q.pop("label")
            q.pop("_meta")
            params = {k: v for k, v in q.items() if k != "q"}
    
            self.logger.info(
                f"do_Q41 | bim={label} | tender_type={tender_type}"
            )
    
            sc, results = self.execute_query(
                q=q["q"], col_name=_PLACE_COL, **params
            )
            if sc != 200:
                self.logger.error(
                    f"do_Q41: Solr returned {sc} for bimester '{label}'"
                )
                last_sc = sc
                labels.append(label)
                pct_single.append(None)
                cov_list.append(None)
                n_lots_total.append(None)
                continue
    
            total_lots = lots_with_value = single_bid_lots = 0
    
            for doc in results.docs:
                lots = _parse_lot_offers(doc.get(offers_field))
                if not lots:
                    total_lots += 1          # doc has no lot info – one observation
                else:
                    for val in lots:
                        total_lots += 1
                        if val is not None:
                            lots_with_value += 1
                            if val == 1:
                                single_bid_lots += 1
    
            labels.append(label)
            cov_list.append(
                _safe(lots_with_value / total_lots * 100) if total_lots > 0 else None
            )
            pct_single.append(
                _safe(single_bid_lots / lots_with_value * 100)
                if lots_with_value > 0 else None
            )
            n_lots_total.append(total_lots)
    
        result = {
            "id":              "single_bidder",
            "bimester_labels": labels,
            "pct_single_bid":  pct_single,
            "coverage":        cov_list,
            "n_lots_total":    n_lots_total,
        }
        return result, last_sc
 
 
    # =======================================================================
    # do_Q42 – Decision speed
    # =======================================================================
    
    def do_Q42(
        self,
        date_start:       str,
        date_end:         str,
        date_field:       str                 = "updated",
        tender_type:      Optional[str]       = None,
        cpv_prefixes:     Optional[List[str]] = None,
        cpv_field:        str                 = "cpv_list",
        budget_min:       Optional[float]     = None,
        budget_max:       Optional[float]     = None,
        budget_field:     str                 = "presupuesto_sin_iva",
        deadline_field:   str                 = "plazo_presentacion",
        award_field:      str                 = "fecha_acuerdo",
        subentidad:       Optional[str]       = None,
        cod_subentidad:   Optional[str]       = None,
        organo_id:        Optional[str]       = None,
        topic_model:      Optional[str]       = None,
        topic_id:         Optional[str]       = None,
        topic_min_weight: Optional[float]     = None,
        extra_fq:         Optional[List[str]] = None,
    ) -> Tuple[dict, int]:
        """
        Executes Q42 – Decision speed.
    
        Issues one Solr request per bimester against the "place" collection,
        fetching only the two date fields. The average delta in days is
        computed in Python.
    
        Returns
        -------
        (result, status_code)
    
        result = {
            "id":              "decision_speed",
            "bimester_labels": ["Ene–Feb 2025", ...],
            "avg_days":        [32.1, 28.4, ...],
            "n_obs":           [1200, 980, ...],  # docs with both dates present
        }
        """
        if not self.check_is_corpus(_PLACE_COL):
            return {"error": f'"{_PLACE_COL}" is not a valid corpus collection'}, 400
    
        queries = self.querier.customize_Q42(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            deadline_field=deadline_field, award_field=award_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )
    
        labels:   List = []
        avg_days: List = []
        n_obs:    List = []
        last_sc = 200
    
        for q in queries:
            label = q.pop("label")
            q.pop("_meta")
            params = {k: v for k, v in q.items() if k != "q"}
    
            self.logger.info(
                f"do_Q42 | bim={label} | tender_type={tender_type}"
            )
    
            sc, results = self.execute_query(
                q=q["q"], col_name=_PLACE_COL, **params
            )
            if sc != 200:
                self.logger.error(
                    f"do_Q42: Solr returned {sc} for bimester '{label}'"
                )
                last_sc = sc
                labels.append(label)
                avg_days.append(None)
                n_obs.append(0)
                continue
    
            deltas: List[float] = []
            for doc in results.docs:
                # plazo_presentacion is a plain ISO string (one per tender);
                # fecha_acuerdo is a list of "lot_id|date" strings (one per lot).
                # We pair the single deadline with every award date.
                raw_deadline = doc.get(deadline_field)
                raw_awards   = doc.get(award_field)
 
                if not raw_deadline or not raw_awards:
                    continue
 
                # Normalise deadline to a single string
                if isinstance(raw_deadline, list):
                    deadline_str = _parse_date_field(raw_deadline)[0] if raw_deadline else None
                else:
                    deadline_str = raw_deadline
 
                if not deadline_str:
                    continue
 
                # Parse every award date and diff against the shared deadline
                for award_str in _parse_date_field(raw_awards):
                    delta = _date_diff_days(deadline_str, award_str)
                    if delta is not None and 0 <= delta <= 365:
                        deltas.append(delta)
    
            labels.append(label)
            avg_days.append(_safe(sum(deltas) / len(deltas)) if deltas else None)
            n_obs.append(len(deltas))
    
        result = {
            "id":              "decision_speed",
            "bimester_labels": labels,
            "avg_days":        avg_days,
            "n_obs":           n_obs,
        }
        return result, last_sc


    # =========================================================================
    # do_Q43 – Direct awards
    # =========================================================================
 
    def do_Q43(
        self,
        date_start:            str,
        date_end:              str,
        date_field:            str                 = "updated",
        tender_type:           Optional[str]       = None,
        cpv_prefixes:          Optional[List[str]] = None,
        cpv_field:             str                 = "cpv_list",
        budget_min:            Optional[float]     = None,
        budget_max:            Optional[float]     = None,
        budget_field:          str                 = "presupuesto_sin_iva",
        procedure_type_field:  str                 = "tipo_procedimiento",
        direct_award_value:    str                 = "Negociado sin publicidad",
        subentidad:            Optional[str]       = None,
        cod_subentidad:        Optional[str]       = None,
        organo_id:             Optional[str]       = None,
        topic_model:           Optional[str]       = None,
        topic_id:              Optional[str]       = None,
        topic_min_weight:      Optional[float]     = None,
        extra_fq:              Optional[List[str]] = None,
    ) -> Tuple[dict, int]:
        """
        Executes Q43 – Direct awards.
 
        Issues one Solr request per bimester fetching only the procedure-type
        field. The direct-award ratio and metadata coverage are computed in
        Python.
 
        Returns
        -------
        (result, status_code)
 
        result = {
            "id":              "direct_awards",
            "bimester_labels": ["Ene–Feb 2025", ...],
            "pct_direct":      [18.3, 15.1, ...],   # % direct-award procedures
            "coverage":        [100.0, 100.0, ...],  # % procedures with field present
            "n_tenders":       [1200, 980, ...],
        }
        """
        if not self.check_is_corpus(_PLACE_COL):
            return {"error": f'"{_PLACE_COL}" is not a valid corpus collection'}, 400
 
        queries = self.querier.customize_Q43(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            procedure_type_field=procedure_type_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )
 
        labels:    List = []
        pct_list:  List = []
        cov_list:  List = []
        n_list:    List = []
        last_sc = 200
 
        for q in queries:
            label = q.pop("label")
            q.pop("_meta")
            params = {k: v for k, v in q.items() if k != "q"}
 
            self.logger.info(f"do_Q43 | bim={label} | tender_type={tender_type}")
 
            sc, results = self.execute_query(q=q["q"], col_name=_PLACE_COL, **params)
            if sc != 200:
                self.logger.error(f"do_Q43: Solr returned {sc} for bimester '{label}'")
                last_sc = sc
                labels.append(label); pct_list.append(None)
                cov_list.append(None); n_list.append(None)
                continue
 
            total = with_value = direct = 0
            for doc in results.docs:
                total += 1
                val = doc.get(procedure_type_field)
                if val is not None:
                    with_value += 1
                    if val == direct_award_value:
                        direct += 1
 
            labels.append(label)
            cov_list.append(_safe(with_value / total * 100) if total > 0 else None)
            pct_list.append(_safe(direct / with_value * 100) if with_value > 0 else None)
            n_list.append(total)
 
        result = {
            "id":              "direct_awards",
            "bimester_labels": labels,
            "pct_direct":      pct_list,
            "coverage":        cov_list,
            "n_tenders":       n_list,
        }
        return result, last_sc
 
    # =========================================================================
    # do_Q44 – TED publication
    # =========================================================================
 
    def do_Q44(
        self,
        date_start:       str,
        date_end:         str,
        date_field:       str                 = "updated",
        tender_type:      Optional[str]       = None,
        cpv_prefixes:     Optional[List[str]] = None,
        cpv_field:        str                 = "cpv_list",
        budget_min:       Optional[float]     = None,
        budget_max:       Optional[float]     = None,
        budget_field:     str                 = "presupuesto_sin_iva",
        ted_field:        str                 = "ted_id",
        subentidad:       Optional[str]       = None,
        cod_subentidad:   Optional[str]       = None,
        organo_id:        Optional[str]       = None,
        topic_model:      Optional[str]       = None,
        topic_id:         Optional[str]       = None,
        topic_min_weight: Optional[float]     = None,
        extra_fq:         Optional[List[str]] = None,
    ) -> Tuple[dict, int]:
        """
        Executes Q44 – TED publication.
 
        Issues one Solr request per bimester fetching only the TED identifier
        field and computes the percentage of procedures published in TED.
 
        Returns
        -------
        (result, status_code)
 
        result = {
            "id":              "ted_publication",
            "bimester_labels": ["Ene–Feb 2025", ...],
            "pct_ted":         [18.0, 22.5, ...],  # % procedures with TED id
            "n_tenders":       [1200, 980, ...],
        }
        """
        if not self.check_is_corpus(_PLACE_COL):
            return {"error": f'"{_PLACE_COL}" is not a valid corpus collection'}, 400
 
        queries = self.querier.customize_Q44(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            ted_field=ted_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )
 
        labels:   List = []
        pct_list: List = []
        n_list:   List = []
        last_sc = 200
 
        for q in queries:
            label = q.pop("label")
            q.pop("_meta")
            params = {k: v for k, v in q.items() if k != "q"}
 
            self.logger.info(f"do_Q44 | bim={label} | tender_type={tender_type}")
 
            sc, results = self.execute_query(q=q["q"], col_name=_PLACE_COL, **params)
            if sc != 200:
                self.logger.error(f"do_Q44: Solr returned {sc} for bimester '{label}'")
                last_sc = sc
                labels.append(label); pct_list.append(None); n_list.append(None)
                continue
 
            total = with_ted = 0
            for doc in results.docs:
                total += 1
                val = doc.get(ted_field)
                if val is not None and val != "":
                    with_ted += 1
 
            labels.append(label)
            pct_list.append(_safe(with_ted / total * 100) if total > 0 else None)
            n_list.append(total)
 
        result = {
            "id":              "ted_publication",
            "bimester_labels": labels,
            "pct_ted":         pct_list,
            "n_tenders":       n_list,
        }
        return result, last_sc
 
    # =========================================================================
    # do_Q45 – SME participation (% lots with >= 1 SME offer)
    # =========================================================================
 
    def do_Q45(
        self,
        date_start:       str,
        date_end:         str,
        date_field:       str                 = "updated",
        tender_type:      Optional[str]       = None,
        cpv_prefixes:     Optional[List[str]] = None,
        cpv_field:        str                 = "cpv_list",
        budget_min:       Optional[float]     = None,
        budget_max:       Optional[float]     = None,
        budget_field:     str                 = "presupuesto_sin_iva",
        sme_field:        str                 = "ofertas_pymes",
        subentidad:       Optional[str]       = None,
        cod_subentidad:   Optional[str]       = None,
        organo_id:        Optional[str]       = None,
        topic_model:      Optional[str]       = None,
        topic_id:         Optional[str]       = None,
        topic_min_weight: Optional[float]     = None,
        extra_fq:         Optional[List[str]] = None,
    ) -> Tuple[dict, int]:
        """
        Executes Q45 – SME participation.
 
        Returns the percentage of lots in which at least one SME submitted
        an offer, together with field coverage.
 
        Returns
        -------
        (result, status_code)
 
        result = {
            "id":              "sme_participation",
            "bimester_labels": ["Ene–Feb 2025", ...],
            "pct_sme":         [62.4, 58.1, ...],  # % lots with >= 1 SME offer
            "coverage":        [97.5, 97.8, ...],
            "n_lots_total":    [4200, 5100, ...],
        }
        """
        if not self.check_is_corpus(_PLACE_COL):
            return {"error": f'"{_PLACE_COL}" is not a valid corpus collection'}, 400
 
        queries = self.querier.customize_Q45(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            sme_field=sme_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )
 
        labels:       List = []
        pct_list:     List = []
        cov_list:     List = []
        n_lots_total: List = []
        last_sc = 200
 
        for q in queries:
            label = q.pop("label")
            q.pop("_meta")
            params = {k: v for k, v in q.items() if k != "q"}
 
            self.logger.info(f"do_Q45 | bim={label} | tender_type={tender_type}")
 
            sc, results = self.execute_query(q=q["q"], col_name=_PLACE_COL, **params)
            if sc != 200:
                self.logger.error(f"do_Q45: Solr returned {sc} for bimester '{label}'")
                last_sc = sc
                labels.append(label); pct_list.append(None)
                cov_list.append(None); n_lots_total.append(None)
                continue
 
            total = with_value = sme_lots = 0
            for doc in results.docs:
                lots = _parse_lot_int_values(doc.get(sme_field))
                if not lots:
                    # Document without the field – skip (no observation)
                    continue
                for val in lots:
                    total += 1
                    if val is not None:
                        with_value += 1
                        if val >= 1:
                            sme_lots += 1
 
            labels.append(label)
            cov_list.append(_safe(with_value / total * 100) if total > 0 else None)
            pct_list.append(_safe(sme_lots / with_value * 100) if with_value > 0 else None)
            n_lots_total.append(total)
 
        result = {
            "id":              "sme_participation",
            "bimester_labels": labels,
            "pct_sme":         pct_list,
            "coverage":        cov_list,
            "n_lots_total":    n_lots_total,
        }
        return result, last_sc
 
    # =========================================================================
    # do_Q46 – SME offer ratio (sum(pyme_offers) / sum(total_offers))
    # =========================================================================
 
    def do_Q46(
        self,
        date_start:       str,
        date_end:         str,
        date_field:       str                 = "updated",
        tender_type:      Optional[str]       = None,
        cpv_prefixes:     Optional[List[str]] = None,
        cpv_field:        str                 = "cpv_list",
        budget_min:       Optional[float]     = None,
        budget_max:       Optional[float]     = None,
        budget_field:     str                 = "presupuesto_sin_iva",
        sme_field:        str                 = "ofertas_pymes",
        offers_field:     str                 = "ofertas_recibidas",
        subentidad:       Optional[str]       = None,
        cod_subentidad:   Optional[str]       = None,
        organo_id:        Optional[str]       = None,
        topic_model:      Optional[str]       = None,
        topic_id:         Optional[str]       = None,
        topic_min_weight: Optional[float]     = None,
        extra_fq:         Optional[List[str]] = None,
    ) -> Tuple[dict, int]:
        """
        Executes Q46 – SME offer ratio.
 
        Computes sum(sme_offers) / sum(total_offers) per bimester, expressed
        as a percentage, together with field coverage.
 
        Returns
        -------
        (result, status_code)
 
        result = {
            "id":              "sme_offer_ratio",
            "bimester_labels": ["Ene–Feb 2025", ...],
            "pct_sme_offers":  [54.2, 55.8, ...],  # % of all offers from SMEs
            "coverage":        [97.5, 97.8, ...],
            "n_lots_total":    [4200, 5100, ...],
        }
        """
        if not self.check_is_corpus(_PLACE_COL):
            return {"error": f'"{_PLACE_COL}" is not a valid corpus collection'}, 400
 
        queries = self.querier.customize_Q46(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            sme_field=sme_field, offers_field=offers_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )
 
        labels:       List = []
        pct_list:     List = []
        cov_list:     List = []
        n_lots_total: List = []
        last_sc = 200
 
        for q in queries:
            label = q.pop("label")
            q.pop("_meta")
            params = {k: v for k, v in q.items() if k != "q"}
 
            self.logger.info(f"do_Q46 | bim={label} | tender_type={tender_type}")
 
            sc, results = self.execute_query(q=q["q"], col_name=_PLACE_COL, **params)
            if sc != 200:
                self.logger.error(f"do_Q46: Solr returned {sc} for bimester '{label}'")
                last_sc = sc
                labels.append(label); pct_list.append(None)
                cov_list.append(None); n_lots_total.append(None)
                continue
 
            total_lots = with_value = sum_sme = sum_total = 0
            for doc in results.docs:
                total_vals = _parse_lot_int_values(doc.get(offers_field))
                sme_vals   = _parse_lot_int_values(doc.get(sme_field))
                # Align both lists by position; zip stops at the shorter one
                for t_val, s_val in zip(total_vals, sme_vals):
                    total_lots += 1
                    if t_val is not None and s_val is not None:
                        with_value += 1
                        sum_total  += t_val
                        sum_sme    += s_val
 
            labels.append(label)
            cov_list.append(_safe(with_value / total_lots * 100) if total_lots > 0 else None)
            pct_list.append(_safe(sum_sme / sum_total * 100) if sum_total > 0 else None)
            n_lots_total.append(total_lots)
 
        result = {
            "id":              "sme_offer_ratio",
            "bimester_labels": labels,
            "pct_sme_offers":  pct_list,
            "coverage":        cov_list,
            "n_lots_total":    n_lots_total,
        }
        return result, last_sc
 
    # =========================================================================
    # do_Q47 – Lots division (% procedures with more than one lot)
    # =========================================================================
 
    def do_Q47(
        self,
        date_start:       str,
        date_end:         str,
        date_field:       str                 = "updated",
        tender_type:      Optional[str]       = None,
        cpv_prefixes:     Optional[List[str]] = None,
        cpv_field:        str                 = "cpv_list",
        budget_min:       Optional[float]     = None,
        budget_max:       Optional[float]     = None,
        budget_field:     str                 = "presupuesto_sin_iva",
        lots_field:       str                 = "lotes",
        subentidad:       Optional[str]       = None,
        cod_subentidad:   Optional[str]       = None,
        organo_id:        Optional[str]       = None,
        topic_model:      Optional[str]       = None,
        topic_id:         Optional[str]       = None,
        topic_min_weight: Optional[float]     = None,
        extra_fq:         Optional[List[str]] = None,
    ) -> Tuple[dict, int]:
        """
        Executes Q47 – Lots division.
 
        Returns the percentage of procedures that contain more than one lot,
        together with field coverage (always 100 % if the field is indexed).
 
        Returns
        -------
        (result, status_code)
 
        result = {
            "id":              "lots_division",
            "bimester_labels": ["Ene–Feb 2025", ...],
            "pct_multi_lot":   [8.3, 9.1, ...],   # % procedures with > 1 lot
            "coverage":        [100.0, 100.0, ...],
            "n_tenders":       [1200, 980, ...],
        }
        """
        if not self.check_is_corpus(_PLACE_COL):
            return {"error": f'"{_PLACE_COL}" is not a valid corpus collection'}, 400
 
        queries = self.querier.customize_Q47(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            lots_field=lots_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )
 
        labels:    List = []
        pct_list:  List = []
        cov_list:  List = []
        n_list:    List = []
        last_sc = 200
 
        for q in queries:
            label = q.pop("label")
            q.pop("_meta")
            params = {k: v for k, v in q.items() if k != "q"}
 
            self.logger.info(f"do_Q47 | bim={label} | tender_type={tender_type}")
 
            sc, results = self.execute_query(q=q["q"], col_name=_PLACE_COL, **params)
            if sc != 200:
                self.logger.error(f"do_Q47: Solr returned {sc} for bimester '{label}'")
                last_sc = sc
                labels.append(label); pct_list.append(None)
                cov_list.append(None); n_list.append(None)
                continue
 
            total = with_value = multi_lot = 0
            for doc in results.docs:
                total += 1
                val = doc.get(lots_field)
                if val is not None:
                    with_value += 1
                    # Field is stored as a list of lot strings
                    n_lots = len(val) if isinstance(val, (list, tuple)) else 1
                    if n_lots > 1:
                        multi_lot += 1
 
            labels.append(label)
            cov_list.append(_safe(with_value / total * 100) if total > 0 else None)
            pct_list.append(_safe(multi_lot / with_value * 100) if with_value > 0 else None)
            n_list.append(total)
 
        result = {
            "id":              "lots_division",
            "bimester_labels": labels,
            "pct_multi_lot":   pct_list,
            "coverage":        cov_list,
            "n_tenders":       n_list,
        }
        return result, last_sc
 
    # =========================================================================
    # do_Q48 – Missing supplier ID
    # =========================================================================
 
    def do_Q48(
        self,
        date_start:        str,
        date_end:          str,
        date_field:        str                 = "updated",
        tender_type:       Optional[str]       = None,
        cpv_prefixes:      Optional[List[str]] = None,
        cpv_field:         str                 = "cpv_list",
        budget_min:        Optional[float]     = None,
        budget_max:        Optional[float]     = None,
        budget_field:      str                 = "presupuesto_sin_iva",
        identifier_field:  str                 = "identificador",
        subentidad:        Optional[str]       = None,
        cod_subentidad:    Optional[str]       = None,
        organo_id:         Optional[str]       = None,
        topic_model:       Optional[str]       = None,
        topic_id:          Optional[str]       = None,
        topic_min_weight:  Optional[float]     = None,
        extra_fq:          Optional[List[str]] = None,
    ) -> Tuple[dict, int]:
        """
        Executes Q48 – Missing supplier ID.
 
        For each awarded lot, checks whether a supplier identifier is present.
        The field is stored as a list of "lot_id|type|value" strings; a lot is
        considered to have a missing identifier when its value component is
        empty or absent.
 
        Returns
        -------
        (result, status_code)
 
        result = {
            "id":              "missing_supplier_id",
            "bimester_labels": ["Ene–Feb 2025", ...],
            "pct_missing":     [6.2, 5.8, ...],   # % lots without supplier id
            "n_lots_total":    [4200, 5100, ...],
        }
        """
        if not self.check_is_corpus(_PLACE_COL):
            return {"error": f'"{_PLACE_COL}" is not a valid corpus collection'}, 400
 
        queries = self.querier.customize_Q48(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            identifier_field=identifier_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )
 
        labels:   List = []
        pct_list: List = []
        n_list:   List = []
        last_sc = 200
 
        for q in queries:
            label = q.pop("label")
            q.pop("_meta")
            params = {k: v for k, v in q.items() if k != "q"}
 
            self.logger.info(f"do_Q48 | bim={label} | tender_type={tender_type}")
 
            sc, results = self.execute_query(q=q["q"], col_name=_PLACE_COL, **params)
            if sc != 200:
                self.logger.error(f"do_Q48: Solr returned {sc} for bimester '{label}'")
                last_sc = sc
                labels.append(label); pct_list.append(None); n_list.append(None)
                continue
 
            total = missing = 0
            for doc in results.docs:
                raw = doc.get(identifier_field)
                # Replicate notebook logic: skip docs without the field entirely
                # (not yet awarded); only evaluate docs that have it.
                if not isinstance(raw, (list, tuple)) or not raw:
                    continue
                for item in raw:
                    total += 1
                    try:
                        # Solr stores as "lot_id|type|value".
                        # null values from the parquet are serialised as the
                        # string "None", so we treat "None" as missing too.
                        parts = item.split("|")
                        supplier_id = parts[2].strip() if len(parts) > 2 else None
                        if supplier_id in (None, "", "None"):
                            missing += 1
                    except (AttributeError, IndexError):
                        missing += 1
 
            labels.append(label)
            pct_list.append(_safe(missing / total * 100) if total > 0 else None)
            n_list.append(total)
 
        result = {
            "id":              "missing_supplier_id",
            "bimester_labels": labels,
            "pct_missing":     pct_list,
            "n_lots_total":    n_list,
        }
        return result, last_sc
    
    # =========================================================================
    # do_Q49 – Missing buyer ID
    # =========================================================================
 
    def do_Q49(
        self,
        date_start:       str,
        date_end:         str,
        date_field:       str                 = "updated",
        tender_type:      Optional[str]       = None,
        cpv_prefixes:     Optional[List[str]] = None,
        cpv_field:        str                 = "cpv_list",
        budget_min:       Optional[float]     = None,
        budget_max:       Optional[float]     = None,
        budget_field:     str                 = "presupuesto_sin_iva",
        buyer_id_field:   str                 = "organo_id",
        subentidad:       Optional[str]       = None,
        cod_subentidad:   Optional[str]       = None,
        organo_id:        Optional[str]       = None,
        topic_model:      Optional[str]       = None,
        topic_id:         Optional[str]       = None,
        topic_min_weight: Optional[float]     = None,
        extra_fq:         Optional[List[str]] = None,
    ) -> Tuple[dict, int]:
        """
        Executes Q49 – Missing buyer ID.
 
        For each procedure, checks whether the contracting-authority identifier
        (organo_id) is present and non-empty.
 
        Returns
        -------
        (result, status_code)
 
        result = {
            "id":              "missing_buyer_id",
            "bimester_labels": ["Ene–Feb 2025", ...],
            "pct_missing":     [0.0, 0.1, ...],  # % procedures without buyer id
            "n_tenders":       [1200, 980, ...],
        }
        """
        if not self.check_is_corpus(_PLACE_COL):
            return {"error": f'"{_PLACE_COL}" is not a valid corpus collection'}, 400
 
        queries = self.querier.customize_Q49(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            buyer_id_field=buyer_id_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )
 
        labels:   List = []
        pct_list: List = []
        n_list:   List = []
        last_sc = 200
 
        for q in queries:
            label = q.pop("label")
            q.pop("_meta")
            params = {k: v for k, v in q.items() if k != "q"}
 
            self.logger.info(f"do_Q49 | bim={label} | tender_type={tender_type}")
 
            sc, results = self.execute_query(q=q["q"], col_name=_PLACE_COL, **params)
            if sc != 200:
                self.logger.error(f"do_Q49: Solr returned {sc} for bimester '{label}'")
                last_sc = sc
                labels.append(label); pct_list.append(None); n_list.append(None)
                continue
 
            total = missing = 0
            for doc in results.docs:
                total += 1
                val = doc.get(buyer_id_field)
                if val is None or (isinstance(val, str) and not val.strip()):
                    missing += 1
 
            labels.append(label)
            pct_list.append(_safe(missing / total * 100) if total > 0 else None)
            n_list.append(total)
 
        result = {
            "id":              "missing_buyer_id",
            "bimester_labels": labels,
            "pct_missing":     pct_list,
            "n_tenders":       n_list,
        }
        return result, last_sc
 