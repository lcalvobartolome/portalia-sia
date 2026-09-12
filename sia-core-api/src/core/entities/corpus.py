"""
This module is a class implementation to manage and hold all the information associated with a logical corpus.

The actual parquet-to-Solr-document transformation is corpus-specific and
lives in corpus_loaders/ (one loader class per corpus, selected via the
CORPUS_LOADERS registry) — this class only owns config loading and
orchestration, so it stays the same regardless of how many corpora exist.

Author: Lorena Calvo-Bartolomé
Date: 27/03/2023
Modified: 24/01/2024 (Updated for NP-Solr-Service (NextProcurement Project))
Modified: 13/04/2026 (Updated for SIA-Core-API (ALIA Project))
Modified: 12/09/2026 (Split per-corpus transformation logic into corpus_loaders/)
"""

import configparser
import pathlib
from typing import List

from src.core.entities.corpus_loaders import CORPUS_LOADERS


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
        section = f"{corpus_name}-config"
        if section not in cf.sections():
            self._logger.error(
                f"Corpus configuration {corpus_name} not found in config file.")

        # path_source can be overridden per-corpus (needed since each corpus'
        # data lives under its own mount); falls back to the global default.
        if cf.has_option(section, "path_source"):
            source = cf.get(section, "path_source")
        else:
            source = cf.get('restapi', 'path_source')
        self.path_source = pathlib.Path(source)

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

    def get_docs_metadata(self):
        """
        Reads the raw corpus file and yields the metadata of each document as a dictionary.
        """
        loader_cls = CORPUS_LOADERS.get(self.corpus_name)
        if loader_cls is None:
            raise NotImplementedError(
                f"{self.corpus_name} corpus processing not implemented yet.")
        yield from loader_cls(self).get_docs_metadata()

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
