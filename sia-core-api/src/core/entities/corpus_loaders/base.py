"""
Base class for per-corpus document loaders.

Each corpus (place, bdns, ...) has its own on-disk layout and its own raw
column names, but must ultimately yield documents shaped for the same Solr
schema. A loader owns that corpus-specific mapping; nothing outside the
loader (Corpus, SIASolrClient) needs to know how a given corpus is laid out
on disk.

Author: Lorena Calvo-Bartolomé
"""

from abc import ABC, abstractmethod
from typing import Iterator, TYPE_CHECKING

if TYPE_CHECKING:
    from src.core.entities.corpus import Corpus


class BaseCorpusLoader(ABC):
    """Reads the raw corpus data for one logical corpus and yields Solr-ready documents."""

    def __init__(self, corpus: "Corpus") -> None:
        self.corpus = corpus
        self.logger = corpus._logger

    @abstractmethod
    def get_docs_metadata(self) -> Iterator[dict]:
        """Yields one dict per document, ready to be indexed in Solr."""
        raise NotImplementedError
