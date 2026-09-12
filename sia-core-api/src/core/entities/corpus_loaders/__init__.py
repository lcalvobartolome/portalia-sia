"""
Registry of per-corpus document loaders.

Adding a new corpus means writing a new BaseCorpusLoader subclass and adding
one entry here — nothing in Corpus or SIASolrClient needs to change.
"""

from typing import Dict, Type

from .base import BaseCorpusLoader
from .bdns import BdnsCorpusLoader
from .place import PlaceCorpusLoader

CORPUS_LOADERS: Dict[str, Type[BaseCorpusLoader]] = {
    "place": PlaceCorpusLoader,
    "bdns": BdnsCorpusLoader,
}

__all__ = ["BaseCorpusLoader", "CORPUS_LOADERS"]
