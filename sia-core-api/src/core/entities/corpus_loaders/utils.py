"""
Shared, corpus-agnostic helpers used by the per-corpus loaders in this package
(place.py, bdns.py, ...). Nothing here should know about a specific corpus_name.

Author: Lorena Calvo-Bartolomé
"""

import ast
import json
import math
import pathlib
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import pytz


def is_valid_xml_char_ordinal(i: int) -> bool:
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


def clean_xml_string(s: str) -> str:
    """
    Cleans string from invalid xml chars
    Solution was found there::
    http://stackoverflow.com/questions/8733233/filtering-out-certain-bytes-in-python
    """
    return "".join(c for c in s if is_valid_xml_char_ordinal(ord(c)))


def parse_time_instant(time) -> Optional[str]:
    """
    Parses a string or datetime-like value representing an instant in time.
    Supports ISO 8601 with timezone (e.g. '2024-12-30T13:52:11.444+01:00'),
    '%Y-%m-%d %H:%M:%S' string formats, and pandas Timestamp / datetime objects.

    Returns None (not the empty string) when the value is missing/unparseable,
    so it can be dropped cleanly from a Solr `pdate` field instead of being sent
    as an invalid empty-string date.
    """
    # Handle pandas NaT
    if time is pd.NaT or (isinstance(time, float) and math.isnan(time)):
        return None
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
    return None


def extract_nested(val, idx: int = 1):
    """Extract a value from a nested [[key, value, ...]] structure."""
    if not val or val == "":
        return None
    try:
        parsed = json.loads(val) if isinstance(val, str) else val
        return parsed[0][idx]
    except (ValueError, IndexError, TypeError):
        return None


def serialize_element(x):
    """Convert a scalar element to string, formatting dates with parse_time_instant."""
    if isinstance(x, (pd.Timestamp, datetime)):
        formatted = parse_time_instant(x)
        return formatted if formatted is not None else ""
    if isinstance(x, str):
        try:
            datetime.fromisoformat(x)
            formatted = parse_time_instant(x)
            return formatted if formatted is not None else x
        except ValueError:
            pass
    return str(x)


def parse_list_field(val, serialize_elements: bool = False, sep: Optional[str] = None):
    """Parse a field that may come as a string repr of a list or already as a list.

    - serialize_elements=True, sep=None  → each sub-element serialized as JSON string
    - serialize_elements=True, sep='|'   → each sub-list joined with sep (no escaping)

    Date-like elements (datetime, pd.Timestamp, or ISO 8601 strings) are formatted
    with parse_time_instant.
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
                sep.join(serialize_element(x) for x in v)
                if isinstance(v, list) else serialize_element(v)
                for v in result
            ]
        return [json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else serialize_element(v) for v in result]
    return [str(v) for v in result]


def parse_embedding(x):
    """Convert an embedding cell (np.ndarray, or a space-separated string of floats) to a list of floats."""
    if isinstance(x, np.ndarray):
        return x.tolist()
    if isinstance(x, str):
        return [float(v) for v in x.split()]
    return x


def is_valid_parquet(f: pathlib.Path) -> bool:
    """Cheap structural check (magic bytes) that a file is a readable parquet file."""
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


def _clean_value(v):
    if isinstance(v, dict):
        return {kk: _clean_value(vv) for kk, vv in v.items()}
    if isinstance(v, list):
        cleaned = [_clean_value(i) for i in v]
        return cleaned if cleaned else None
    if isinstance(v, (float, np.floating)) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


def clean_record(rec: dict) -> dict:
    """
    Replaces NaN/inf floats, pandas NA/NaT (nullable dtypes), None and empty lists
    with None throughout a document dict, so it can be safely JSON-serialized and
    sent to Solr (a missing/invalid value should simply be absent from the field,
    not crash the request or corrupt the index).
    """
    return {k: _clean_value(v) for k, v in rec.items()}


def alias_common_fields(df: pd.DataFrame, corpus) -> pd.DataFrame:
    """
    Renames the corpus's configured id/title/date source columns (corpus.id_field,
    corpus.title_field, corpus.date_field, read from the [<corpus>-config] section)
    onto the canonical 'id'/'title'/'date' columns the shared Solr fields expect.
    Every loader calls this the same way; only the underlying column names it
    operates on differ per corpus, driven entirely by config.cf.
    """
    if "id" in df.columns and "id" != corpus.id_field:
        df = df.rename(columns={"id": "id_"})
    df = df.rename(columns={corpus.id_field: "id"})
    if corpus.title_field != "title":
        df["title"] = df[corpus.title_field]
    if corpus.date_field != "date":
        df["date"] = df[corpus.date_field]
    return df


def build_searcheable_field(df: pd.DataFrame, corpus) -> pd.Series:
    """Concatenates the corpus's configured SearcheableField columns into one text blob per row."""
    searcheable = list(corpus.SearcheableField)
    if corpus.id_field in searcheable:
        searcheable.remove(corpus.id_field)
        searcheable.append("id")
    return df[searcheable].apply(lambda x: " ".join(x.astype(str)), axis=1)
