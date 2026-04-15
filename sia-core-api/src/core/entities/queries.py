"""
This module defines a class with the NP-Solr-API specific queries used to interact with Solr.


Author: Lorena Calvo-Bartolomé
Date: 19/04/2023
"""
from __future__ import annotations
from datetime import datetime, timezone
from typing import List, Tuple, Optional
import json
 

def _year_bounds_utc(year: int) -> tuple[str, str]:
    """Return ISO8601 UTC bounds [start, end) for a calendar year."""
    start = datetime(year, 1, 1, tzinfo=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")
    end = datetime(
        year + 1, 1, 1, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return start, end

_DATE_FMT = "%Y-%m-%dT%H:%M:%SZ"
_MONTH_ES = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr",
    5: "May", 6: "Jun", 7: "Jul", 8: "Ago",
    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic",
}

def _bimester_ranges(date_start: str, date_end: str) -> List[dict]:
    """
    Partition [date_start, date_end) into natural bimester buckets
    (Jan–Feb, Mar–Apr, …). Only bimesters that overlap with the
    requested interval are returned.

    Returns a list of dicts:
        {"label": "Ene–Feb 2025", "start": "...", "end": "..."}
    where start/end are the effective (clamped) bounds.
    """
    t_start = datetime.strptime(date_start, _DATE_FMT).replace(tzinfo=timezone.utc)
    t_end   = datetime.strptime(date_end,   _DATE_FMT).replace(tzinfo=timezone.utc)

    year  = t_start.year
    month = 2 * ((t_start.month - 1) // 2) + 1  # round down to nearest odd month

    ranges: List[dict] = []
    while True:
        bim_start = datetime(year, month, 1, tzinfo=timezone.utc)
        nxt_month = month + 2
        nxt_year  = year
        if nxt_month > 12:
            nxt_month -= 12
            nxt_year  += 1
        bim_end = datetime(nxt_year, nxt_month, 1, tzinfo=timezone.utc)

        if bim_start >= t_end:
            break

        eff_start = max(bim_start, t_start)
        eff_end   = min(bim_end,   t_end)

        ranges.append({
            "label": f"{_MONTH_ES[month]}–{_MONTH_ES[month + 1]} {year}",
            "start": eff_start.strftime(_DATE_FMT),
            "end":   eff_end.strftime(_DATE_FMT),
        })

        month += 2
        if month > 12:
            month -= 12
            year  += 1

    return ranges


def _build_common_fq(
    date_field:       str,
    date_start:       str,
    date_end:         str,
    tender_type:      Optional[str],
    cpv_prefixes:     Optional[List[str]],
    cpv_field:        str,
    budget_min:       Optional[float],
    budget_max:       Optional[float],
    budget_field:     str,
    subentidad:       Optional[str],
    cod_subentidad:   Optional[str],
    organo_id:        Optional[str],
    topic_model:      Optional[str],
    topic_id:         Optional[str],
    topic_min_weight: Optional[float],
    extra_fq:         Optional[List[str]],
) -> List[str]:
    """
    Build the list of Solr filter query clauses shared by all indicators.
    Returns a list so callers can filter individual clauses (e.g. replace
    the date range per bimester). Convert to a single fq string with
    ' AND '.join(result) before passing to execute_query.
    """
    clauses: List[str] = []

    # 4.1 – Data source
    if tender_type is not None:
        clauses.append(f"tender_type:{tender_type}")

    # 4.2 – Temporal range (exclusive upper bound: [start TO end} )
    clauses.append(f"{date_field}:[{date_start} TO {date_end}}}")

    # 4.3 – Budget range
    if budget_min is not None or budget_max is not None:
        lo = str(budget_min) if budget_min is not None else "*"
        hi = str(budget_max) if budget_max is not None else "*"
        clauses.append(f"{budget_field}:[{lo} TO {hi}]")

    # 4.4 – CPV classification (prefix match)
    if cpv_prefixes:
        terms = " OR ".join(f"{p}*" for p in cpv_prefixes)
        clauses.append(f"{cpv_field}:({terms})")

    # 4.5 – Geography
    if subentidad:
        clauses.append(f'subentidad_nacional:"{subentidad.replace(chr(34), chr(92) + chr(34))}"')
    if cod_subentidad:
        clauses.append(f"codigo_subentidad_territorial:{cod_subentidad}")

    # 4.6 – Contracting authority
    if organo_id:
        clauses.append(f"organo_id:{organo_id}")

    # 4.8 – Topic model filter
    if topic_model and topic_id is not None and topic_min_weight is not None:
        clauses.append(
            f"{{!payload_check f=doctpc_{topic_model} v={topic_id} "
            f"op=gte payloadVal={topic_min_weight}}}"
        )

    # Caller-supplied extra filters
    if extra_fq:
        clauses.extend(extra_fq)

    return clauses

def _bimester_fq(base_fq: List[str], date_field: str, r: dict) -> str:
    """
    Replace the global date-range clause in base_fq with the
    bimester-specific one. Receives a list, returns a single fq string.
    """
    prefix = f"{date_field}:["
    clauses = [c for c in base_fq if not c.startswith(prefix)]
    clauses.append(f"{date_field}:[{r['start']} TO {r['end']}}}")
    return " AND ".join(clauses)


class Queries(object):

    def __init__(self) -> None:

        # ================================================================
        # # Q1: getThetasDocById  ##################################################################
        # # Get document-topic distribution of a selected document in a
        # # corpus collection
        # http://localhost:8983/solr/{col}/select?fl=doctpc_{model}&q=id:{id}
        # ================================================================
        self.Q1 = {
            'q': 'id:"{}"',
            'fl': 'doctpc_{}',
        }

        # ================================================================
        # # Q2: getCorpusMetadataFields  ##################################################################
        # # Get the name of the metadata fields available for
        # a specific corpus collection (not all corpus have
        # the same metadata available)
        # http://localhost:8983/solr/#/Corpora/query?q=corpus_name:Cordis&q.op=OR&indent=true&fl=fields&useParams=
        # ================================================================
        self.Q2 = {
            'q': 'corpus_name:{}',
            'fl': 'fields',
        }

        # ================================================================
        # # Q3: getNrDocsColl ##################################################################
        # # Get number of documents in a collection
        # http://localhost:8983/solr/{col}/select?q=*:*&wt=json&rows=0
        # ================================================================
        self.Q3 = {
            'q': '*:*',
            'rows': '0',
        }

        # ================================================================
        # # Q5: getDocsWithHighSimWithDocByid
        # ################################################################
        # # Retrieve documents that have a high semantic relationship with
        # # a selected document
        # ---------------------------------------------------------------
        # Previous steps:
        # ---------------------------------------------------------------
        # 1. Get thetas of selected documents
        # 2. Parse thetas in Q1
        # 3. Execute Q4
        # ================================================================
        self.Q5 = {
            'q': "{{!vd f=doctpc_{} vector=\"{}\" distance=\"{}\"}}",
            'fl': "id,generated_objective,link,score",
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # # Q6: getMetadataDocById
        # ################################################################
        # # Get metadata of a selected document in a corpus collection
        # ---------------------------------------------------------------
        # Previous steps:
        # ---------------------------------------------------------------
        # 1. Get metadata fields of that corpus collection with Q2
        # 2. Parse metadata in Q6
        # 3. Execute Q6
        # ================================================================
        self.Q6 = {
            'q': 'id:"{}"',
            'fl': '{}'
        }

        # ================================================================
        # # Q7: getDocsWithString
        # ################################################################
        # # Retrieves the ids of the documents in a corpus collections in which a given field contains a given string.
        # http://localhost:8983/solr/#/{collection}/query?q=title:{string}&q.op=OR&indent=true&useParams=
        # ================================================================
        self.Q7 = {
            'q': '{}:{}',
            'fl': 'id,generated_objective',
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # # Q8: getTopicsLabels
        # ################################################################
        # # Get the label associated to each of the topics in a given model
        # http://localhost:8983/solr/{model}/select?fl=id%2C%20tpc_labels&indent=true&q.op=OR&q=*%3A*&useParams=
        # ================================================================
        self.Q8 = {
            'q': '*:*',
            'fl': 'id,tpc_labels',
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # # Q9: getTopicTopDocs
        # ################################################################
        # # Get the top documents for a given topic in a model collection
        # http://localhost:8983/solr/cordis/select?indent=true&q.op=OR&q=%7B!term%20f%3D{model}%7Dt{topic_id}&useParams=
        # http://localhost:8983/solr/#/{corpus_collection}/query?q=*:*&q.op=OR&indent=true&fl=doctpc_{model_name},%20nwords_per_doc&sort=payload(doctpc_{model_name},t{topic_id})%20desc,%20nwords_per_doc%20desc&useParams=
        # http://localhost:8983/solr/#/np_all/query?q=*:*&q.op=OR&indent=true&fl=doctpc_np_5tpcs,%20nwords_per_doc&sort=payload(doctpc_np_5tpcs,t0)%20desc,%20nwords_per_doc%20desc&useParams=
        # ================================================================
        self.Q9 = {
            'q': '*:*',
            'sort': 'payload(doctpc_{},t{}) desc, nwords_per_doc desc',
            'fl': 'payload(doctpc_{},t{}), generated_objective, nwords_per_doc, id',
            'fq': [
                'doctpc_{}:[* TO *]',           
                'generated_objective:[* TO *]',      
                'nwords_per_doc:[* TO *]'     
            ],
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # # Q10: getModelInfo
        # ################################################################
        # # Get the information (chemical description, label, statistics,
        # top docs, etc.) associated to each topic in a model collection
        # ================================================================
        self.Q10 = {
            'q': '*:*',
            'fl': 'id,alphas,top_words_betas,topic_entropy,topic_coherence,ndocs_active,tpc_descriptions,tpc_labels,coords',
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # # Q14: getDocsSimilarToFreeTextTM
        # ################################################################
        # # Get documents that are semantically similar to a free text
        # according to a given model
        # ================================================================
        self.Q14 = self.Q5

        # ================================================================
        # # Q15: getLemmasDocById  ##################################################################
        # # Get lemmas of a selected document in a corpus collection
        # http://localhost:8983/solr/{col}/select?fl=lemmas&q=id:{id}
        # http://localhost:8983/solr/np_all/select?fl=lemmas&q=id:505302
        # ================================================================
        self.Q15 = {
            'q': 'id:"{}"',
            'fl': 'lemmas',
            'start': '{}',
            'rows': '{}'
        }

        # If adding a new one, start numeration at 20
        # ================================================================
        # # Q20: getDocsRelatedToWord
        # # Get documents related to a word according to a given topic model.
        # # To calculate this, we use a Word2Vec model trained on the same data as the topic model. In this model, the chemical descriptions of topics are embedded in the same space as words. When a word is inputted, it is embedded in this space, and the closest topic, based on its chemical description, is selected. Subsequently, the documents associated with that topic are retrieved.
        # ##################################################################
        self.Q20 = {
            'q': "{{!knn f=tpc_embeddings topK=100}}{}",
            'fl': "id,generated_objective,link,place_id,score",
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # # Q21: getDocsSimilarToFreeTextEmb
        # # Retrieve documents that are semantically similar to a given free text using BERT embeddings. The free text is represented by its BERT embeddings, and these embeddings for the documents in the collection are precalculated and indexed into Solr for efficient retrieval.
        # ################################################################
        self.Q21 = {
            'q': "{{!knn f=embeddings topK=100}}{}",
            'fl': "id,generated_objective,link,place_id,score",
            'start': '{}',
            'rows': '{}'
        }
        # ================================================================
        # # Q21_e: getDocsSimilarToFreeTextEmbAndBM25
        self.Q21_e = {
            'q': "{{!knn f=embeddings topK=100}}{}",
            'fq': '{{!edismax qf={}}} {}',
            'fl': 'id,generated_objective,link,place_id,score',
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # Q30: getDocsByYear
        #   - Uses an fq date range [start TO end} (end-exclusive)
        #   - Defaults to field 'date'
        # ################################################################
        self.Q30 = {
            'q': '*:*',
            'fq': '{}:[{} TO {}}}',
            'fl': '{}',
            'start': '{}',
            'rows': '{}'
        }

        # ================================================================
        # Q31: getAllYears
        #   - JSON Facet: yearly buckets on 'date' from a start year to NOW
        #   - NOTE: the parameter key must be exactly 'json.facet'
        # ################################################################
        self.Q31 = {
            'q': '*:*',
            'rows': '0',
            # will be filled with a JSON string like:
            # {"years":{"type":"range","field":"date","start":"2000-01-01T00:00:00Z","end":"NOW","gap":"+1YEAR","hardend":true}}
            'json.facet': '{}'
        }

        # ================================================================
        # Q32: getDocsByYearAugmented
        # - it includes a search keyword in a given field (e.g., title, objective, etc.) like Q7; if no keyword is needed, use '*'
        # - it includes a date range like Q30
        # - it includes a SortBY and SortOrder (e.g., 'date desc' or 'id asc')
        # ################################################################
        self.Q32 = {
            'q': '{}:{}',
            'fq': '{}:[{} TO {}}}',
            'sort': '{}',
            'fl': '{}',
            'start': '{}',
            'rows': '{}'
        }

    def customize_Q1(self,
                     id: str,
                     model_name: str) -> dict:
        """Customizes query Q1 'getThetasDocById'.

        Parameters
        ----------
        id: str
            Document id.
        model_name: str
            Name of the topic model whose topic distribution is to be retrieved.

        Returns
        -------
        custom_q1: dict
            Customized query Q1.
        """

        custom_q1 = {
            'q': self.Q1['q'].format(id),
            'fl': self.Q1['fl'].format(model_name),
        }
        return custom_q1

    def customize_Q2(self,
                     corpus_name: str) -> dict:
        """Customizes query Q2 'getCorpusMetadataFields'

        Parameters
        ----------
        corpus_name: str
            Name of the corpus collection whose metadata fields are to be retrieved.

        Returns
        -------
        custom_q2: dict
            Customized query Q2.
        """

        custom_q2 = {
            'q': self.Q2['q'].format(corpus_name),
            'fl': self.Q2['fl'],
        }

        return custom_q2

    def customize_Q3(self) -> dict:
        """Customizes query Q3 'getNrDocsColl'

        Returns
        -------
        self.Q3: dict
            The query Q3 (no customization is needed).
        """

        return self.Q3

    def customize_Q5(self,
                     model_name: str,
                     thetas: str,
                     distance: str,
                     start: str,
                     rows: str) -> dict:
        """Customizes query Q5 'getDocsWithHighSimWithDocByid'

        Parameters
        ----------
        model_name: str
            Name of the topic model whose topic distribution is to be retrieved.
        thetas: str
            Topic distribution of the selected document.
        distance: str
            Distance metric to be used.
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.

        Returns
        -------
        custom_q5: dict
            Customized query Q5.
        """

        custom_q5 = {
            'q': self.Q5['q'].format(model_name, thetas, distance),
            'fl': self.Q5['fl'].format(model_name),
            'start': self.Q5['start'].format(start),
            'rows': self.Q5['rows'].format(rows),
        }
        return custom_q5

    def customize_Q6(self,
                     id: str,
                     meta_fields: str) -> dict:
        """Customizes query Q6 'getMetadataDocById'


        Parameters
        ----------
        id: str
            Document id.
        meta_fields: str
            Metadata fields of the corpus collection to be retrieved.

        Returns
        -------
        custom_q6: dict
            Customized query Q6.
        """

        custom_q6 = {
            'q': self.Q6['q'].format(id),
            'fl': self.Q6['fl'].format(meta_fields)
        }

        return custom_q6

    def customize_Q7(self,
                     title_field: str,
                     string: str,
                     start: str,
                     rows: str) -> dict:
        """Customizes query Q7 'getDocsWithString'

        Parameters
        ----------
        title_field: str
            Title field of the corpus collection.
        string: str
            String to be searched in the title field.
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.

        Returns
        -------
        custom_q7: dict
            Customized query Q7.
        """

        custom_q7 = {
            'q': self.Q7['q'].format(title_field, string),
            'fl': self.Q7['fl'],
            'start': self.Q7['start'].format(start),
            'rows': self.Q7['rows'].format(rows)
        }

        return custom_q7

    def customize_Q8(self,
                     start: str,
                     rows: str) -> dict:
        """Customizes query Q8 'getTopicsLabels'

        Parameters
        ----------
        rows: str
            Number of rows to retrieve.
        start: str
            Start value.

        Returns
        -------
        self.Q8: dict
            The query Q8
        """

        custom_q8 = {
            'q': self.Q8['q'],
            'fl': self.Q8['fl'],
            'start': self.Q8['start'].format(start),
            'rows': self.Q8['rows'].format(rows),
        }

        return custom_q8

    def customize_Q9(self,
                     model_name: str,
                     topic_id: str,
                     start: str,
                     rows: str) -> dict:
        """Customizes query Q9 'getDocsByTopic'

        Parameters
        ----------
        model_name: str
            Name of the topic model whose topic distribution is going to be used for retreving the top documents for the topic given by 'topic'.
        topic_id: str
            Topic number.
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.

        Returns
        -------
        custom_q9: dict
            Customized query Q9.
        """

        custom_q9 = {
            'q': self.Q9['q'],
            'sort': self.Q9['sort'].format(model_name, topic_id),
            'fl': self.Q9['fl'].format(model_name, topic_id),
            #'fq': self.Q9['fq'],
            'start': self.Q9['start'].format(start),
            'rows': self.Q9['rows'].format(rows),
        }
        #custom_q9["fq"][0] = custom_q9["fq"][0].format(model_name)
        
        return custom_q9

    def customize_Q10(self,
                      start: str,
                      rows: str,
                      only_id: bool) -> dict:
        """Customizes query Q10 'getModelInfo'

        Parameters
        ----------
        start: str
            Start value.
        rows: str

        Returns
        -------
        custom_q10: dict
            Customized query Q10.
        """

        if only_id:
            custom_q10 = {
                'q': self.Q10['q'],
                'fl': 'id',
                'start': self.Q10['start'].format(start),
                'rows': self.Q10['rows'].format(rows),
            }
        else:
            custom_q10 = {
                'q': self.Q10['q'],
                'fl': self.Q10['fl'],
                'start': self.Q10['start'].format(start),
                'rows': self.Q10['rows'].format(rows),
            }

        return custom_q10

    def customize_Q14(self,
                      model_name: str,
                      thetas: str,
                      distance: str,
                      start: str,
                      rows: str) -> dict:
        """Customizes query Q14 'getDocsSimilarToFreeText'

        Parameters
        ----------
        model_name: str
            Name of the topic model whose topic distribution is to be retrieved.
        thetas: str
            Topic distribution of the user's free text.
        distance: str
            Distance metric to be used.
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.

        Returns
        -------
        custom_q14: dict
            Customized query Q14.
        """

        custom_q14 = {
            'q': self.Q14['q'].format(model_name, thetas, distance),
            'fl': self.Q14['fl'].format(model_name),
            'start': self.Q14['start'].format(start),
            'rows': self.Q14['rows'].format(rows),
        }
        return custom_q14

    def customize_Q15(self,
                      id: str) -> dict:
        """Customizes query Q15 'getLemmasDocById'.

        Parameters
        ----------
        id: str
            Document id.

        Returns
        -------
        custom_q15: dict
            Customized query Q15.
        """

        custom_q15 = {
            'q': self.Q15['q'].format(id),
            'fl': self.Q15['fl'],
        }
        return custom_q15

    def customize_Q20(
        self,
        wd_embeddings: str,
        start: str,
        rows: str
    ) -> dict:
        """Customizes query Q20 'getDocsRelatedToDoc'

        Parameters
        ----------
        wd_embeddings: str
            Word embeddings of the user's free word.
        distance: str
            Distance metric to be used.
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.

        Returns
        -------
        custom_q5: dict
            Customized query Q5.
        """

        custom_q20 = {
            'q': self.Q20['q'].format(wd_embeddings),
            'fl': self.Q20['fl'],
            'start': self.Q20['start'].format(start),
            'rows': self.Q20['rows'].format(rows),
        }
        return custom_q20

    def customize_Q21(
        self,
        doc_embeddings: str,
        start: str,
        rows: str
    ) -> dict:
        """Customizes query Q21 'getDocsSimilarToFreeTextEmb'

        Parameters
        ----------
        doc_embeddings: str
            Embeddings of the user's free doc.
        distance: str
            Distance metric to be used.
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.

        Returns
        -------
        custom_q5: dict
            Customized query Q5.
        """

        custom_q21 = {
            'q': self.Q21['q'].format(doc_embeddings),
            'fl': self.Q21['fl'],
            'start': self.Q21['start'].format(start),
            'rows': self.Q21['rows'].format(rows),
        }
        return custom_q21

    def customize_Q21_e(
        self,
        doc_embeddings: str,
        keyword: str,
        start: str,
        rows: str,
        query_fields: str
    ) -> dict:
        """Customizes query Q21_e 'getDocsSimilarToFreeTextEmbAndBM25'

        Parameters
        ----------
        doc_embeddings: str
            Embeddings of the user's free doc.
        distance: str
            Distance metric to be used.
        start: str
            Start value.
        rows: str
            Number of rows to retrieve.

        Returns
        -------
        custom_q21_e: dict
            Customized query Q21_e.
        """

        custom_q21_e = {
            'q': self.Q21_e['q'].format(doc_embeddings),
            'fq': self.Q21_e['fq'].format(query_fields, keyword),
            'fl': self.Q21_e['fl'],
            'start': self.Q21_e['start'].format(start),
            'rows': self.Q21_e['rows'].format(rows),
        }
        return custom_q21_e

    def customize_Q30(
        self,
        year: int,
        start: str = '0',
        rows: str = '10',
        date_field: str = 'date',
        display_fields: str = 'id,title,generated_objective,cpv,cpv_predicted,criterios_adjudicacion,criterios_solvencia,condiciones_especiales'
    ) -> dict:
        """
        Build an fq that selects the given calendar year (UTC) on "date_field".
        """
        s, e = _year_bounds_utc(int(year))
        custom_q30 = {
            'q': self.Q30['q'],
            'fq': self.Q30['fq'].format(date_field, s, e),
            'fl': self.Q30['fl'].format(display_fields),
            'start': self.Q30['start'].format(start),
            'rows': self.Q30['rows'].format(rows),
        }
        return custom_q30

    def customize_Q31(
        self,
        start_year: int = 2000,
        date_field: str = 'date'
    ) -> dict:
        """
        Yearly range facet from Jan 1 of "start_year" up to NOW on "date_field".
        Returns rows=0 and a json.facet param ready for /select.
        """
        start_iso, _ = _year_bounds_utc(int(start_year))
        facet_json = (
            '{'
            f'"years":{{'
            f'"type":"range","field":"{date_field}",'
            f'"start":"{start_iso}","end":"NOW","gap":"+1YEAR","hardend":true'
            f'}}'
            '}'
        )
        custom_q31 = {
            'q': self.Q31['q'],
            'rows': self.Q31['rows'],
            'json.facet': self.Q31['json.facet'].format(facet_json)
        }
        return custom_q31

    def customize_Q32(
        self,
        sort_by_order: List[Tuple[str, str]],
        start_year: int,
        end_year: int,
        keyword: str,
        searchable_field: str,
        date_field: str,
        display_fields: str,
        start: str = '0',
        rows: str = '10',
    ) -> dict:
        """
        - Search keyword in a given field (use '*' for no keyword filter).
        - If BOTH start_year and end_year are provided, apply a date-range filter.
        Otherwise, return all years (no date filter).
        - Supports multiple sort fields with asc/desc.
        """

        # Build sort string
        sort_by_order_lst = []
        for sb, so in sort_by_order:
            if so.lower() not in ('asc', 'desc'):
                raise ValueError("Sort order must be either 'asc' or 'desc'.")
            sort_by_order_lst.append(f'{sb} {so.lower()}')
        sort_by_order_str = ', '.join(sort_by_order_lst)

        # Decide whether to apply the date filter
        apply_date_filter = (start_year is not None or end_year is not None)
        if apply_date_filter:
            if start_year is not None and end_year is not None:
                if start_year > end_year:
                    raise ValueError("'start_year' must be <= 'end_year'.")
                s, _ = _year_bounds_utc(start_year)
                _, e = _year_bounds_utc(end_year)
            elif start_year is not None:
                s, _ = _year_bounds_utc(start_year)
                _, e = _year_bounds_utc(start_year)
            elif end_year is not None:
                s, _ = _year_bounds_utc(end_year)
                _, e = _year_bounds_utc(end_year)
                
        # Assemble query
        custom_q32 = {
            'q': self.Q32['q'].format(searchable_field, keyword),
            'sort': self.Q32['sort'].format(sort_by_order_str),
            'fl': self.Q32['fl'].format(display_fields),
            'start': self.Q32['start'].format(start),
            'rows': self.Q32['rows'].format(rows),
        }

        # Only include fq if both years are set; otherwise return all years
        if apply_date_filter:
            custom_q32['fq'] = self.Q32['fq'].format(date_field, s, e)

        return custom_q32
    
    """
    Indicators
    ----------
    Q40 – Total procurement  : count of tenders + sum of budget, per bimester
    Q41 – Single bidder      : % of lots with exactly 1 offer, per bimester
    Q42 – Decision speed     : avg days between deadline and award, per bimester
    
    Filter set (section 4 of the functional spec)
    ---------------------------------------------
    tender_type               : "insiders" | "outsiders" | "minors" | None
    date_start / date_end     : temporal range on "date_field"
    date_field                : "updated" or "plazo_presentacion"
    budget_min / budget_max   : range on "presupuesto_sin_iva"
    cpv_prefixes              : prefix match on "cpv_list"
    subentidad                : region name  ("subentidad_nacional")
    cod_subentidad            : territorial code ("codigo_subentidad_territorial")
    organo_id                 : contracting authority ("organo_id")
    topic_model --> to be specified @TODO
    """


    # =========================================================================
    # Q40 – Total procurement
    # =========================================================================
    def customize_Q40(
        self,
        date_field:       str                 = "updated",
        date_start:       str                 = "2025-01-01T00:00:00Z",
        date_end:         str                 = "2026-01-01T00:00:00Z",
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
    ) -> dict:
        """
        Customize Q40 – Total procurement.

        Uses Solr JSON Facets (one query-type sub-facet per bimester) to
        aggregate server-side. No documents are transferred (rows=0).

        Solr json.facet structure
        -------------------------
        {
        "Ene_Feb_2025": {
            "type": "query",
            "q": "<date_field>:[start TO end}]",
            "facet": {
            "n_tenders":    "unique(<id_field>)",
            "total_budget": "sum(<value_field>)"
            }
        },
        ...one entry per bimester...
        }

        Returns
        -------
        dict with keys:
            q, fq, rows, json.facet  – forwarded to Solr
            _meta                    – consumed by do_Q40, not sent to Solr
        """
        ranges  = _bimester_ranges(date_start, date_end)
        base_fq = _build_common_fq(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )

        per_bim: dict = {}
        for r in ranges:
            key = r["label"].replace(" ", "_").replace("–", "_")
            per_bim[key] = {
                "type":  "query",
                "q": f"{date_field}:[{r['start']} TO {r['end']}}}",
                "facet": {
                    "n_tenders":    f"unique({id_field})",
                    "total_budget": f"sum({value_field})",
                },
            }

        return {
            "q":          "*:*",
            "fq":         " AND ".join(base_fq),   
            "rows":       "0",
            "json.facet": json.dumps(per_bim),
            "_meta":      {"ranges": ranges},
        }


    # =========================================================================
    # Q41 – Single bidder
    # =========================================================================
    def customize_Q41(
        self,
        date_field:       str                 = "updated",
        date_start:       str                 = "2025-01-01T00:00:00Z",
        date_end:         str                 = "2026-01-01T00:00:00Z",
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
    ) -> List[dict]:
        """
        Customize Q41 – Single bidder.

        Returns one Solr query dict per bimester, fetching only id +
        ofertas_recibidas. The single-bidder ratio is computed in Python
        (do_Q41) because the field is a nested structure that Solr cannot
        aggregate natively.

        Returns
        -------
        List of dicts, each with keys:
            label           – bimester label (consumed by do_Q41)
            q, fq, fl, rows – forwarded to Solr
            _meta           – consumed by do_Q41, not sent to Solr
        """
        ranges  = _bimester_ranges(date_start, date_end)
        base_fq = _build_common_fq(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )

        return [
            {
                "label":  r["label"],
                "q":      "*:*",
                "fq":     _bimester_fq(base_fq, date_field, r),
                "fl":     f"{id_field},{offers_field}",
                "rows":   "1000000",
                "_meta":  {"range": r},
            }
            for r in ranges
        ]


    # =========================================================================
    # Q42 – Decision speed
    # =========================================================================
    def customize_Q42(
        self,
        date_field:       str                 = "updated",
        date_start:       str                 = "2025-01-01T00:00:00Z",
        date_end:         str                 = "2026-01-01T00:00:00Z",
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
    ) -> List[dict]:
        """
        Customize Q42 – Decision speed.

        Returns one Solr query dict per bimester, fetching only the two date
        fields. The average delta in days is computed in Python by do_Q42.

        Returns
        -------
        List of dicts, each with keys:
            label           – bimester label (consumed by do_Q42)
            q, fq, fl, rows – forwarded to Solr
            _meta           – consumed by do_Q42, not sent to Solr
        """
        ranges  = _bimester_ranges(date_start, date_end)
        base_fq = _build_common_fq(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )

        return [
            {
                "label": r["label"],
                "q":     "*:*",
                "fq":    _bimester_fq(base_fq, date_field, r),
                "fl":    f"{deadline_field},{award_field}",
                "rows":  "1000000",
                "_meta": {"range": r},
            }
            for r in ranges
        ]

    # =========================================================================
    # Q43 – Direct awards
    # =========================================================================
    def customize_Q43(
        self,
        date_field:            str                 = "updated",
        date_start:            str                 = "2025-01-01T00:00:00Z",
        date_end:              str                 = "2026-01-01T00:00:00Z",
        tender_type:           Optional[str]       = None,
        cpv_prefixes:          Optional[List[str]] = None,
        cpv_field:             str                 = "cpv_list",
        budget_min:            Optional[float]     = None,
        budget_max:            Optional[float]     = None,
        budget_field:          str                 = "presupuesto_sin_iva",
        procedure_type_field:  str                 = "tipo_procedimiento",
        subentidad:            Optional[str]       = None,
        cod_subentidad:        Optional[str]       = None,
        organo_id:             Optional[str]       = None,
        topic_model:           Optional[str]       = None,
        topic_id:              Optional[str]       = None,
        topic_min_weight:      Optional[float]     = None,
        extra_fq:              Optional[List[str]] = None,
    ) -> List[dict]:
        """
        Customize Q43 – Direct awards.

        Returns one Solr query dict per bimester fetching only the
        procedure-type field. The direct-award ratio is computed in Python
        (do_Q43) by checking whether tipo_procedimiento equals
        "Negociado sin publicidad".

        Returns
        -------
        List of dicts with keys: label, q, fq, fl, rows, _meta
        """
        ranges  = _bimester_ranges(date_start, date_end)
        base_fq = _build_common_fq(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )

        return [
            {
                "label": r["label"],
                "q":     "*:*",
                "fq":    _bimester_fq(base_fq, date_field, r),
                "fl":    procedure_type_field,
                "rows":  "1000000",
                "_meta": {"range": r},
            }
            for r in ranges
        ]

    # =========================================================================
    # Q44 – TED publication
    # =========================================================================
    def customize_Q44(
        self,
        date_field:       str                 = "updated",
        date_start:       str                 = "2025-01-01T00:00:00Z",
        date_end:         str                 = "2026-01-01T00:00:00Z",
        tender_type:      Optional[str]       = None,
        cpv_prefixes:     Optional[List[str]] = None,
        cpv_field:        str                 = "cpv_list",
        budget_min:       Optional[float]     = None,
        budget_max:       Optional[float]     = None,
        budget_field:     str                 = "presupuesto_sin_iva",
        ted_field:        str                 = "TED_id",
        subentidad:       Optional[str]       = None,
        cod_subentidad:   Optional[str]       = None,
        organo_id:        Optional[str]       = None,
        topic_model:      Optional[str]       = None,
        topic_id:         Optional[str]       = None,
        topic_min_weight: Optional[float]     = None,
        extra_fq:         Optional[List[str]] = None,
    ) -> List[dict]:
        """
        Customize Q44 – TED publication.

        Returns one Solr query dict per bimester fetching only the TED
        identifier field. The publication ratio is computed in Python
        (do_Q44) by checking whether the field is present and non-empty.

        Returns
        -------
        List of dicts with keys: label, q, fq, fl, rows, _meta
        """
        ranges  = _bimester_ranges(date_start, date_end)
        base_fq = _build_common_fq(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )

        return [
            {
                "label": r["label"],
                "q":     "*:*",
                "fq":    _bimester_fq(base_fq, date_field, r),
                "fl":    ted_field,
                "rows":  "1000000",
                "_meta": {"range": r},
            }
            for r in ranges
        ]

    # =========================================================================
    # Q45 – SME participation (% lots with at least one SME offer)
    # =========================================================================
    def customize_Q45(
        self,
        date_field:       str                 = "updated",
        date_start:       str                 = "2025-01-01T00:00:00Z",
        date_end:         str                 = "2026-01-01T00:00:00Z",
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
    ) -> List[dict]:
        """
        Customize Q45 – SME participation.

        Returns one Solr query dict per bimester fetching only the
        ofertas_pymes field. The SME participation ratio is computed in
        Python (do_Q45).

        Returns
        -------
        List of dicts with keys: label, q, fq, fl, rows, _meta
        """
        ranges  = _bimester_ranges(date_start, date_end)
        base_fq = _build_common_fq(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )

        return [
            {
                "label": r["label"],
                "q":     "*:*",
                "fq":    _bimester_fq(base_fq, date_field, r),
                "fl":    sme_field,
                "rows":  "1000000",
                "_meta": {"range": r},
            }
            for r in ranges
        ]

    # =========================================================================
    # Q46 – SME offer ratio (sum(pyme_offers) / sum(total_offers))
    # =========================================================================
    def customize_Q46(
        self,
        date_field:       str                 = "updated",
        date_start:       str                 = "2025-01-01T00:00:00Z",
        date_end:         str                 = "2026-01-01T00:00:00Z",
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
    ) -> List[dict]:
        """
        Customize Q46 – SME offer ratio.

        Returns one Solr query dict per bimester fetching both the total
        offers and SME offers fields. The ratio is computed in Python
        (do_Q46) as sum(sme_offers) / sum(total_offers).

        Returns
        -------
        List of dicts with keys: label, q, fq, fl, rows, _meta
        """
        ranges  = _bimester_ranges(date_start, date_end)
        base_fq = _build_common_fq(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )

        return [
            {
                "label": r["label"],
                "q":     "*:*",
                "fq":    _bimester_fq(base_fq, date_field, r),
                "fl":    f"{offers_field},{sme_field}",
                "rows":  "1000000",
                "_meta": {"range": r},
            }
            for r in ranges
        ]

    # =========================================================================
    # Q47 – Lots division (% procedures with more than one lot)
    # =========================================================================
    def customize_Q47(
        self,
        date_field:       str                 = "updated",
        date_start:       str                 = "2025-01-01T00:00:00Z",
        date_end:         str                 = "2026-01-01T00:00:00Z",
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
    ) -> List[dict]:
        """
        Customize Q47 – Lots division.

        Returns one Solr query dict per bimester fetching only the lots
        field. The percentage of multi-lot procedures is computed in
        Python (do_Q47).

        Returns
        -------
        List of dicts with keys: label, q, fq, fl, rows, _meta
        """
        ranges  = _bimester_ranges(date_start, date_end)
        base_fq = _build_common_fq(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )

        return [
            {
                "label": r["label"],
                "q":     "*:*",
                "fq":    _bimester_fq(base_fq, date_field, r),
                "fl":    lots_field,
                "rows":  "1000000",
                "_meta": {"range": r},
            }
            for r in ranges
        ]

    # =========================================================================
    # Q48 – Missing supplier ID (% awarded lots without supplier identifier)
    # =========================================================================
    def customize_Q48(
        self,
        date_field:        str                 = "updated",
        date_start:        str                 = "2025-01-01T00:00:00Z",
        date_end:          str                 = "2026-01-01T00:00:00Z",
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
    ) -> List[dict]:
        """
        Customize Q48 – Missing supplier ID.

        Returns one Solr query dict per bimester fetching only the
        identificador field. The percentage of lots without a valid
        supplier identifier is computed in Python (do_Q48).

        Returns
        -------
        List of dicts with keys: label, q, fq, fl, rows, _meta
        """
        ranges  = _bimester_ranges(date_start, date_end)
        base_fq = _build_common_fq(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )

        return [
            {
                "label": r["label"],
                "q":     "*:*",
                "fq":    _bimester_fq(base_fq, date_field, r),
                "fl":    identifier_field,
                "rows":  "1000000",
                "_meta": {"range": r},
            }
            for r in ranges
        ]

    # =========================================================================
    # Q49 – Missing buyer ID (% procedures without contracting authority ID)
    # =========================================================================
    def customize_Q49(
        self,
        date_field:       str                 = "updated",
        date_start:       str                 = "2025-01-01T00:00:00Z",
        date_end:         str                 = "2026-01-01T00:00:00Z",
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
    ) -> List[dict]:
        """
        Customize Q49 – Missing buyer ID.

        Returns one Solr query dict per bimester fetching only the
        organo_id field. The percentage of procedures without a buyer
        identifier is computed in Python (do_Q49).

        Returns
        -------
        List of dicts with keys: label, q, fq, fl, rows, _meta
        """
        ranges  = _bimester_ranges(date_start, date_end)
        base_fq = _build_common_fq(
            date_field=date_field, date_start=date_start, date_end=date_end,
            tender_type=tender_type,
            cpv_prefixes=cpv_prefixes, cpv_field=cpv_field,
            budget_min=budget_min, budget_max=budget_max, budget_field=budget_field,
            subentidad=subentidad, cod_subentidad=cod_subentidad,
            organo_id=organo_id, topic_model=topic_model,
            topic_id=topic_id, topic_min_weight=topic_min_weight,
            extra_fq=extra_fq,
        )

        return [
            {
                "label": r["label"],
                "q":     "*:*",
                "fq":    _bimester_fq(base_fq, date_field, r),
                "fl":    buyer_id_field,
                "rows":  "1000000",
                "_meta": {"range": r},
            }
            for r in ranges
        ]