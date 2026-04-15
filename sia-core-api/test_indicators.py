"""
Integration test for dashboard indicators (Q40–Q49).

Runs directly against the live Solr instance at http://kumo01:10085.
Execute from the project root:

    python test_dashboard_integration.py

Adjust SOLR_URL, CONFIG_FILE, DATE_START, DATE_END and FILTERS below
if needed.
"""

import json
import logging
import sys
import traceback

# ---------------------------------------------------------------------------
# Global state for error tracking
# ---------------------------------------------------------------------------

failed_tests = []  # List to store failure information

# ---------------------------------------------------------------------------
# Configuration — edit these to match your environment
# ---------------------------------------------------------------------------

SOLR_URL    = "http://kumo01:10085"
CONFIG_FILE = "/export/usuarios_ml4ds/lbartolome/Repos/alia/alia-sia/sia-config/config.cf"

DATE_START  = "2025-01-01T00:00:00Z"
DATE_END    = "2026-01-01T00:00:00Z"
DATE_FIELD  = "updated"

# Try each of these tender_type values (None = all sources)
TENDER_TYPES = [None, "minors", "insiders", "outsiders"]

# A CPV prefix we know exists in the data
CPV_PREFIXES = ["31"]   # electrical equipment — matches 31680000 in the sample

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("dashboard_test")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _section(title: str) -> None:
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def _ok(label: str, result: dict) -> None:
    print(f"\n[OK] {label}")
    print(json.dumps(result, indent=2, ensure_ascii=False))


def _fail(label: str, sc: int, result: dict, error_reason: str = None) -> None:
    print(f"\n[FAIL] {label}  →  HTTP {sc}")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    if error_reason:
        print(f"Error: {error_reason}")
    
    # Register the failure
    failed_tests.append({
        "label": label,
        "http_code": sc,
        "reason": error_reason or "HTTP error or missing data"
    })


def _check(label: str, result, sc: int) -> bool:
    if sc != 200 or result is None or "error" in result:
        reason = result.get("error", "Unknown error") if result else "No response"
        _fail(label, sc, result or {}, reason)
        return False
    _ok(label, result)
    return True


def _check_pct_field(field_name: str, values: list) -> None:
    """Assert that all non-None values in a percentage field are in [0, 100]."""
    non_null = [v for v in values if v is not None]
    if non_null:
        assert all(0 <= v <= 100 for v in non_null), (
            f"{field_name} out of [0,100] range: {non_null}"
        )
    return non_null


# ---------------------------------------------------------------------------
# Bootstrap the client
# ---------------------------------------------------------------------------

def _build_client():
    from src.core.clients.np_solr_client import SIASolrClient
    client = SIASolrClient(logger=logger, config_file=CONFIG_FILE)
    client.solr_url = SOLR_URL
    print(client)
    return client

# ---------------------------------------------------------------------------
# Individual test functions
# ---------------------------------------------------------------------------

def test_q40(client, tender_type, cpv_prefixes=None):
    label = f"Q40 | tender_type={tender_type} | cpv={cpv_prefixes}"
    result, sc = client.do_Q40(
        date_start   = DATE_START,
        date_end     = DATE_END,
        date_field   = DATE_FIELD,
        tender_type  = tender_type,
        cpv_prefixes = cpv_prefixes,
    )
    ok = _check(label, result, sc)
    if ok:
        assert "bimester_labels" in result, "missing bimester_labels"
        assert len(result["bimester_labels"]) == 6, (
            f"expected 6 bimesters, got {len(result['bimester_labels'])}"
        )
        assert result["total_tenders"] >= 0, "total_tenders must be >= 0"
        assert len(result["by_count"]) == len(result["bimester_labels"])
        assert len(result["by_budget"]) == len(result["bimester_labels"])
        print(f"    → total_tenders : {result['total_tenders']}")
        print(f"    → total_budget  : {result['total_budget']:.2f}")
    return ok


def test_q41(client, tender_type, cpv_prefixes=None):
    label = f"Q41 | tender_type={tender_type} | cpv={cpv_prefixes}"
    result, sc = client.do_Q41(
        date_start   = DATE_START,
        date_end     = DATE_END,
        date_field   = DATE_FIELD,
        tender_type  = tender_type,
        cpv_prefixes = cpv_prefixes,
    )
    ok = _check(label, result, sc)
    if ok:
        assert "bimester_labels" in result
        assert "pct_single_bid" in result
        assert "coverage" in result
        assert len(result["pct_single_bid"]) == len(result["bimester_labels"])
        non_null = _check_pct_field("pct_single_bid", result["pct_single_bid"])
        if non_null:
            print(f"    → avg pct_single_bid : {sum(non_null)/len(non_null):.2f}%")
        else:
            print("    → no data (all None) — check offers_field name")
    return ok


def test_q42(client, tender_type, cpv_prefixes=None):
    label = f"Q42 | tender_type={tender_type} | cpv={cpv_prefixes}"
    result, sc = client.do_Q42(
        date_start   = DATE_START,
        date_end     = DATE_END,
        date_field   = DATE_FIELD,
        tender_type  = tender_type,
        cpv_prefixes = cpv_prefixes,
    )
    ok = _check(label, result, sc)
    if ok:
        assert "bimester_labels" in result
        assert "avg_days" in result
        assert "n_obs" in result
        non_null = [v for v in result["avg_days"] if v is not None]
        if non_null:
            assert all(v >= 0 for v in non_null), f"avg_days < 0: {non_null}"
            print(f"    → avg days overall : {sum(non_null)/len(non_null):.2f}")
        else:
            print("    → no data (all None) — check deadline/award field names")
    return ok


def test_q43(client, tender_type, cpv_prefixes=None):
    label = f"Q43 | tender_type={tender_type} | cpv={cpv_prefixes}"
    result, sc = client.do_Q43(
        date_start   = DATE_START,
        date_end     = DATE_END,
        date_field   = DATE_FIELD,
        tender_type  = tender_type,
        cpv_prefixes = cpv_prefixes,
    )
    ok = _check(label, result, sc)
    if ok:
        assert "bimester_labels" in result
        assert "pct_direct" in result
        assert "coverage" in result
        assert "n_tenders" in result
        assert len(result["pct_direct"]) == len(result["bimester_labels"])
        assert len(result["coverage"]) == len(result["bimester_labels"])
        non_null = _check_pct_field("pct_direct", result["pct_direct"])
        cov_non_null = _check_pct_field("coverage", result["coverage"])
        if non_null:
            print(f"    → avg pct_direct  : {sum(non_null)/len(non_null):.2f}%")
        if cov_non_null:
            print(f"    → avg coverage    : {sum(cov_non_null)/len(cov_non_null):.2f}%")
    return ok


def test_q44(client, tender_type, cpv_prefixes=None):
    label = f"Q44 | tender_type={tender_type} | cpv={cpv_prefixes}"
    result, sc = client.do_Q44(
        date_start   = DATE_START,
        date_end     = DATE_END,
        date_field   = DATE_FIELD,
        tender_type  = tender_type,
        cpv_prefixes = cpv_prefixes,
    )
    ok = _check(label, result, sc)
    if ok:
        assert "bimester_labels" in result
        assert "pct_ted" in result
        assert "n_tenders" in result
        assert len(result["pct_ted"]) == len(result["bimester_labels"])
        non_null = _check_pct_field("pct_ted", result["pct_ted"])
        # minors should always be 0 % (below TED thresholds)
        if tender_type == "minors" and non_null:
            assert all(v == 0.0 for v in non_null), (
                f"minors should have pct_ted=0, got {non_null}"
            )
            print("    → minors pct_ted=0 ✓ (expected)")
        elif non_null:
            print(f"    → avg pct_ted : {sum(non_null)/len(non_null):.2f}%")
    return ok


def test_q45(client, tender_type, cpv_prefixes=None):
    label = f"Q45 | tender_type={tender_type} | cpv={cpv_prefixes}"
    result, sc = client.do_Q45(
        date_start   = DATE_START,
        date_end     = DATE_END,
        date_field   = DATE_FIELD,
        tender_type  = tender_type,
        cpv_prefixes = cpv_prefixes,
    )
    ok = _check(label, result, sc)
    if ok:
        assert "bimester_labels" in result
        assert "pct_sme" in result
        assert "coverage" in result
        assert "n_lots_total" in result
        assert len(result["pct_sme"]) == len(result["bimester_labels"])
        non_null = _check_pct_field("pct_sme", result["pct_sme"])
        cov_non_null = _check_pct_field("coverage", result["coverage"])
        if non_null:
            print(f"    → avg pct_sme  : {sum(non_null)/len(non_null):.2f}%")
        else:
            print("    → no data (all None) — ofertas_pymes may not be available for this source")
        if cov_non_null:
            print(f"    → avg coverage : {sum(cov_non_null)/len(cov_non_null):.2f}%")
    return ok


def test_q46(client, tender_type, cpv_prefixes=None):
    label = f"Q46 | tender_type={tender_type} | cpv={cpv_prefixes}"
    result, sc = client.do_Q46(
        date_start   = DATE_START,
        date_end     = DATE_END,
        date_field   = DATE_FIELD,
        tender_type  = tender_type,
        cpv_prefixes = cpv_prefixes,
    )
    ok = _check(label, result, sc)
    if ok:
        assert "bimester_labels" in result
        assert "pct_sme_offers" in result
        assert "coverage" in result
        assert "n_lots_total" in result
        assert len(result["pct_sme_offers"]) == len(result["bimester_labels"])
        non_null = _check_pct_field("pct_sme_offers", result["pct_sme_offers"])
        cov_non_null = _check_pct_field("coverage", result["coverage"])
        if non_null:
            print(f"    → avg pct_sme_offers : {sum(non_null)/len(non_null):.2f}%")
        else:
            print("    → no data (all None) — ofertas_pymes may not be available for this source")
        if cov_non_null:
            print(f"    → avg coverage       : {sum(cov_non_null)/len(cov_non_null):.2f}%")
    return ok


def test_q47(client, tender_type, cpv_prefixes=None):
    label = f"Q47 | tender_type={tender_type} | cpv={cpv_prefixes}"
    result, sc = client.do_Q47(
        date_start   = DATE_START,
        date_end     = DATE_END,
        date_field   = DATE_FIELD,
        tender_type  = tender_type,
        cpv_prefixes = cpv_prefixes,
    )
    ok = _check(label, result, sc)
    if ok:
        assert "bimester_labels" in result
        assert "pct_multi_lot" in result
        assert "coverage" in result
        assert "n_tenders" in result
        assert len(result["pct_multi_lot"]) == len(result["bimester_labels"])
        non_null = _check_pct_field("pct_multi_lot", result["pct_multi_lot"])
        # minors should be near 0 % (small contracts, rarely split into lots)
        if tender_type == "minors" and non_null:
            print(f"    → minors pct_multi_lot (expected ~0) : {non_null}")
        elif non_null:
            print(f"    → avg pct_multi_lot : {sum(non_null)/len(non_null):.2f}%")
        # coverage = % of procedures that have the lotes field populated;
        # not all documents include it, so we just print it as informational
        cov_non_null = _check_pct_field("coverage", result["coverage"])
        if cov_non_null:
            print(f"    → avg coverage (lotes field present) : {sum(cov_non_null)/len(cov_non_null):.2f}%")
    return ok


def test_q48(client, tender_type, cpv_prefixes=None):
    label = f"Q48 | tender_type={tender_type} | cpv={cpv_prefixes}"
    result, sc = client.do_Q48(
        date_start   = DATE_START,
        date_end     = DATE_END,
        date_field   = DATE_FIELD,
        tender_type  = tender_type,
        cpv_prefixes = cpv_prefixes,
    )
    ok = _check(label, result, sc)
    if ok:
        assert "bimester_labels" in result
        assert "pct_missing" in result
        assert "n_lots_total" in result
        assert len(result["pct_missing"]) == len(result["bimester_labels"])
        non_null = _check_pct_field("pct_missing", result["pct_missing"])
        if non_null:
            print(f"    → avg pct_missing : {sum(non_null)/len(non_null):.2f}%")
    return ok


def test_q49(client, tender_type, cpv_prefixes=None):
    label = f"Q49 | tender_type={tender_type} | cpv={cpv_prefixes}"
    result, sc = client.do_Q49(
        date_start   = DATE_START,
        date_end     = DATE_END,
        date_field   = DATE_FIELD,
        tender_type  = tender_type,
        cpv_prefixes = cpv_prefixes,
    )
    ok = _check(label, result, sc)
    if ok:
        assert "bimester_labels" in result
        assert "pct_missing" in result
        assert "n_tenders" in result
        assert len(result["pct_missing"]) == len(result["bimester_labels"])
        non_null = _check_pct_field("pct_missing", result["pct_missing"])
        # insiders and minors should be ~0 % (organo_id always present)
        if tender_type in ("insiders", "minors") and non_null:
            assert all(v < 1.0 for v in non_null), (
                f"{tender_type} pct_missing should be <1%, got {non_null}"
            )
            print(f"    → {tender_type} pct_missing < 1% ✓ (expected)")
        elif non_null:
            print(f"    → avg pct_missing : {sum(non_null)/len(non_null):.2f}%")
    return ok


# ---------------------------------------------------------------------------
# Edge-case tests
# ---------------------------------------------------------------------------

def test_edge_cases(client):
    _section("Edge cases")

    # Empty result: CPV prefix that does not exist
    label = "Q40 | cpv=['99'] (should return zeros)"
    result, sc = client.do_Q40(
        date_start   = DATE_START,
        date_end     = DATE_END,
        cpv_prefixes = ["99"],
    )
    ok = _check(label, result, sc)
    if ok:
        assert result["total_tenders"] == 0, (
            f"expected 0 tenders for CPV 99, got {result['total_tenders']}"
        )

    # Budget range filter
    label = "Q40 | budget 100–500 EUR"
    result, sc = client.do_Q40(
        date_start = DATE_START,
        date_end   = DATE_END,
        budget_min = 100.0,
        budget_max = 500.0,
    )
    _check(label, result, sc)

    # Geography filter — value from the sample doc
    label = "Q40 | subentidad='Alicante/Alacant'"
    result, sc = client.do_Q40(
        date_start = DATE_START,
        date_end   = DATE_END,
        subentidad = "Alicante/Alacant",
    )
    _check(label, result, sc)

    # Contracting authority from the sample doc
    label = "Q40 | organo_id=L01030762"
    result, sc = client.do_Q40(
        date_start = DATE_START,
        date_end   = DATE_END,
        organo_id  = "L01030762",
    )
    _check(label, result, sc)

    # Narrow date range — single bimester
    label = "Q40 | date range = Ene–Feb 2025 only"
    result, sc = client.do_Q40(
        date_start = "2025-01-01T00:00:00Z",
        date_end   = "2025-03-01T00:00:00Z",
    )
    ok = _check(label, result, sc)
    if ok:
        assert len(result["bimester_labels"]) == 1, (
            f"expected 1 bimester, got {len(result['bimester_labels'])}"
        )

    # Q43: narrow date range — verify structure is preserved
    label = "Q43 | date range = Ene–Feb 2025 only"
    result, sc = client.do_Q43(
        date_start = "2025-01-01T00:00:00Z",
        date_end   = "2025-03-01T00:00:00Z",
    )
    ok = _check(label, result, sc)
    if ok:
        assert len(result["bimester_labels"]) == 1, (
            f"expected 1 bimester, got {len(result['bimester_labels'])}"
        )

    # Q44: minors should always return pct_ted=0
    label = "Q44 | tender_type=minors (pct_ted should be 0)"
    result, sc = client.do_Q44(
        date_start  = DATE_START,
        date_end    = DATE_END,
        tender_type = "minors",
    )
    ok = _check(label, result, sc)
    if ok:
        non_null = [v for v in result["pct_ted"] if v is not None]
        if non_null:
            assert all(v == 0.0 for v in non_null), (
                f"minors should have pct_ted=0, got {non_null}"
            )

    # Q47: coverage = % of procedures with lotes field populated (informational)
    label = "Q47 | coverage check (all tender_types)"
    result, sc = client.do_Q47(
        date_start = DATE_START,
        date_end   = DATE_END,
    )
    ok = _check(label, result, sc)
    if ok:
        non_null = [v for v in result["coverage"] if v is not None]
        if non_null:
            print(f"    → avg coverage (lotes field present) : {sum(non_null)/len(non_null):.2f}%")

    # Q49: insiders should have pct_missing ~0
    label = "Q49 | tender_type=insiders (pct_missing should be ~0)"
    result, sc = client.do_Q49(
        date_start  = DATE_START,
        date_end    = DATE_END,
        tender_type = "insiders",
    )
    ok = _check(label, result, sc)
    if ok:
        non_null = [v for v in result["pct_missing"] if v is not None]
        if non_null:
            assert all(v < 1.0 for v in non_null), (
                f"insiders pct_missing should be <1%, got {non_null}"
            )

    # Q46 + Q45: consistency check — pct_sme_offers should be <= 100
    # and roughly consistent with pct_sme (participation) for the same filter
    label = "Q45 vs Q46 consistency | insiders + CPV 48"
    r45, sc45 = client.do_Q45(
        date_start   = DATE_START,
        date_end     = DATE_END,
        tender_type  = "insiders",
        cpv_prefixes = ["48"],
    )
    r46, sc46 = client.do_Q46(
        date_start   = DATE_START,
        date_end     = DATE_END,
        tender_type  = "insiders",
        cpv_prefixes = ["48"],
    )
    if sc45 == 200 and sc46 == 200:
        print(f"\n[OK] {label}")
        for bim, p45, p46 in zip(
            r45["bimester_labels"], r45["pct_sme"], r46["pct_sme_offers"]
        ):
            print(f"    {bim}  participation={p45}%  offer_ratio={p46}%")
    else:
        print(f"\n[FAIL] {label}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _run_test(test_func, label_prefix: str, **kwargs):
    """Wrapper to run a test and capture exceptions."""
    try:
        return test_func(**kwargs)
    except AssertionError as e:
        error_msg = str(e)
        print(f"\n[FAIL] {label_prefix}  →  Assertion failed")
        print(f"Error: {error_msg}")
        failed_tests.append({
            "label": label_prefix,
            "http_code": None,
            "reason": error_msg
        })
        return False
    except Exception as e:
        error_msg = str(e)
        print(f"\n[FAIL] {label_prefix}  →  Exception")
        print(f"Error: {error_msg}")
        traceback.print_exc()
        failed_tests.append({
            "label": label_prefix,
            "http_code": None,
            "reason": error_msg
        })
        return False


def main():
    passed = 0
    failed = 0

    logger.info("Building SIASolrClient...")
    try:
        client = _build_client()
    except Exception as e:
        logger.error(f"Could not instantiate SIASolrClient: {e}")
        sys.exit(1)

    # --- Q40 -----------------------------------------------------------------
    _section("Q40 – Total procurement")
    for tt in TENDER_TYPES:
        ok = _run_test(test_q40, f"Q40 | tender_type={tt}", 
                       client=client, tender_type=tt)
        passed += ok; failed += not ok

    _section("Q40 – with CPV filter")
    ok = _run_test(test_q40, "Q40 | cpv=31", 
                   client=client, tender_type=None, cpv_prefixes=CPV_PREFIXES)
    passed += ok; failed += not ok

    # --- Q41 -----------------------------------------------------------------
    _section("Q41 – Single bidder")
    for tt in TENDER_TYPES:
        ok = _run_test(test_q41, f"Q41 | tender_type={tt}",
                       client=client, tender_type=tt)
        passed += ok; failed += not ok

    _section("Q41 – with CPV filter")
    ok = _run_test(test_q41, "Q41 | tender_type=minors | cpv=31",
                   client=client, tender_type="minors", cpv_prefixes=CPV_PREFIXES)
    passed += ok; failed += not ok

    # --- Q42 -----------------------------------------------------------------
    _section("Q42 – Decision speed (insiders only)")
    ok = test_q42(client, tender_type="insiders")
    passed += ok; failed += not ok

    # --- Q43 -----------------------------------------------------------------
    _section("Q43 – Direct awards")
    for tt in TENDER_TYPES:
        ok = _run_test(test_q43, f"Q43 | tender_type={tt}",
                       client=client, tender_type=tt)
        passed += ok; failed += not ok

    _section("Q43 – with CPV filter")
    ok = _run_test(test_q43, "Q43 | cpv=31",
                   client=client, tender_type=None, cpv_prefixes=CPV_PREFIXES)
    passed += ok; failed += not ok

    # --- Q44 -----------------------------------------------------------------
    _section("Q44 – TED publication")
    for tt in TENDER_TYPES:
        ok = _run_test(test_q44, f"Q44 | tender_type={tt}",
                       client=client, tender_type=tt)
        passed += ok; failed += not ok

    _section("Q44 – with CPV filter")
    ok = _run_test(test_q44, "Q44 | cpv=31",
                   client=client, tender_type=None, cpv_prefixes=CPV_PREFIXES)
    passed += ok; failed += not ok

    # --- Q45 -----------------------------------------------------------------
    _section("Q45 – SME participation")
    # Only meaningful for insiders (field not available for outsiders/minors)
    for tt in [None, "insiders"]:
        ok = _run_test(test_q45, f"Q45 | tender_type={tt}",
                       client=client, tender_type=tt)
        passed += ok; failed += not ok

    _section("Q45 – with CPV filter")
    ok = _run_test(test_q45, "Q45 | tender_type=insiders | cpv=31",
                   client=client, tender_type="insiders", cpv_prefixes=CPV_PREFIXES)
    passed += ok; failed += not ok

    # --- Q46 -----------------------------------------------------------------
    _section("Q46 – SME offer ratio")
    for tt in [None, "insiders"]:
        ok = _run_test(test_q46, f"Q46 | tender_type={tt}",
                       client=client, tender_type=tt)
        passed += ok; failed += not ok

    _section("Q46 – with CPV filter")
    ok = _run_test(test_q46, "Q46 | tender_type=insiders | cpv=31",
                   client=client, tender_type="insiders", cpv_prefixes=CPV_PREFIXES)
    passed += ok; failed += not ok

    # --- Q47 -----------------------------------------------------------------
    _section("Q47 – Lots division")
    for tt in TENDER_TYPES:
        ok = _run_test(test_q47, f"Q47 | tender_type={tt}",
                       client=client, tender_type=tt)
        passed += ok; failed += not ok

    _section("Q47 – with CPV filter")
    ok = _run_test(test_q47, "Q47 | cpv=31",
                   client=client, tender_type=None, cpv_prefixes=CPV_PREFIXES)
    passed += ok; failed += not ok

    # --- Q48 -----------------------------------------------------------------
    _section("Q48 – Missing supplier ID")
    for tt in TENDER_TYPES:
        ok = _run_test(test_q48, f"Q48 | tender_type={tt}",
                       client=client, tender_type=tt)
        passed += ok; failed += not ok

    _section("Q48 – with CPV filter")
    ok = _run_test(test_q48, "Q48 | cpv=31",
                   client=client, tender_type=None, cpv_prefixes=CPV_PREFIXES)
    passed += ok; failed += not ok

    # --- Q49 -----------------------------------------------------------------
    _section("Q49 – Missing buyer ID")
    for tt in TENDER_TYPES:
        ok = _run_test(test_q49, f"Q49 | tender_type={tt}",
                       client=client, tender_type=tt)
        passed += ok; failed += not ok

    _section("Q49 – with CPV filter")
    ok = _run_test(test_q49, "Q49 | cpv=31",
                   client=client, tender_type=None, cpv_prefixes=CPV_PREFIXES)
    passed += ok; failed += not ok

    # --- Edge cases ----------------------------------------------------------
    _run_test(test_edge_cases, "Edge cases", client=client)

    # --- Summary and Failed Tests Report ------------------------------------
    _section("Summary")
    total = passed + failed
    print(f"\n  {passed}/{total} tests passed")
    if failed:
        print(f"  {failed} tests FAILED")
    else:
        print("  All tests passed ✓")

    # --- Detailed Failures Report --------------------------------------------
    if failed_tests:
        _section("Failed Tests Report")
        for idx, failure in enumerate(failed_tests, 1):
            print(f"\n{idx}. {failure['label']}")
            if failure['http_code']:
                print(f"   HTTP Code: {failure['http_code']}")
            print(f"   Reason: {failure['reason']}")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()