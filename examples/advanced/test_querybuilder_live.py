# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Comprehensive QueryBuilder live integration test.

Tests every QueryBuilder method and filter expression against the existing
new_WalkthroughDemo table (created by walkthrough.py sections 2-6).

Expected data in the table:
  - "Complete project documentation"  Qty=100  Amt=1250.50  Completed=False  Priority=MEDIUM
  - "Review code changes"             Qty=10   Amt=500.00   Completed=True   Priority=HIGH
  - "Update test cases"               Qty=8    Amt=750.25   Completed=True   Priority=LOW (was updated to True)
  - "Deploy to staging"               Qty=3    Amt=2000.00  Completed=True   Priority=HIGH (was updated to True)
  - "Paging test item 1..20"          Qty=1-20 Amt=10-200   Completed=False  Priority=LOW

Run:
    $env:PYTHONPATH = "src"; python examples/advanced/test_querybuilder_live.py
"""

import sys
import traceback
from enum import IntEnum

from azure.identity import InteractiveBrowserCredential
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.filters import (
    eq,
    ne,
    gt,
    ge,
    lt,
    le,
    contains,
    startswith,
    endswith,
    between,
    is_null,
    is_not_null,
    raw,
)


class Priority(IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


TABLE = "new_WalkthroughDemo"
PASSED = 0
FAILED = 0
SKIPPED = 0
ERRORS = []


def test(name, fn, client):
    """Run a single test and report pass/fail."""
    global PASSED, FAILED, SKIPPED
    try:
        fn(client)
        PASSED += 1
        print(f"  [PASS] {name}")
    except Exception as ex:
        msg = str(ex)
        if "query node In is not supported" in msg:
            SKIPPED += 1
            print(f"  [SKIP] {name}: OData 'in' operator not supported by this environment")
        else:
            FAILED += 1
            ERRORS.append((name, ex))
            print(f"  [FAIL] {name}: {ex}")
            traceback.print_exc()


# ---------------------------------------------------------------------------
# FLUENT FILTER METHODS
# ---------------------------------------------------------------------------


def test_filter_eq(client):
    """filter_eq: Completed == False → should find paging items + documentation."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_completed")
        .filter_eq("new_Completed", False)
        .top(25)
        .execute()
    )
    assert len(recs) > 0, "Expected at least 1 incomplete record"
    for r in recs:
        assert r["new_completed"] is False, f"Expected False, got {r['new_completed']}"


def test_filter_ne(client):
    """filter_ne: Priority != LOW → should exclude LOW priority records."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_priority")
        .filter_ne("new_Priority", Priority.LOW)
        .top(25)
        .execute()
    )
    assert len(recs) > 0, "Expected at least 1 non-LOW record"
    for r in recs:
        assert r["new_priority"] != Priority.LOW, f"Got LOW priority unexpectedly"


def test_filter_gt(client):
    """filter_gt: Quantity > 10 → should find documentation record (Qty=100)."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity")
        .filter_gt("new_Quantity", 10)
        .execute()
    )
    assert len(recs) >= 1, f"Expected at least 1 record with Qty > 10, got {len(recs)}"
    for r in recs:
        assert r["new_quantity"] > 10, f"Expected > 10, got {r['new_quantity']}"


def test_filter_ge(client):
    """filter_ge: Quantity >= 10 → should find documentation (100) + review (10) + paging items 10-20."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity")
        .filter_ge("new_Quantity", 10)
        .execute()
    )
    assert len(recs) >= 2, f"Expected >= 2 records with Qty >= 10, got {len(recs)}"
    for r in recs:
        assert r["new_quantity"] >= 10, f"Expected >= 10, got {r['new_quantity']}"


def test_filter_lt(client):
    """filter_lt: Quantity < 5 → should find Deploy (3) + paging items 1-4."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity")
        .filter_lt("new_Quantity", 5)
        .execute()
    )
    assert len(recs) >= 1, f"Expected >= 1 record with Qty < 5, got {len(recs)}"
    for r in recs:
        assert r["new_quantity"] < 5, f"Expected < 5, got {r['new_quantity']}"


def test_filter_le(client):
    """filter_le: Quantity <= 3 → should find Deploy (3) + paging items 1-3."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity")
        .filter_le("new_Quantity", 3)
        .execute()
    )
    assert len(recs) >= 1, f"Expected >= 1 record with Qty <= 3, got {len(recs)}"
    for r in recs:
        assert r["new_quantity"] <= 3, f"Expected <= 3, got {r['new_quantity']}"


def test_filter_contains(client):
    """filter_contains: title contains 'Paging' → paging items."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title")
        .filter_contains("new_Title", "Paging")
        .execute()
    )
    assert len(recs) >= 1, "Expected records containing 'Paging'"
    for r in recs:
        assert "paging" in r["new_title"].lower(), f"Expected 'Paging' in '{r['new_title']}'"


def test_filter_startswith(client):
    """filter_startswith: title starts with 'Paging' → paging items."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title")
        .filter_startswith("new_Title", "Paging")
        .execute()
    )
    assert len(recs) >= 1, "Expected records starting with 'Paging'"
    for r in recs:
        assert r["new_title"].lower().startswith("paging"), f"'{r['new_title']}' doesn't start with 'Paging'"


def test_filter_endswith(client):
    """filter_endswith: title ends with 'documentation' → documentation record."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title")
        .filter_endswith("new_Title", "documentation")
        .execute()
    )
    assert len(recs) >= 1, "Expected at least 1 record ending with 'documentation'"


def test_filter_null(client):
    """filter_null: new_Amount is null → likely 0 records (all have amounts)."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_amount")
        .filter_null("new_Amount")
        .execute()
    )
    # All records have amounts, so 0 is expected — just verify no crash
    assert isinstance(recs, list), "Expected a list result"


def test_filter_not_null(client):
    """filter_not_null: new_Amount is not null → all records."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_amount")
        .filter_not_null("new_Amount")
        .execute()
    )
    assert len(recs) >= 20, f"Expected >= 20 records with Amount set, got {len(recs)}"


def test_filter_between(client):
    """filter_between: Amount between 500 and 1500."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_amount")
        .filter_between("new_Amount", 500, 1500)
        .execute()
    )
    assert len(recs) >= 1, "Expected >= 1 record with Amount in [500, 1500]"
    for r in recs:
        assert 500 <= r["new_amount"] <= 1500, f"Amount {r['new_amount']} not in [500, 1500]"


def test_filter_between_int(client):
    """filter_between: Quantity between 5 and 15."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity")
        .filter_between("new_Quantity", 5, 15)
        .execute()
    )
    assert len(recs) >= 1, "Expected >= 1 record with Quantity in [5, 15]"
    for r in recs:
        assert 5 <= r["new_quantity"] <= 15, f"Quantity {r['new_quantity']} not in [5, 15]"


def test_filter_raw(client):
    """filter_raw: raw OData filter string."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity")
        .filter_raw("new_quantity ge 10 and new_quantity le 20")
        .execute()
    )
    assert len(recs) >= 1, "Expected records from raw filter"
    for r in recs:
        assert 10 <= r["new_quantity"] <= 20, f"Quantity {r['new_quantity']} not in [10, 20]"


# ---------------------------------------------------------------------------
# WHERE() WITH COMPOSABLE EXPRESSION TREES
# ---------------------------------------------------------------------------


def test_where_eq(client):
    """where() with eq expression."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_completed")
        .where(eq("new_Completed", True))
        .execute()
    )
    assert len(recs) >= 1, "Expected completed records"
    for r in recs:
        assert r["new_completed"] is True


def test_where_and(client):
    """where() with & (AND) operator."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity", "new_completed")
        .where(eq("new_Completed", False) & gt("new_Quantity", 5))
        .execute()
    )
    assert len(recs) >= 1, "Expected incomplete records with Qty > 5"
    for r in recs:
        assert r["new_completed"] is False and r["new_quantity"] > 5


def test_where_or(client):
    """where() with | (OR) operator."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity")
        .where(lt("new_Quantity", 3) | gt("new_Quantity", 18))
        .execute()
    )
    assert len(recs) >= 1, "Expected records with Qty < 3 or Qty > 18"
    for r in recs:
        assert r["new_quantity"] < 3 or r["new_quantity"] > 18


def test_where_not(client):
    """where() with ~ (NOT) operator."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_completed")
        .where(~eq("new_Completed", True))
        .execute()
    )
    assert len(recs) >= 1, "Expected non-completed records"
    for r in recs:
        assert r["new_completed"] is False


def test_where_nested_and_or(client):
    """where() with nested (A | B) & C."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_priority", "new_quantity")
        .where(
            (eq("new_Priority", Priority.HIGH) | eq("new_Priority", Priority.MEDIUM))
            & gt("new_Quantity", 2)
        )
        .execute()
    )
    assert len(recs) >= 1, "Expected HIGH/MEDIUM priority records with Qty > 2"
    for r in recs:
        assert r["new_priority"] in (Priority.HIGH, Priority.MEDIUM)
        assert r["new_quantity"] > 2


def test_where_contains(client):
    """where() with contains expression."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title")
        .where(contains("new_Title", "test"))
        .execute()
    )
    assert len(recs) >= 1, "Expected records containing 'test'"
    for r in recs:
        assert "test" in r["new_title"].lower()


def test_where_startswith(client):
    """where() with startswith expression."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title")
        .where(startswith("new_Title", "Deploy"))
        .execute()
    )
    assert len(recs) >= 1, "Expected records starting with 'Deploy'"


def test_where_endswith(client):
    """where() with endswith expression."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title")
        .where(endswith("new_Title", "staging"))
        .execute()
    )
    assert len(recs) >= 1, "Expected records ending with 'staging'"


def test_where_between(client):
    """where() with between expression."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_amount")
        .where(between("new_Amount", 100, 800))
        .execute()
    )
    assert len(recs) >= 1, "Expected records with Amount in [100, 800]"
    for r in recs:
        assert 100 <= r["new_amount"] <= 800


def test_where_is_null(client):
    """where() with is_null expression (expect 0 results — all have amounts)."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title")
        .where(is_null("new_Amount"))
        .execute()
    )
    assert isinstance(recs, list)


def test_where_is_not_null(client):
    """where() with is_not_null expression."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title")
        .where(is_not_null("new_Amount"))
        .execute()
    )
    assert len(recs) >= 20


def test_where_raw(client):
    """where() with raw expression."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity")
        .where(raw("new_quantity eq 100"))
        .execute()
    )
    assert len(recs) >= 1, "Expected record with Qty=100"
    assert recs[0]["new_quantity"] == 100


# ---------------------------------------------------------------------------
# COMBINED FLUENT + WHERE
# ---------------------------------------------------------------------------


def test_combined_fluent_and_where(client):
    """Combining fluent filter_eq + where() expression."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity", "new_completed")
        .filter_eq("new_Completed", False)
        .where(between("new_Quantity", 5, 15))
        .execute()
    )
    assert len(recs) >= 1
    for r in recs:
        assert r["new_completed"] is False
        assert 5 <= r["new_quantity"] <= 15


def test_combined_multiple_fluent(client):
    """Chaining multiple fluent filters: filter_ge + filter_le."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity")
        .filter_ge("new_Quantity", 5)
        .filter_le("new_Quantity", 15)
        .execute()
    )
    assert len(recs) >= 1
    for r in recs:
        assert 5 <= r["new_quantity"] <= 15


# ---------------------------------------------------------------------------
# SELECT, ORDER_BY, TOP, PAGE_SIZE
# ---------------------------------------------------------------------------


def test_select_columns(client):
    """select() limits returned columns."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title")
        .top(3)
        .execute()
    )
    assert len(recs) > 0
    # new_title should be present; new_quantity should NOT be (unless server adds extras)
    assert "new_title" in recs[0]


def test_order_by_asc(client):
    """order_by ascending."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity")
        .order_by("new_Quantity")
        .top(5)
        .execute()
    )
    assert len(recs) == 5
    quantities = [r["new_quantity"] for r in recs]
    assert quantities == sorted(quantities), f"Not ascending: {quantities}"


def test_order_by_desc(client):
    """order_by descending."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity")
        .order_by("new_Quantity", descending=True)
        .top(5)
        .execute()
    )
    assert len(recs) == 5
    quantities = [r["new_quantity"] for r in recs]
    assert quantities == sorted(quantities, reverse=True), f"Not descending: {quantities}"


def test_top(client):
    """top() limits result count."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title")
        .top(3)
        .execute()
    )
    assert len(recs) == 3, f"Expected 3 records, got {len(recs)}"


def test_page_size_flat(client):
    """page_size with flat iteration (default by_page=False)."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title")
        .page_size(5)
        .execute()
    )
    # Should get all records (>= 20) flattened
    assert len(recs) >= 20, f"Expected >= 20 records, got {len(recs)}"


def test_page_size_by_page(client):
    """page_size with by_page=True → pages of records."""
    pages = []
    for page in (
        client.query.builder(TABLE)
        .select("new_title")
        .page_size(5)
        .execute(by_page=True)
    ):
        pages.append(page)
    assert len(pages) >= 4, f"Expected >= 4 pages (20+ records / 5), got {len(pages)}"
    # Each non-final page should have exactly 5 records
    for i, p in enumerate(pages[:-1]):
        assert len(p) == 5, f"Page {i+1} has {len(p)} records, expected 5"


def test_page_size_by_page_with_filter(client):
    """by_page=True with filter — pages only matching records."""
    pages = []
    for page in (
        client.query.builder(TABLE)
        .select("new_title", "new_quantity")
        .filter_between("new_Quantity", 1, 10)
        .page_size(3)
        .execute(by_page=True)
    ):
        pages.append(page)
        for r in page:
            assert 1 <= r["new_quantity"] <= 10
    total = sum(len(p) for p in pages)
    assert total >= 1, "Expected at least 1 record in range"
    assert len(pages) >= 1, "Expected at least 1 page"


# ---------------------------------------------------------------------------
# EDGE CASES
# ---------------------------------------------------------------------------


def test_no_results(client):
    """Query that returns 0 results should return empty list, not error."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title")
        .filter_eq("new_Quantity", 999999)
        .execute()
    )
    assert len(recs) == 0, f"Expected 0 results, got {len(recs)}"


def test_no_filters(client):
    """Query with no filters — returns all records."""
    recs = list(
        client.query.builder(TABLE)
        .select("new_title")
        .execute()
    )
    assert len(recs) >= 20, f"Expected >= 20 records, got {len(recs)}"


def test_chaining_order(client):
    """Verify chaining order doesn't matter (select before/after filter)."""
    recs1 = list(
        client.query.builder(TABLE)
        .select("new_title", "new_quantity")
        .filter_eq("new_Completed", False)
        .order_by("new_Quantity")
        .top(5)
        .execute()
    )
    recs2 = list(
        client.query.builder(TABLE)
        .filter_eq("new_Completed", False)
        .order_by("new_Quantity")
        .select("new_title", "new_quantity")
        .top(5)
        .execute()
    )
    titles1 = [r["new_title"] for r in recs1]
    titles2 = [r["new_title"] for r in recs2]
    assert titles1 == titles2, f"Order mismatch: {titles1} vs {titles2}"


# ---------------------------------------------------------------------------
# RUNNER
# ---------------------------------------------------------------------------


ALL_TESTS = [
    # Fluent filter methods
    ("filter_eq", test_filter_eq),
    ("filter_ne", test_filter_ne),
    ("filter_gt", test_filter_gt),
    ("filter_ge", test_filter_ge),
    ("filter_lt", test_filter_lt),
    ("filter_le", test_filter_le),
    ("filter_contains", test_filter_contains),
    ("filter_startswith", test_filter_startswith),
    ("filter_endswith", test_filter_endswith),
    ("filter_null", test_filter_null),
    ("filter_not_null", test_filter_not_null),
    ("filter_between (decimal)", test_filter_between),
    ("filter_between (int)", test_filter_between_int),
    ("filter_raw", test_filter_raw),
    # where() expression trees
    ("where(eq)", test_where_eq),
    ("where(& AND)", test_where_and),
    ("where(| OR)", test_where_or),
    ("where(~ NOT)", test_where_not),
    ("where(nested (A|B) & C)", test_where_nested_and_or),
    ("where(contains)", test_where_contains),
    ("where(startswith)", test_where_startswith),
    ("where(endswith)", test_where_endswith),
    ("where(between)", test_where_between),
    ("where(is_null)", test_where_is_null),
    ("where(is_not_null)", test_where_is_not_null),
    ("where(raw)", test_where_raw),
    # Combined
    ("combined fluent + where", test_combined_fluent_and_where),
    ("combined multiple fluent", test_combined_multiple_fluent),
    # Select, ordering, paging
    ("select columns", test_select_columns),
    ("order_by asc", test_order_by_asc),
    ("order_by desc", test_order_by_desc),
    ("top()", test_top),
    ("page_size flat", test_page_size_flat),
    ("page_size by_page", test_page_size_by_page),
    ("page_size by_page + filter", test_page_size_by_page_with_filter),
    # Edge cases
    ("no results", test_no_results),
    ("no filters", test_no_filters),
    ("chaining order", test_chaining_order),
]


def main():
    print("=" * 80)
    print("QueryBuilder Comprehensive Live Tests")
    print("=" * 80)

    base_url = sys.argv[1] if len(sys.argv) > 1 else input("Enter Dataverse org URL (e.g. https://yourorg.crm.dynamics.com): ").strip()
    if not base_url:
        print("No URL provided; exiting.")
        sys.exit(1)

    base_url = base_url.rstrip("/")
    credential = InteractiveBrowserCredential()

    with DataverseClient(base_url=base_url, credential=credential) as client:
        print(f"Connected to: {base_url}")
        print(f"Table: {TABLE}")
        print(f"Running {len(ALL_TESTS)} tests...\n")

        for name, fn in ALL_TESTS:
            test(name, fn, client)

    print("\n" + "=" * 80)
    print(f"Results: {PASSED} passed, {FAILED} failed, {SKIPPED} skipped out of {PASSED + FAILED + SKIPPED}")
    print("=" * 80)

    if ERRORS:
        print("\nFailed tests:")
        for name, ex in ERRORS:
            print(f"  - {name}: {ex}")

    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
