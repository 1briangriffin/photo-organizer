"""
Unit tests for PipelineCandidates — the in-memory working-set wrapper
threaded through Phase B writers as an input filter.
"""
from photo_organizer.pipeline.candidates import PipelineCandidates


def test_empty_by_default():
    c = PipelineCandidates()
    assert len(c) == 0
    assert c.ids() == set()


def test_add_single_id():
    c = PipelineCandidates()
    c.add(7)
    assert len(c) == 1
    assert 7 in c
    assert c.ids() == {7}


def test_add_many_accepts_iterable():
    c = PipelineCandidates()
    c.add_many([1, 2, 3])
    assert c.ids() == {1, 2, 3}
    # Generator works too — must not require list/set up-front.
    c.add_many(x for x in (4, 5))
    assert c.ids() == {1, 2, 3, 4, 5}


def test_add_deduplicates():
    c = PipelineCandidates()
    c.add(1)
    c.add(1)
    c.add_many([1, 1, 1])
    assert len(c) == 1


def test_ids_returns_a_copy():
    """Callers receive a copy so external mutation can't corrupt the
    pipeline's working set."""
    c = PipelineCandidates()
    c.add_many([10, 20])
    view = c.ids()
    view.add(999)
    assert 999 not in c
    assert c.ids() == {10, 20}


def test_contains_coerces_int_like():
    c = PipelineCandidates()
    c.add(42)
    assert 42 in c
    # Works with string-ified ids — some DB cursors return strings via
    # libraries that auto-convert. Internally we normalise to int.
    assert "42" in c


def test_add_coerces_to_int():
    c = PipelineCandidates()
    c.add("5")
    assert 5 in c
    assert c.ids() == {5}
