"""Tests for py_misc_utils.object_tracker.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): track_objects() scans gc.get_objects(), invoking
tracker.track(obj) for each object and - for matches - building a
newline report of info lines plus '  refby = <name> (<qualname>)'
lines sorted by priority; a tracker returning None skips the object; a
raising tracker logs a warning per object and yields an empty report;
max_references caps the refby lines.

The GC scan is monkeypatched to a tiny synthetic graph: the real scan
walks every tracked object and the unbounded referrer-graph traversal
of _get_tracking_references (which lacks a visited set) explodes on the
referrer cycles of a real interpreter heap (report-only hazard).
"""

import logging

import pytest

from py_misc_utils import object_tracker as ot


@pytest.fixture
def fake_gc(monkeypatch):
  """Install a synthetic GC object graph for the tracker scan."""

  def install(objects):
    monkeypatch.setattr(ot.gc, 'get_objects', lambda: list(objects))

    return objects

  return install


def test_track_objects_reports_matches(fake_gc):
  """Matching objects produce an info line plus refby lines."""
  leaf = dict(tag='leaf')
  holder = dict(leaf=leaf)
  fake_gc([holder, leaf])

  class Tracker:

    def track(self, obj):
      if obj is leaf:
        return (0, 'found the leaf')

      return None

  report = ot.track_objects(Tracker(), max_references=1)

  assert 'found the leaf' in report
  assert 'refby = ' in report


def test_track_objects_none_skips(fake_gc):
  """A tracker returning None for everything produces an empty report."""
  fake_gc([dict(a=1)])

  class Tracker:

    def track(self, obj):
      return None

  assert ot.track_objects(Tracker()) == ''


def test_track_objects_priority_ordering(fake_gc):
  """Lower priority entries are reported first."""
  d1 = dict(tag='d1')
  d2 = dict(tag='d2')
  l1 = [1, 2]
  fake_gc([d1, d2, l1])

  class PrioTracker:

    def track(self, obj):
      if isinstance(obj, (list, dict)):
        prio = 0 if isinstance(obj, dict) else 1

        return (prio, f'type={type(obj).__name__}')

      return None

  report = ot.track_objects(PrioTracker(), max_references=1)
  infos = [line for line in report.splitlines() if line and not line.startswith(' ')]

  assert infos
  dict_idx = [i for i, s in enumerate(infos) if 'dict' in s]
  list_idx = [i for i, s in enumerate(infos) if 'list' in s]
  assert dict_idx and list_idx
  assert max(dict_idx) < min(list_idx)


def test_track_objects_tracker_exception_logged(fake_gc, caplog):
  """A tracker raising exceptions logs warnings and yields no report."""
  fake_gc([dict(a=1)])

  class BadTracker:

    def track(self, obj):
      raise RuntimeError('tracker boom')

  logging.disable(logging.CRITICAL)
  try:
    with caplog.at_level(logging.WARNING):
      report = ot.track_objects(BadTracker())
  finally:
    logging.disable(logging.NOTSET)

  assert report == ''
  assert any('tracker boom' in rec.message for rec in caplog.records)


def test_track_objects_max_references(fake_gc):
  """max_references caps the number of refby lines per object."""
  leaf = dict(tag='leaf')
  h1 = dict(one=leaf)
  h2 = dict(two=leaf)
  fake_gc([h1, h2, leaf])

  class Tracker:

    def track(self, obj):
      return (0, 'the-leaf') if obj is leaf else None

  report = ot.track_objects(Tracker(), max_references=2)
  refby_lines = [l for l in report.splitlines() if l.strip().startswith('refby')]

  assert len(refby_lines) == 2

