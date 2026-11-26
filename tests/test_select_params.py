"""Tests for the py_misc_utils.select_params parameter search module.

Expectations follow the documented API contract: helpers normalize and
bound index arrays, the Selector records every evaluated configuration in
scores_db, selection is score-descending with unique point IDs and a
minimum-gain guard, batched fetching resumes from processed_scores without
re-evaluating points, status pickles roundtrip, and the subprocess helpers
tag/extract scores with the SPSCORE marker.
"""

import os
import subprocess
import sys

import numpy as np
import pytest

import py_misc_utils.select_params as sp


def _pool_score_fn(**kw):
  """Module-level picklable scoring function for multiprocessing tests."""
  return kw['a'] * 10 + kw['b']


@pytest.fixture
def seeded_rng(monkeypatch):
  """Force every default_rng() call in the module to a single seeded gen."""
  gen = np.random.default_rng(42)
  monkeypatch.setattr(np.random, 'default_rng', lambda: gen)

  return gen


def _mkpt(pid, idx):
  return sp.Point(pid, np.array(idx, dtype=np.int32))


def test_point_namedtuple_fields():
  """Point carries the point ID and the parameter index array."""
  pt = sp.Point(3, np.array([1, 2], dtype=np.int32))

  assert pt.pid == 3
  assert list(pt.idx) == [1, 2]


def test_norm_params_sorts_and_converts():
  """_norm_params() converts values to sorted numpy arrays."""
  nparams = sp._norm_params({'b': [3, 1, 2], 'a': [5, 4]})

  assert list(nparams['a']) == [4, 5]
  assert list(nparams['b']) == [1, 2, 3]
  assert isinstance(nparams['a'], np.ndarray)


def test_norm_params_scalar_param_crashes_report_only():
  """Scalar parameter values crash with numpy AxisError.

  Report-only finding: _norm_params() docstring says values are converted
  to arrays and sorted, but a bare scalar becomes a 0-d array on which
  np.sort() fails with AxisError instead of being wrapped as a 1-element
  space. Pending a decision on whether to validate inputs or wrap scalars.
  """
  with pytest.raises(np.exceptions.AxisError):
    sp._norm_params({'a': 5})


def test_get_space_sorted_keys_and_sizes():
  """_get_space() returns sorted keys and per-key space sizes."""
  nparams = sp._norm_params({'b': [1, 2, 3], 'a': [4, 5]})

  skeys, space = sp._get_space(nparams)

  assert skeys == ['a', 'b']
  assert space == [2, 3]


def test_mkdelta_zero_std_returns_clipped_integer_indices():
  """_mkdelta() with delta_std=0 reproduces idx, clipped to valid bounds.

  Clipping must use space-1 as the upper bound so generated indices stay
  inside the parameter value space.

  Report-only finding: the explicit .astype(np.int32) is undone by
  np.clip() because the upper bound (aspace - 1) is int64, so the result
  dtype is int64, not the int32 the cast intends.
  """
  idx = sp._mkdelta([1, 1], [5, 5], 0.0)

  assert idx.dtype == np.int64
  assert list(idx) == [1, 1]

  out = sp._mkdelta([10, -5], [5, 5], 0.0)

  assert list(out) == [4, 0]


def test_mp_score_fn_calls_with_kwargs():
  """_mp_score_fn() invokes the score function with params as keywords."""
  assert sp._mp_score_fn(lambda a, b: a + b, {'a': 2, 'b': 3}) == 5


def test_is_worth_gain_zero_prior_strictly_greater():
  """With a zero prior score, only a strictly greater score is a gain."""
  assert bool(sp._is_worth_gain(0, 1, 0.01))
  assert not sp._is_worth_gain(0, -1, 0.01)
  assert not sp._is_worth_gain(0, 0, 0.01)


def test_is_worth_gain_threshold_boundary_inclusive():
  """A gain exactly equal to min_gain_pct is accepted (>= comparison)."""
  assert bool(sp._is_worth_gain(100.0, 110.0, 0.1))
  assert not sp._is_worth_gain(100.0, 109.99, 0.1)


def test_is_worth_gain_negative_prior_uses_abs():
  """The gain is computed over the absolute prior score."""
  assert bool(sp._is_worth_gain(-100.0, -90.0, 0.1))
  assert not sp._is_worth_gain(-100.0, -95.0, 0.1)


def test_generating_yields_until_count_or_attempts():
  """_generating() stops at count items or count * _KGEN_EXTRA attempts."""
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})

  # With nothing appended, all round(count * _KGEN_EXTRA) attempts run.
  dest = []

  assert list(sel._generating(dest, 5)) == list(range(10))

  # Appending inside the loop stops the generator at the target count; the
  # explicit iterator protocol makes the StopIteration contract visible.
  dest2, attempts = [], []
  gen = sel._generating(dest2, 2)
  while True:
    try:
      n = next(gen)
    except StopIteration:
      break
    attempts.append(n)
    dest2.append(n)

  assert attempts == [0, 1]
  assert dest2 == [0, 1]

  # A zero count yields nothing.
  assert list(sel._generating([], 0)) == []


def test_init_normalizes_and_computes_space():
  """Selector init normalizes params and derives sorted keys/space."""
  sel = sp.Selector({'b': [3, 1, 2], 'a': [5, 4]})

  assert sel.skeys == ['a', 'b']
  assert sel.space == [2, 3]
  assert list(sel.nparams['a']) == [4, 5]
  assert sel.pts == []
  assert sel.best_score is None


def test_seeds_map_to_nearest_values():
  """Seeds are mapped to the nearest available parameter values."""
  sel = sp.Selector({
      'a': [1, 2, 3],
      'b': [10, 20, 30]
  },
                    seeds=[{
                        'a': 2,
                        'b': 20
                    }, {
                        'a': 2.6,
                        'b': 25
                    }])

  assert len(sel.pts) == 2
  # Seed 1 matches exactly: a=2 -> idx 1, b=20 -> idx 1.
  assert list(sel.pts[0].idx) == [1, 1]
  # Seed 2: a=2.6 is nearest to 3 (idx 2); b=25 ties 20/30, argmin picks 20.
  assert list(sel.pts[1].idx) == [2, 1]
  assert sel.cpid == 2


def test_duplicate_seeds_both_queued_empty_processed_set():
  """Duplicate seeds are both queued because processed is empty at init.

  Report-only finding: _load_seeds() checks _is_processed() to skip
  already-processed seeds, but the processed set is empty during __init__,
  so identical seeds are never skipped and both get queued (wasted
  evaluations). The current behavior is pinned here.
  """
  sel = sp.Selector({
      'a': [1, 2, 3],
      'b': [10, 20, 30]
  },
                    seeds=[{
                        'a': 2,
                        'b': 20
                    }, {
                        'a': 2,
                        'b': 20
                    }])

  assert len(sel.pts) == 2


def test_seed_missing_key_raises_keyerror():
  """A seed missing a parameter name raises KeyError."""
  with pytest.raises(KeyError):
    sp.Selector({'a': [1, 2, 3], 'b': [10, 20]}, seeds=[{'a': 1}])


def test_make_param_returns_python_scalars():
  """_make_param() plucks values as Python scalars, not numpy types."""
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})

  param = sel._make_param(np.array([2, 1], dtype=np.int32))

  assert param == {'a': 3, 'b': 20}
  assert type(param['a']) is int
  assert type(param['b']) is int


def test_register_scores_populates_scores_db():
  """_register_scores() records every param value and the SCORE column."""
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})

  sel._register_scores([{'a': 1, 'b': 10}, {'a': 3, 'b': 20}], [5.0, 7.0])

  assert sel.scores_db['a'] == [1, 3]
  assert sel.scores_db['b'] == [10, 20]
  assert sel.scores_db['SCORE'] == [5.0, 7.0]


def test_randgen_yields_in_bounds_unprocessed_points(seeded_rng):
  """_randgen() yields in-bounds points never processed before the call."""
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})

  rpoints = sel._randgen(4)

  assert len(rpoints) == 4
  assert [pt.pid for pt in rpoints] == [0, 1, 2, 3]
  assert sel.cpid == 4
  for pt in rpoints:
    assert 0 <= pt.idx[0] < 3
    assert 0 <= pt.idx[1] < 2

  # Every generated point is checked against the pre-call processed set.
  pre_existing = np.array([0, 1], dtype=np.int64).tobytes()
  sel.processed.add(pre_existing)

  assert all(pt.idx.tobytes() != pre_existing for pt in sel._randgen(4))


def test_randgen_in_batch_duplicates_report_only(seeded_rng):
  """_randgen() can emit duplicate points within a single batch.

  Report-only finding: the dedup check consults only the processed set,
  not the points already appended to the current batch, so the same index
  tuple can be evaluated twice in one batch (wasted score evaluations).
  With the seeded generator, 4 requested points collapse to 3 unique.
  """
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})

  rpoints = sel._randgen(4)

  assert len(rpoints) == 4
  assert len({pt.idx.tobytes() for pt in rpoints}) == 3


def test_randgen_exhausted_space_returns_empty():
  """When the whole space is processed, _randgen() gives up and returns []."""
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})

  for a in range(3):
    for b in range(2):
      sel.processed.add(np.array([a, b], dtype=np.int64).tobytes())

  assert sel._randgen(2) == []
  assert sel.cpid == 0


def test_select_deltas_count_modes_and_pid_inheritance(seeded_rng):
  """_select_deltas() honors delta_spacek modes and inherits the pid."""
  sel = sp.Selector({
      'a': [1, 2, 3, 4],
      'b': [10, 20],
      'c': [1, 2, 3, 4],
      'd': [10, 20]
  })
  pt = _mkpt(7, [1, 1, 2, 0])

  # delta_spacek=None -> one delta per dimension.
  deltas = sel._select_deltas(pt, None, 0.0)
  assert len(deltas) == 4
  assert all(d.pid == 7 for d in deltas)
  # int64, not the int32 _mkdelta()'s cast intends (see _mkdelta finding).
  assert all(d.idx.dtype == np.int64 for d in deltas)

  # Positive delta_spacek is a fraction of dimensions (ceil).
  assert len(sel._select_deltas(pt, 0.5, 0.0)) == 2

  # Negative delta_spacek is an absolute count.
  single = sel._select_deltas(pt, -1, 0.0)
  assert len(single) == 1
  assert list(single[0].idx) == [1, 1, 2, 0]
  assert single[0].pid == 7


def test_is_processed_roundtrip_and_dtype_caveat():
  """_is_processed() matches on raw tobytes, so it is dtype-sensitive.

  Report-only finding: processed keys are raw byte strings, so logically
  equal indices with different dtypes (e.g. the int64 arrays from
  _randgen vs the int32 arrays from _mkdelta/seeds) are treated as
  distinct points and can be evaluated twice.
  """
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})

  sel.processed.add(np.array([1, 1], dtype=np.int32).tobytes())

  assert sel._is_processed(np.array([1, 1], dtype=np.int32)) is True
  assert sel._is_processed(np.array([1, 1], dtype=np.int64)) is False
  assert sel._is_processed(np.array([2, 1], dtype=np.int32)) is False


@pytest.mark.parametrize('mp_ctx', ['spawn', 'forkserver'])
def test_score_slice_sequential_and_pool_agree(mp_ctx):
  """_score_slice() returns identical scores sequentially and via a pool."""
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20, 30]})
  pts = [_mkpt(0, [0, 0]), _mkpt(1, [2, 1]), _mkpt(2, [1, 2])]
  sel.pts = list(pts)

  seq = sel._score_slice(pts, _pool_score_fn, n_jobs=1)
  assert seq == [20.0, 50.0, 50.0]
  assert sel.scores_db['SCORE'] == seq
  assert sel.scores_db['a'] == [1, 3, 2]

  sel2 = sp.Selector({'a': [1, 2, 3], 'b': [10, 20, 30]})
  sel2.pts = list(pts)
  par = sel2._score_slice(pts, _pool_score_fn, n_jobs=2, mp_ctx=mp_ctx)

  assert par == seq


def test_fetch_scores_batches_and_saves_status(temp_path):
  """_fetch_scores() evaluates in scores_x_run chunks, saving after each.

  The batch boundaries and the per-batch persistence are both observed
  from the public side: the score function is a caller-supplied input and
  it is invoked between batches, so snapshotting the status file from
  within it shows what had been persisted before each point was scored.
  """
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})
  sel.pts = [
      _mkpt(i, [a, b])
      for i, (a, b) in enumerate([(0, 0), (1, 0), (2, 0), (0, 1), (1, 1)])
  ]

  calls = []
  persisted = []

  status_path = os.path.join(temp_path, 'status.pkl')

  def score_fn(a, b):
    calls.append((a, b))
    saved = (sp.Selector.load_status(status_path).processed_scores
             if os.path.exists(status_path) else None)
    persisted.append(saved)

    return a * 100 + b

  sel._fetch_scores(score_fn, n_jobs=1, scores_x_run=2, status_path=status_path)

  # 5 points in chunks of 2 -> 3 batches.
  assert calls == [(1, 10), (2, 10), (3, 10), (1, 20), (2, 20)]
  # Nothing saved during the first batch; then 2 and 4 points were on disk,
  # i.e. a save happened after each completed batch (and only then).
  assert persisted == [None, None, 2, 2, 4]
  assert sel.processed_scores == 5
  assert len(sel.current_scores) == 5
  assert sel.current_scores == [110.0, 210.0, 310.0, 120.0, 220.0]

  # The last saved status reflects the fully processed batch sequence.
  loaded = sp.Selector.load_status(status_path)
  assert loaded.processed_scores == 5


def test_fetch_scores_resume_skips_processed():
  """_fetch_scores() resumes from processed_scores without re-evaluation."""
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})
  sel.pts = [
      _mkpt(i, [a, b]) for i, (a, b) in enumerate([(0, 0), (1, 0), (2, 0), (0, 1)])
  ]
  # Simulate a restored status where the first batch was already scored.
  sel.processed_scores = 2

  calls = []

  def score_fn(a, b):
    calls.append((a, b))

    return a * 100 + b

  sel._fetch_scores(score_fn, n_jobs=1, scores_x_run=2)

  # Only the remaining two points are evaluated.
  assert calls == [(3, 10), (1, 20)]
  assert sel.processed_scores == 4
  assert sel.current_scores == [310.0, 120.0]


def test_select_top_n_unique_pids_and_cap():
  """_select_top_n() keeps the best score per pid and caps at top_n."""
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20, 30]})
  sel.pts = [_mkpt(10, [0, 0]), _mkpt(11, [1, 1]), _mkpt(12, [2, 2])]
  sel.current_scores = [30.0, 20.0, 10.0]

  fsidx = sel._select_top_n(2, 0.01)

  assert fsidx == [0, 1]
  assert sel.pid_scores == {10: 30.0, 11: 20.0}


def test_select_top_n_min_gain_rejects_and_keeps_pid_scores():
  """Scores without the required gain are rejected and not recorded."""
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})
  sel.pts = [_mkpt(5, [0, 0]), _mkpt(6, [1, 1])]
  sel.current_scores = [10.0, 9.0]
  sel.pid_scores = {5: 10.0, 6: 9.0}

  assert sel._select_top_n(2, 0.5) == []
  assert sel.pid_scores == {5: 10.0, 6: 9.0}


def test_select_top_n_boundary_gain_accepted():
  """A rescore exactly at min_pid_gain_pct replaces the pid score."""
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})
  sel.pts = [_mkpt(0, [0, 0]), _mkpt(0, [1, 1])]
  sel.current_scores = [10.0, 11.0]
  sel.pid_scores = {0: 10.0}

  fsidx = sel._select_top_n(2, 0.1)

  # 11.0 is a 10% gain over 10.0 and is accepted; 10.0 then loses to it.
  assert fsidx == [1]
  assert sel.pid_scores == {0: 11.0}


def test_call_finds_optimum_on_small_grid(seeded_rng, temp_path):
  """__call__() explores a tiny grid, finds the optimum and persists state."""
  calls = []

  def score_fn(a, b):
    calls.append((a, b))

    return -((a - 2)**2) * 10 - ((b - 20)**2)

  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})
  sel(score_fn,
      status_path=os.path.join(temp_path, 'status.pkl'),
      explore_pct=1.0,
      max_blanks_pct=0.5,
      top_n=2,
      scores_x_run=2,
      n_jobs=1)

  assert sel.best_score == 0
  assert sel.best_param == {'a': 2, 'b': 20}
  # The best score is the maximum of every evaluated score.
  assert sel.best_score == max(sel.scores_db['SCORE'])
  assert len(sel.processed) == len(set(sel.processed))
  assert calls


def test_call_constant_scores_terminate_via_blanks(seeded_rng):
  """A flat score surface stops the search through the blanks budget."""
  calls = []

  def score_fn(a, b):
    calls.append((a, b))

    return 5.0

  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})
  sel(score_fn, explore_pct=1.0, max_blanks_pct=0.2, top_n=2, scores_x_run=2, n_jobs=1)

  assert sel.best_score == 5.0
  assert sel.best_param is not None
  assert sel.blanks >= 1
  assert len(calls) >= 2


def test_save_load_status_roundtrip(temp_path):
  """save_status()/load_status() restore the full search state."""
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]}, seeds=[{'a': 2, 'b': 20}])
  sel.pts.extend(sel._randgen(2))
  sel._register_scores([{'a': 2, 'b': 20}], [3.5])
  sel.processed.add(np.array([1, 1], dtype=np.int32).tobytes())
  sel.pid_scores[0] = 3.5
  sel.best_score, sel.best_idx = 3.5, np.array([1, 1], dtype=np.int32)
  sel.best_param = {'a': 2, 'b': 20}
  sel.blanks, sel.cpid, sel.processed_scores = 2, 3, 1
  sel.current_scores = [3.5]

  path = os.path.join(temp_path, 'status.pkl')
  sel.save_status(path)
  loaded = sp.Selector.load_status(path)

  assert loaded.skeys == sel.skeys
  assert loaded.space == sel.space
  assert [pt.pid for pt in loaded.pts] == [pt.pid for pt in sel.pts]
  assert [pt.idx.tobytes() for pt in loaded.pts] == \
    [pt.idx.tobytes() for pt in sel.pts]
  assert sorted(loaded.processed) == sorted(sel.processed)
  assert loaded.pid_scores == sel.pid_scores
  assert loaded.scores_db['SCORE'] == [3.5]
  assert loaded.best_score == 3.5
  assert loaded.best_param == {'a': 2, 'b': 20}
  assert loaded.blanks == 2
  assert loaded.cpid == 3
  assert loaded.processed_scores == 1


def test_format_score_match_score_roundtrip():
  """format_score() produces the SPSCORE tag that match_score() parses."""
  assert sp.format_score(3.25) == '[SPSCORE=3.250000]'
  assert sp.match_score(sp.format_score(3.25)) == [3.25]


def test_match_score_multiple_and_none():
  """match_score() extracts every tag and returns [] without matches."""
  assert sp.match_score('[SPSCORE=1][SPSCORE=-2.5]') == [1.0, -2.5]
  assert sp.match_score('noise [SPSCORE=2.25] end') == [2.25]
  assert sp.match_score('no tags here') == []


def test_match_score_malformed_raises_valueerror():
  """A non-numeric score value raises ValueError from float()."""
  with pytest.raises(ValueError):
    sp.match_score('[SPSCORE=abc]')


def test_run_score_process_success():
  """run_score_process() returns extracted scores plus the full output."""
  cmdline = [
      sys.executable, '-c', "print('[SPSCORE=1.5]'); print('noise [SPSCORE=2.25] end')"
  ]

  scores, output = sp.run_score_process(cmdline)

  assert scores == [1.5, 2.25]
  assert 'noise' in output
  assert '[SPSCORE=1.5]' in output


def test_run_score_process_failure_raises():
  """A failing scoring command re-raises subprocess.CalledProcessError."""
  cmdline = [
      sys.executable, '-c', "import sys; print('boom [SPSCORE=9.9]'); sys.exit(3)"
  ]

  with pytest.raises(subprocess.CalledProcessError):
    sp.run_score_process(cmdline)
