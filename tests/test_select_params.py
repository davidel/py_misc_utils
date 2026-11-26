"""Tests for the py_misc_utils.select_params parameter search module.

Expectations follow the documented API contract, driven through the
public search loop: the Selector normalizes parameter spaces, maps seeds
to the nearest available values, records every evaluated configuration
in scores_db, selection keeps the best score per point ID behind a
minimum-gain guard, batched fetching persists status after each batch,
sequential and pooled runs agree on the optimum, status pickles roundtrip,
and the subprocess helpers tag/extract scores with the SPSCORE marker.

The random generator is pinned via the seeded_rng fixture; since the
fixture is function-scoped every test starts from the same generator
state, which keeps the pinned scores reproducible.

Internal helpers (_norm_params, _get_space, _mkdelta, _generating,
_randgen, _select_deltas, _is_processed, _score_slice, _fetch_scores,
_select_top_n, _register_scores) are not exercised directly; their
behavior is observable through the public attributes updated by
__call__(). Findings that were only visible at helper level are
recorded as report-only notes in SCRATCH/REFACTORING_PLAN.md.
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


def test_init_normalizes_and_computes_space():
  """Selector init normalizes params and derives sorted keys/space."""
  sel = sp.Selector({'b': [3, 1, 2], 'a': [5, 4]})

  assert sel.skeys == ['a', 'b']
  assert sel.space == [2, 3]
  assert list(sel.nparams['a']) == [4, 5]
  assert list(sel.nparams['b']) == [1, 2, 3]
  assert isinstance(sel.nparams['a'], np.ndarray)
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
  # Value 2 maps to index 1 in [1, 2, 3]; 2.6 maps to index 2 (nearest
  # value 3) and 25 to the nearest value 20.
  assert [list(pt.idx) for pt in sel.pts] == [[1, 1], [2, 1]]


def test_duplicate_seeds_both_queued_empty_processed_set():
  """Duplicate seeds are both queued while the processed set is empty."""
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


def test_call_seeds_evaluated_first_and_status_saved_per_batch(seeded_rng, temp_path):
  """A seeds-driven run evaluates the seeds before exploring and saves
  the status after each completed batch."""
  status_path = os.path.join(temp_path, 'status.pkl')
  calls, persisted = [], []

  def score_fn(a, b):
    calls.append((a, b))
    persisted.append(
        sp.Selector.load_status(status_path).processed_scores if os.path.
        exists(status_path) else None)
    return a * 10 + b

  sel = sp.Selector({
      'a': [1, 2],
      'b': [10, 20]
  },
                    seeds=[{
                        'a': 1,
                        'b': 10
                    }, {
                        'a': 2,
                        'b': 10
                    }, {
                        'a': 1,
                        'b': 20
                    }, {
                        'a': 2,
                        'b': 20
                    }])
  sel(score_fn,
      status_path=status_path,
      explore_pct=1.0,
      max_blanks_pct=1.0,
      top_n=4,
      scores_x_run=2,
      n_jobs=1)

  # The four seeds cover the whole 2x2 grid: each configuration is
  # evaluated exactly once, seeds first.
  assert sorted(calls) == [(1, 10), (1, 20), (2, 10), (2, 20)]
  # Nothing is on disk during the first batch; each completed batch then
  # persists the number of scores evaluated so far.
  assert persisted == [None, None, 2, 2]
  assert sel.best_score == 40
  assert sel.best_param == {'a': 2, 'b': 20}
  assert sel.scores_db['SCORE'] == [20, 30, 30, 40]


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
      n_jobs=1)

  assert calls
  assert sel.best_score == 0
  assert sel.best_param == {'a': 2, 'b': 20}
  # The best score is the maximum of every evaluated score.
  assert sel.best_score == max(sel.scores_db['SCORE'])
  assert len(sel.processed) == len(set(sel.processed))


def test_call_pid_scores_track_best_score_per_pid(seeded_rng):
  """pid_scores records each point ID's best score; a rejected rescore
  leaves the recorded score untouched."""
  calls = []

  def score_fn(a, b):
    calls.append((a, b))

    return 5.0

  # A flat surface with a 100% required gain: no rescore is ever accepted.
  sel = sp.Selector({'a': [1, 2], 'b': [10]})
  sel(score_fn,
      explore_pct=1.0,
      max_blanks_pct=1.0,
      top_n=2,
      scores_x_run=2,
      n_jobs=1,
      min_pid_gain_pct=1.0)

  assert calls == [(1, 10), (2, 10)]
  assert sel.pid_scores == {0: 5.0, 1: 5.0}
  assert sel.best_score == 5.0


def test_call_empty_selection_counts_blanks_instead_of_crashing(seeded_rng):
  """A pending point whose recorded score cannot be improved leaves the
  selection empty: the iteration counts as blanks instead of raising
  IndexError, and random exploration with fresh point IDs continues.

  Report-only finding fixed: __call__ indexed current_scores[fsidx[0]]
  without guarding an empty selection.
  """
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})

  # Public attributes only: stage a single pending point whose pid
  # already has a high recorded score.
  sel.pts = [sp.Point(0, np.array([0, 0], dtype=np.int32))]
  sel.pid_scores = {0: 1000.0}

  sel(lambda a, b: 20.0, explore_pct=1.0, max_blanks_pct=1.0, top_n=2, n_jobs=1)

  # Every score is 20.0 and the first fresh-pid evaluation is accepted,
  # so the optimum is eventually recorded without any crash.
  assert sel.best_score == 20.0
  assert sel.best_param is not None
  assert sel.blanks >= 1


def test_call_exhausted_space_makes_no_evaluations(seeded_rng):
  """When every configuration is marked processed, __call__ is a no-op."""
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20]})

  # Public attributes only: mark the whole space as processed.
  for a in range(3):
    for b in range(2):
      sel.processed.add(np.array([a, b], dtype=np.int64).tobytes())

  calls = []
  sel(lambda a, b: calls.append((a, b)) or 1.0,
      explore_pct=1.0,
      max_blanks_pct=1.0,
      top_n=2,
      n_jobs=1)

  assert calls == []
  assert sel.best_score is None


def test_call_sequential_finds_optimum(seeded_rng):
  """A sequential run reaches the grid optimum."""
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20, 30]})
  sel(_pool_score_fn, explore_pct=1.0, max_blanks_pct=1.0, top_n=3, n_jobs=1)

  assert (sel.best_score, sel.best_param) == (60, {'a': 3, 'b': 30})


def test_call_fork_pool_finds_same_optimum(seeded_rng):
  """A fork-pool run draws the same points as the sequential run.

  Each test starts from the same generator state (function-scoped
  fixture), so the only difference against the sequential run is the
  execution model: the same points are evaluated and the same optimum
  is found.
  """
  sel = sp.Selector({'a': [1, 2, 3], 'b': [10, 20, 30]})
  sel(_pool_score_fn,
      explore_pct=1.0,
      max_blanks_pct=1.0,
      top_n=3,
      n_jobs=2,
      mp_ctx='forkserver')

  assert (sel.best_score, sel.best_param) == (60, {'a': 3, 'b': 30})


def test_save_load_status_roundtrip(seeded_rng, temp_path):
  """save_status()/load_status() restore the state recorded at the last
  save, which happens inside the batch fetch.

  Everything the selection does after the fetch is absent from the file:
  the pid scores recorded by the selection and the points queued for the
  next iteration. The processed set, in contrast, is saved current:
  points are marked as processed as soon as their score is fetched.
  """
  path = os.path.join(temp_path, 'status.pkl')

  sel = sp.Selector({
      'a': [1, 2],
      'b': [10, 20]
  },
                    seeds=[{
                        'a': 1,
                        'b': 10
                    }, {
                        'a': 2,
                        'b': 10
                    }])
  sel(lambda a, b: a * 10 + b,
      status_path=path,
      explore_pct=1.0,
      max_blanks_pct=1.0,
      top_n=2,
      scores_x_run=2,
      n_jobs=1)

  loaded = sp.Selector.load_status(path)

  assert loaded.skeys == sel.skeys
  assert loaded.space == sel.space
  # The saved points are the ones pending at the last save: the deltas
  # (pids 1, 1, 0, 0) and the random points (pids 2, 3) queued by the
  # second iteration -- the seeds were consumed by the first one. The
  # live object moved on to the third-iteration queue.
  assert [pt.pid for pt in loaded.pts] == [1, 1, 0, 0, 2, 3]
  assert loaded.cpid == 4
  assert sel.cpid == 5
  # The selection of the last iteration (pids 2 and 3) postdates the
  # last save, so the file only knows the seed scores.
  assert loaded.pid_scores == {1: 30, 0: 20}
  assert sel.pid_scores == {1: 30, 0: 20, 3: 30, 2: 30}
  # The processed marks are current on disk. The tiny space keeps
  # regenerating the same two configurations, so the 2 seed keys plus
  # the 2 distinct int64 keys of the fetched points make 4 entries (the
  # dtype-sensitive raw tobytes() keys of the report-only finding keep
  # the seed and delta keys separate).
  assert len(loaded.processed) == 4
  assert len(sel.processed) == 4
  # Both scores were found before the last save, so best matches here.
  assert loaded.best_score == 30
  assert loaded.best_param == {'a': 2, 'b': 10}
  assert sel.best_score == 30
  assert sel.best_param == {'a': 2, 'b': 10}
  # Every evaluated score is on disk, while the live counter was reset
  # for the next iteration.
  assert loaded.scores_db['SCORE'] == sel.scores_db['SCORE']
  assert loaded.scores_db['SCORE'] == [20, 30, 30, 30, 20, 20, 30, 30]
  # processed_scores counts the progress within the current iteration
  # while scores_db accumulates across iterations, so the file pairs the
  # 8 recorded scores with a counter of 6; a resumed run continues at
  # pts[6:] and no evaluated configuration is repeated.
  assert loaded.processed_scores == 6
  assert sel.processed_scores == 0


def test_status_saves_carry_current_processed_marks(seeded_rng, temp_path):
  """Status saves record every configuration fetched so far.

  Report-only finding fixed: the processed set used to be marked only at
  iteration end, after the last save, so mid-run saves carried no
  information about the points being scored and a resumed run
  regenerated and re-evaluated already scored configurations.

  Note: dedup matches raw tobytes() keys, and seeds are keyed as int32
  while delta/random points are keyed as int64 (separate report-only
  finding), so cross-dtype duplicates remain possible until that is
  addressed.
  """
  status_path = os.path.join(temp_path, 'status.pkl')
  seed_keys = [np.array([a - 1, 0], dtype=np.int32).tobytes() for a in (1, 2, 3)]

  sel = sp.Selector({
      'a': [1, 2, 3],
      'b': [10, 20]
  },
                    seeds=[{
                        'a': 1,
                        'b': 10
                    }, {
                        'a': 2,
                        'b': 10
                    }, {
                        'a': 3,
                        'b': 10
                    }])
  sel(lambda a, b: a * 10 + b,
      status_path=status_path,
      explore_pct=0.5,
      max_blanks_pct=1.0,
      top_n=3,
      scores_x_run=3,
      n_jobs=1)

  assert sel.best_score == 40

  # Every fetched point is already in the processed set of the last
  # save: the on-disk state is never behind the evaluations.
  loaded = sp.Selector.load_status(status_path)
  assert loaded.processed == sel.processed
  for skey in seed_keys:
    assert skey in loaded.processed


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
      sys.executable, '-c',
      "import sys; print('computed [SPSCORE=1.5]'); print('[SPSCORE=2.5]')"
  ]

  scores, output = sp.run_score_process(cmdline)

  assert scores == [1.5, 2.5]
  assert '[SPSCORE=1.5]' in output


def test_run_score_process_failure_raises():
  """A failing scoring command re-raises subprocess.CalledProcessError."""
  cmdline = [
      sys.executable, '-c', "import sys; print('boom [SPSCORE=9.9]'); sys.exit(3)"
  ]

  with pytest.raises(subprocess.CalledProcessError):
    sp.run_score_process(cmdline)
