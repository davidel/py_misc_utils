"""Parameter selection and optimization utilities for hyperparameter search.

This module provides the Selector class which performs intelligent parameter
space exploration using a combination of random seeding, delta-based local
search around promising points, and parallel score evaluation. It also includes
utility functions for formatting and extracting scores from subprocess output.
"""

import collections
import functools
import multiprocessing as mp
import multiprocessing.pool as mp_pool
import os
import pickle
import re
import subprocess

import numpy as np

from . import alog
from . import gfs
from . import utils as ut

Point = collections.namedtuple('Point', 'pid, idx')
# Named tuple representing a search point with pid (point ID) and idx
# (parameter index array).

_SCORES_X_RUN = ut.getenv('SCORES_X_RUN', dtype=int, defval=10)
# Default number of scores to process per batch run.

_KGEN_EXTRA = ut.getenv('KGEN_EXTRA', dtype=float, defval=2)
# Multiplier for maximum random generation attempts.


def _norm_params(params):
  """Normalize parameter values to sorted numpy arrays.

  Converts each parameter value to a numpy array if needed and sorts it.

  Args:
    params: Dictionary mapping parameter names to array-like values.

  Returns:
    A dictionary mapping parameter names to sorted numpy arrays.
  """
  nparams = dict()
  for k, v in params.items():
    if not isinstance(v, np.ndarray):
      v = np.array(v)
    nparams[k] = np.sort(v)

  return nparams


def _get_space(params):
  """Extract sorted parameter keys and the size of each parameter's value space.

  Args:
    params: Dictionary mapping parameter names to sorted numpy arrays.

  Returns:
    A tuple (skeys, space) where skeys is a list of sorted parameter name strings
    and space is a list of integers representing the number of values for each
    parameter.
  """
  skeys = sorted(params.keys())

  return skeys, [len(params[k]) for k in skeys]


def _mkdelta(idx, space, delta_std):
  """Sample a perturbed index around the given index within the space bounds.

  Adds Gaussian noise scaled by delta_std to each component of idx, rounds to
  integers, and clips to the valid range [0, space-1].

  Args:
    idx: Array-like of integer indices.
    space: Array-like of space sizes for each dimension.
    delta_std: Standard deviation scale for the Gaussian perturbation.

  Returns:
    A numpy array of integer indices clipped to valid bounds.
  """
  # Sample around the index.
  rng = np.random.default_rng()

  aspace = np.array(space)
  delta = np.array(idx) + rng.standard_normal(len(idx)) * aspace * delta_std
  delta = np.rint(delta).astype(np.int32)

  return np.clip(delta, np.zeros_like(delta), aspace - 1)


def _mp_score_fn(score_fn, params):
  """Call score_fn with the given params as keyword arguments.

  Used as the target function for multiprocessing Pool.map.

  Args:
    score_fn: A callable that accepts keyword arguments.
    params: Dictionary of keyword arguments for score_fn.

  Returns:
    The result of calling score_fn(**params).
  """
  return score_fn(**params)


def _is_worth_gain(pscore, score, min_gain_pct):
  """Determine if a new score represents a meaningful gain over a previous score.

  Args:
    pscore: The previous score value.
    score: The new score value to compare.
    min_gain_pct: Minimum relative gain threshold (as a fraction).

  Returns:
    True if the new score is sufficiently better than the previous score,
    False otherwise.
  """
  pabs = np.abs(pscore)
  if np.isclose(pabs, 0):
    return score > pscore

  delta = (score - pscore) / pabs

  return delta >= min_gain_pct


class Selector:
  """Intelligent parameter space explorer that searches for optimal configurations.

  Uses a combination of random seeding, delta-based local search around the best
  points, and additional random exploration to efficiently search a high-dimensional
  parameter space. Scores are evaluated in parallel using multiprocessing.
    cpid: Counter for assigning point IDs.
    current_scores: List of scores from the current batch.
    processed_scores: Count of scores processed so far.
  """

  def __init__(self, params, seeds=None):
    """Initialize the Selector with a parameter space and optional seed points.

    Args:
      params: Dictionary mapping parameter names to array-like values. Each
        value will be converted to a sorted numpy array.
      seeds: Optional iterable of parameter dicts to use as starting points.
        Each seed's values are matched to the nearest available parameter value.
        Defaults to None.
    """
    self.nparams = _norm_params(params)
    self.processed = set()
    self.scores_db = collections.defaultdict(list)
    self.best_score, self.best_idx, self.best_param = None, None, None
    self.pid_scores = dict()
    self.blanks = 0
    self.skeys, self.space = _get_space(self.nparams)
    self.pts, self.cpid = [], 0
    self.current_scores, self.processed_scores = [], 0
    if seeds:
      self._load_seeds(seeds)

  def _load_seeds(self, seeds):
    """Load seed parameter configurations as initial search points.

    For each seed, finds the nearest available parameter value for each key
    and adds the resulting point if it hasn't been processed already.

    Args:
      seeds: Iterable of parameter dicts to use as starting points.
    """
    for sd in seeds:
      idx = np.zeros(len(self.space), dtype=np.int32)
      for i, k in enumerate(self.skeys):
        v = sd[k]
        idx[i] = np.argmin(np.abs(self.nparams[k] - v))

      if not self._is_processed(idx):
        self.pts.append(Point(self.cpid, idx))
        self.cpid += 1
      else:
        alog.info(f'Seed already processed: {sd}')

  def _register_scores(self, xparams, scores):
    """Record scores and parameter values for a batch of evaluated points.

    Args:
      xparams: List of parameter dicts that were evaluated.
      scores: List of score values corresponding to each param dict.
    """
    for params, score in zip(xparams, scores):
      alog.debug0(f'SCORE: {score} -- {params}')

      for k, v in params.items():
        self.scores_db[k].append(v)
      self.scores_db['SCORE'].append(score)

  def _make_param(self, idx):
    """Convert an index array to a parameter dict using scalar values.

    Args:
      idx: Array-like of integer indices into the parameter value spaces.

    Returns:
      A dictionary mapping parameter names to Python scalar values.
    """
    param = dict()
    for i, k in enumerate(self.skeys):
      # We keep parameters as numpy arrays, but when we pluck values we want to
      # return them in Python scalar form.
      pvalue = self.nparams[k][idx[i]]
      param[k] = pvalue.item()

    return param

  def _score_slice(self, pts, score_fn, n_jobs=None, mp_ctx=None):
    """Evaluate score_fn on a slice of points, optionally in parallel.

    Args:
      pts: List of Point namedtuples to evaluate.
      score_fn: Callable accepting keyword arguments from parameter dicts.
      n_jobs: Number of parallel workers. If None, uses os.cpu_count(). If 1,
        runs sequentially. Defaults to None.
      mp_ctx: Multiprocessing context method name. Defaults to None.

    Returns:
      A list of score values, one per point in pts.
    """
    xparams = [self._make_param(pt.idx) for pt in pts]

    n_jobs = os.cpu_count() if n_jobs is None else n_jobs
    if n_jobs == 1:
      scores = [score_fn(**p) for p in xparams]
    else:
      context = mp.get_context(mp_ctx if mp_ctx is not None else mp.get_start_method())
      fn = functools.partial(_mp_score_fn, score_fn)
      with mp_pool.Pool(processes=n_jobs if n_jobs > 0 else None,
                        context=context) as pool:
        scores = list(pool.map(fn, xparams))

    self._register_scores(xparams, scores)

    return scores

  def _fetch_scores(self,
                    score_fn,
                    n_jobs=None,
                    mp_ctx=None,
                    scores_x_run=None,
                    status_path=None):
    """Fetch scores in batches from the pending points list.

    Processes points in chunks of scores_x_run, evaluating each batch and
    optionally saving status after each batch.

    Points are marked as processed as soon as their score is fetched, so
    that status files saved mid-run never carry stale dedup information:
    a run resumed from any save does not re-evaluate already scored
    configurations.

    Args:
      score_fn: Callable accepting keyword arguments from parameter dicts.
      n_jobs: Number of parallel workers. Defaults to None.
      mp_ctx: Multiprocessing context method name. Defaults to None.
      scores_x_run: Number of scores to process per batch. Defaults to
        _SCORES_X_RUN.
      status_path: Optional file path to save intermediate status. Defaults to None.
    """
    scores_x_run = scores_x_run or _SCORES_X_RUN

    for i in range(self.processed_scores, len(self.pts), scores_x_run):
      current_points = self.pts[i:i + scores_x_run]

      scores = self._score_slice(current_points, score_fn, n_jobs=n_jobs, mp_ctx=mp_ctx)

      for pt in current_points:
        self.processed.add(pt.idx.tobytes())

      self.current_scores.extend(scores)
      self.processed_scores += len(current_points)

      alog.info(f'Processed {self.processed_scores}/{len(self.pts)}: ' \
                f'{ut.format(sorted(self.current_scores, reverse=True), ".6e")}')

      if status_path is not None:
        self.save_status(status_path)

  def _select_top_n(self, top_n, min_pid_gain_pct):
    """Select indices of the top N points by score, ensuring unique PIDs.

    Uses np.argsort with np.flip for descending sort, then filters to keep
    only the best score per point ID. Applies a minimum gain threshold.

    Args:
      top_n: Maximum number of points to select.
      min_pid_gain_pct: Minimum relative gain required to accept a new score
        for an already-seen point ID.

    Returns:
      A list of indices into self.pts for the selected top points.
    """
    # The np.argsort() has no "reverse" option, so it's either np.flip() or negate
    # the scores.
    sidx = np.flip(np.argsort(self.current_scores))

    pseen, fsidx = set(), []
    for i in sidx:
      pt = self.pts[i]
      if pt.pid not in pseen:
        pseen.add(pt.pid)
        pscore = self.pid_scores.get(pt.pid)
        if pscore is None or _is_worth_gain(pscore, self.current_scores[i],
                                            min_pid_gain_pct):
          self.pid_scores[pt.pid] = self.current_scores[i]
          fsidx.append(i)
          # The sidx array contains indices mapping to a descending sort of the
          # scores, so once we have top_n of them, we know we have selected the
          # higher ones available.
          if len(fsidx) >= top_n:
            break

    return fsidx

  def _is_processed(self, idx):
    """Check if a given index array has already been processed.

    Args:
      idx: Array-like of integer indices.

    Returns:
      True if the index's byte representation is in the processed set.
    """
    return idx.tobytes() in self.processed

  def _generating(self, dest, count):
    """Generator yielding attempt numbers while generating new points.

    Yields attempt numbers until either the destination has enough points or
    the maximum number of attempts is reached.

    Args:
      dest: List to append generated points to.
      count: Target number of points to generate.

    Yields:
      Integer attempt numbers.
    """
    max_attempts = round(count * _KGEN_EXTRA)
    n = 0
    while count > len(dest) and n < max_attempts:
      yield n
      n += 1

  def _randgen(self, count):
    """Generate random unprocessed points within the parameter space.

    Args:
      count: Number of random points to generate.

    Returns:
      A list of Point namedtuples with random indices.
    """
    rng = np.random.default_rng()
    high = np.array(self.space, dtype=np.int32)
    low = np.zeros_like(high)

    rpoints = []
    for _ in self._generating(rpoints, count):
      idx = rng.integers(low, high)
      if not self._is_processed(idx):
        rpoints.append(Point(self.cpid, idx))
        self.cpid += 1

    return rpoints

  def _select_deltas(self, pt, delta_spacek, delta_std):
    """Generate delta perturbations around a given point.

    Creates new points near pt.idx by sampling within a subset of dimensions
    determined by delta_spacek, with perturbation scale delta_std.

    Args:
      pt: The Point namedtuple to generate deltas around.
      delta_spacek: Fraction or count of dimensions to perturb. If None, all
        dimensions. If positive, fraction of total dimensions. If negative,
        absolute count of dimensions.
      delta_std: Standard deviation scale for Gaussian perturbation.

    Returns:
      A list of Point namedtuples representing delta variations.
    """
    if delta_spacek is None:
      num_deltas = len(self.space)
    elif delta_spacek > 0:
      num_deltas = int(np.ceil(len(self.space) * delta_spacek))
    else:
      num_deltas = int(np.rint(-delta_spacek))

    deltas = []
    for _ in self._generating(deltas, num_deltas):
      idx = _mkdelta(pt.idx, self.space, delta_std)
      if not self._is_processed(idx):
        deltas.append(Point(pt.pid, idx))

    return deltas

  def __call__(self,
               score_fn,
               status_path=None,
               delta_spacek=None,
               delta_std=0.1,
               top_n=10,
               explore_pct=0.05,
               rnd_pct=0.2,
               min_pid_gain_pct=0.01,
               max_blanks_pct=0.1,
               scores_x_run=None,
               n_jobs=None,
               mp_ctx=None):
    """Run the parameter search optimization loop.

    Iteratively evaluates score batches, selects the best points, explores
    around them with deltas, and adds random exploration until the search
    budget is exhausted or no improvement is found.

    Args:
      score_fn: Callable accepting keyword arguments from parameter dicts,
        returning a numeric score (higher is better).
      status_path: Optional file path to save intermediate status. Defaults
        to None.
      delta_spacek: Fraction or count of dimensions to perturb for local
        search. Defaults to None.
      delta_std: Standard deviation scale for Gaussian perturbation around
        best points. Defaults to 0.1.
      top_n: Number of top points to select each iteration. Defaults to 10.
      explore_pct: Fraction of total space to explore as the maximum budget.
        Defaults to 0.05.
      rnd_pct: Fraction of next points to fill with random exploration.
        Defaults to 0.2.
      min_pid_gain_pct: Minimum relative gain to accept a new score for an
        already-seen point ID. Defaults to 0.01.
      max_blanks_pct: Fraction of max_explore allowed as consecutive
        non-improving iterations before stopping. Defaults to 0.1.
      scores_x_run: Number of scores to process per batch. Defaults to None
        (uses _SCORES_X_RUN).
      n_jobs: Number of parallel workers. Defaults to None.
      mp_ctx: Multiprocessing context method name. Defaults to None.
    """
    alog.debug0(f'{len(self.space)} parameters, {np.prod(self.space)} configurations')

    if not self.pts:
      self.pts.extend(self._randgen(top_n))

    max_explore = int(np.prod(self.space) * explore_pct)
    max_blanks = int(max_explore * max_blanks_pct)
    while self.pts and len(self.processed) < max_explore and self.blanks < max_blanks:
      alog.debug0(f'{len(self.pts)} points, {len(self.processed)} processed ' \
                  f'(max {max_explore}), {self.blanks}/{max_blanks} blanks')

      self._fetch_scores(score_fn,
                         n_jobs=n_jobs,
                         mp_ctx=mp_ctx,
                         scores_x_run=scores_x_run,
                         status_path=status_path)

      fsidx = self._select_top_n(top_n, min_pid_gain_pct)

      # An empty selection means no pending point improved over its
      # recorded score: the iteration counts as blanks instead of
      # crashing on the missing best entry.
      score = self.current_scores[fsidx[0]] if fsidx else None

      if score is not None and (self.best_score is None or score > self.best_score):
        self.best_score = score
        self.best_idx = self.pts[fsidx[0]].idx
        self.best_param = self._make_param(self.best_idx)
        self.blanks = 0

        alog.debug0(f'BestScore = {self.best_score:.5e}\tParam = {self.best_param}')
      else:
        self.blanks += len(self.pts)
        alog.info(
            f'Score not improved (current run top {score}, best {self.best_score})')

      # Sample around best points ...
      next_pts = []
      for i in fsidx:
        next_pts.extend(self._select_deltas(self.pts[i], delta_spacek, delta_std))

      # And randomly add ones in search of better scores.
      rnd_count = max(top_n, int(rnd_pct * len(next_pts)))
      next_pts.extend(self._randgen(rnd_count))

      self.pts = next_pts
      self.current_scores, self.processed_scores = [], 0

  def save_status(self, path):
    """Save the current Selector state to a pickle file.

    Args:
      path: Filesystem path to write the pickle file to.
    """
    with gfs.open(path, mode='wb') as sfd:
      pickle.dump(self, sfd, protocol=ut.pickle_proto())

  @staticmethod
  def load_status(path):
    """Load a Selector state from a pickle file.

    Args:
      path: Filesystem path to read the pickle file from.

    Returns:
        A Selector instance restored from the pickle file.
    """
    with gfs.open(path, mode='rb') as sfd:
      return pickle.load(sfd)


_SCORE_TAG = 'SPSCORE'
# Tag string used to mark scores in subprocess output.

_SCORE_FMT = os.getenv('SPSCORE_FMT', 'f')
# Format string for score values, from SPSCORE_FMT env var or default 'f'.


def format_score(s):
  """Format a score value as a tagged string for subprocess output.

  Args:
    s: The score value to format.

  Returns:
    A string like '[SPSCORE=0.123456]' using the configured format.
  """
  return f'[{_SCORE_TAG}={s:{_SCORE_FMT}}]'


def match_score(data):
  """Extract all score values from text containing tagged score markers.

  Searches for patterns like [SPSCORE=value] in the data and returns
  the values as floats.

  Args:
    data: Text string to search for score markers.

  Returns:
    A list of float score values found in the data.
  """
  matches = re.findall(f'\\[{_SCORE_TAG}=' + r'([^]]+)\]', data)

  return [float(m) for m in matches]


def run_score_process(cmdline):
  """Run an external scoring command and extract scores from its output.

  Executes the command line, captures stdout, decodes if necessary, and
  extracts score values using match_score(). Raises on process failure.

  Args:
    cmdline: List of command line arguments to execute.

  Returns:
    A tuple (scores, output) where scores is a list of floats extracted
    from the command output and output is the full decoded stdout string.

  Raises:
    subprocess.CalledProcessError: If the command exits with non-zero status.
  """
  try:
    output = subprocess.check_output(cmdline, stderr=subprocess.STDOUT)
  except subprocess.CalledProcessError as ex:
    alog.exception(ex,
                   exmsg=f'Error while running scoring process: {ex.output.decode()}')
    raise

  if isinstance(output, bytes):
    output = output.decode()

  return match_score(output), output
