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
from . import np_utils as npu
from . import utils as ut


Point = collections.namedtuple('Point', 'pid, idx')


def _norm_params(params):
  nparams = dict()
  for k, v in params.items():
    if not isinstance(v, np.ndarray):
      v = np.array(v)
    nparams[k] = np.sort(v)

  return nparams


def _get_space(params):
  skeys = sorted(params.keys())

  return skeys, [len(params[k]) for k in skeys]


def _mkdelta(idx, space, delta_std):
  # Sample around the index.
  rng = np.random.default_rng()

  aspace = np.array(space)
  delta = np.array(idx) + rng.standard_normal(len(idx)) * aspace * delta_std
  delta = np.rint(delta).astype(np.int32)

  return np.clip(delta, np.zeros_like(delta), aspace - 1)


def _select_deltas(pt, space, delta_spacek, delta_std):
  if delta_spacek is None:
    num_deltas = len(space)
  elif delta_spacek > 0:
    num_deltas = int(np.ceil(len(space) * delta_spacek))
  else:
    num_deltas = int(np.rint(-delta_spacek))

  return [Point(pt.pid, _mkdelta(pt.idx, space, delta_std)) for _ in range(num_deltas)]


def _mp_score_fn(score_fn, params):
  return score_fn(**params)


def _is_worth_gain(pscore, score, min_gain_pct):
  pabs = np.abs(pscore)
  if np.isclose(pabs, 0):
    return score > pscore

  delta = (score - pscore) / pabs

  return delta >= min_gain_pct


class Selector:

  def __init__(self, params):
    self.nparams = _norm_params(params)
    self.processed = set()
    self.scores_db = collections.defaultdict(list)
    self.best_score, self.best_idx, self.best_param = None, None, None
    self.pid_scores = dict()
    self.blanks = 0
    self.skeys, self.space = _get_space(self.nparams)
    self.pts, self.cpid = [], 0
    self.current_scores, self.processed_scores = [], 0

  def _register_scores(self, xparams, scores):
    for params, score in zip(xparams, scores):
      alog.debug0(f'SCORE: {score} -- {params}')

      for k, v in params.items():
        self.scores_db[k].append(v)
      self.scores_db['SCORE'].append(score)

  def _make_param(self, idx):
    param = dict()
    for i, k in enumerate(self.skeys):
      # We keep parameters as numpy arrays, but when we pluck values we want to
      # return them in Python scalar form.
      pvalue = self.nparams[k][idx[i]]
      param[k] = pvalue.item() if npu.is_numpy(pvalue) else pvalue

    return param

  def _score_slice(self, pts, score_fn, n_jobs=None, mp_ctx=None):
    xparams = [self._make_param(pt.idx) for pt in pts]

    n_jobs = os.cpu_count() if n_jobs is None else n_jobs
    if n_jobs == 1:
      scores = [score_fn(**p) for p in xparams]
    else:
      context = mp.get_context(mp_ctx if mp_ctx is not None else mp.get_start_method())
      fn = functools.partial(_mp_score_fn, score_fn)
      with mp_pool.Pool(processes=n_jobs if n_jobs > 0 else None, context=context) as pool:
        scores = list(pool.map(fn, xparams))

    self._register_scores(xparams, scores)

    return scores

  def _fetch_scores(self, score_fn, n_jobs=None, mp_ctx=None, scores_x_run=None,
                    status_path=None):
    scores_x_run = scores_x_run or ut.getenv('SCORES_X_RUN', dtype=int, defval=10)

    for i in range(self.processed_scores, len(self.pts), scores_x_run):
      current_points = self.pts[i: i + scores_x_run]

      scores = self._score_slice(current_points, score_fn, n_jobs=n_jobs, mp_ctx=mp_ctx)

      self.current_scores.extend(scores)
      self.processed_scores += len(current_points)

      alog.info(f'Processed {self.processed_scores}/{len(self.pts)}: ' \
                f'{ut.format(sorted(self.current_scores, reverse=True), ".6e")}')

      if status_path is not None:
        self.save_status(status_path)

  def _select_top_n(self, top_n, min_pid_gain_pct):
    # The np.argsort() has no "reverse" option, so it's either np.flip() or negate
    # the scores.
    sidx = np.flip(np.argsort(self.current_scores))

    pseen, fsidx = set(), []
    for i in sidx:
      pt = self.pts[i]
      if pt.pid not in pseen:
        pseen.add(pt.pid)
        pscore = self.pid_scores.get(pt.pid, None)
        if pscore is None or _is_worth_gain(pscore, self.current_scores[i], min_pid_gain_pct):
          self.pid_scores[pt.pid] = self.current_scores[i]
          fsidx.append(i)
          # The sidx array contains indices mapping to a descending sort of the
          # scores, so once we have top_n of them, we know we have selected the
          # higher ones available.
          if len(fsidx) >= top_n:
            break

    return fsidx

  def _select_missing(self, pts):
    new_pts = []
    for pt in pts:
      if pt.idx.tobytes() not in self.processed:
        new_pts.append(pt)

    return new_pts

  def _random_generate(self, count):
    rng = np.random.default_rng()
    low = np.zeros(len(self.space), dtype=np.int32)
    high = np.array(self.space, dtype=np.int32)

    rpoints = []
    for _ in range(count):
      ridx = rng.integers(low, high)
      rpoints.append(Point(self.cpid, ridx))
      self.cpid += 1

    return rpoints

  def _randgen(self, rnd_n):
    rnd_pts = self._random_generate(rnd_n)

    return self._select_missing(rnd_pts)

  def __call__(self, score_fn,
               status_path=None,
               delta_spacek=None,
               delta_std=0.2,
               top_n=10,
               rnd_n=10,
               explore_pct=0.05,
               min_pid_gain_pct=0.01,
               max_blanks_pct=0.1,
               scores_x_run=None,
               n_jobs=None,
               mp_ctx=None):
    alog.debug0(f'{len(self.space)} parameters, {np.prod(self.space)} configurations')

    if not self.pts:
      self.pts.extend(self._randgen(rnd_n))

    max_explore = int(np.prod(self.space) * explore_pct)
    max_blanks = int(max_explore * max_blanks_pct)
    while self.pts and len(self.processed) < max_explore and self.blanks < max_blanks:
      alog.debug0(f'{len(self.pts)} points, {len(self.processed)} processed ' \
                  f'(max {max_explore}), {self.blanks} / {max_blanks} blanks')

      self._fetch_scores(score_fn,
                         n_jobs=n_jobs,
                         mp_ctx=mp_ctx,
                         scores_x_run=scores_x_run,
                         status_path=status_path)

      fsidx = self._select_top_n(top_n, min_pid_gain_pct)

      score = self.current_scores[fsidx[0]]
      if self.best_score is None or score > self.best_score:
        self.best_score = score
        self.best_idx = self.pts[fsidx[0]].idx
        self.best_param = self._make_param(self.best_idx)
        self.blanks = 0

        alog.debug0(f'BestScore = {self.best_score:.5e}\tParam = {self.best_param}')
      else:
        self.blanks += len(self.pts)
        alog.info(f'Score not improved: blanks={self.blanks}/{max_blanks}')

      for pt in self.pts:
        self.processed.add(pt.idx.tobytes())

      # Sample around best points ...
      next_pts = []
      for i in fsidx:
        dpts = _select_deltas(self.pts[i], self.space, delta_spacek, delta_std)
        next_pts.extend(self._select_missing(dpts))

      # And randomly add ones in search of better scores.
      next_pts.extend(self._randgen(rnd_n))

      self.pts = next_pts
      self.current_scores, self.processed_scores = [], 0

  def save_status(self, path):
    with open(path, mode='wb') as sfd:
      pickle.dump(self, sfd, protocol=ut.pickle_proto())

  @staticmethod
  def load_status(path):
    with open(path, mode='rb') as sfd:
      return pickle.load(sfd)


_SCORE_TAG = 'SPSCORE'
_SCORE_FMT = os.getenv('SPSCORE_FMT', 'f')

def format_score(s):
  return f'[{_SCORE_TAG}={s:{_SCORE_FMT}}]'


def match_score(data):
  matches = re.findall(f'\[{_SCORE_TAG}=' + r'([^]]+)\]', data)

  return [float(m) for m in matches]


def run_score_process(cmdline):
  try:
    output = subprocess.check_output(cmdline, stderr=subprocess.STDOUT)
  except subprocess.CalledProcessError as ex:
    alog.exception(ex, exmsg=f'Error while running scoring process: {ex.output.decode()}')
    raise

  if isinstance(output, bytes):
    output = output.decode()

  return match_score(output), output

