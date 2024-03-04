import collections
import functools
import multiprocessing as mp
import multiprocessing.pool as mp_pool
import os
import pickle
import random
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


def _random_generate(space, count, pid):
  rng = np.random.default_rng()
  low = np.zeros(len(space), dtype=np.int32)
  high = np.array(space)

  rpoints = []
  for n in range(count):
    ridx = rng.integers(low, high)
    rpoints.append(Point(pid + n, ridx))

  return rpoints, pid + count


def _make_param(idx, skeys, params):
  param = dict()
  for i, k in enumerate(skeys):
    # We keep parameters as numpy arrays, but when we pluck values we want to
    # return them in Python scalar form.
    pvalue = params[k][idx[i]]
    param[k] = pvalue.item() if npu.is_numpy(pvalue) else pvalue

  return param


def _mp_score_fn(score_fn, params):
  return score_fn(**params)


def _score_slice(pts, skeys, params, score_fn, scores_db=None, n_jobs=None,
                 mp_ctx=None):
  xparams = [_make_param(pt.idx, skeys, params) for pt in pts]
  if n_jobs is None:
    scores = [score_fn(**p) for p in xparams]
  else:
    context = mp.get_context(mp_ctx if mp_ctx is not None else mp.get_start_method())
    fn = functools.partial(_mp_score_fn, score_fn)
    with mp_pool.Pool(processes=n_jobs if n_jobs > 0 else None, context=context) as pool:
      scores = list(pool.map(fn, xparams))

  return scores, xparams


def _get_scores(pts, skeys, params, score_fn, scores_db=None, n_jobs=None,
                mp_ctx=None):
  scores_x_run = ut.getenv('SCORES_X_RUN', dtype=int, defval=10)

  xparams, scores = [], []
  for i in range(0, len(pts), scores_x_run):
    cscores, cparams = _score_slice(pts[i: i + scores_x_run], skeys, params, score_fn,
                                    scores_db=scores_db,
                                    n_jobs=n_jobs,
                                    mp_ctx=mp_ctx)
    scores.extend(cscores)
    xparams.extend(cparams)
    if scores_db is not None:
      _register_scores(cparams, cscores, scores_db)

  return scores, xparams


def _select_missing(pts, processed, dest):
  for pt in pts:
    if pt.idx.tobytes() not in processed:
      dest.append(pt)


def _is_worth_gain(pscore, score, min_gain_pct):
  pabs = np.abs(pscore)
  if np.isclose(pabs, 0):
    return score > pscore

  delta = (score - pscore) / pabs

  return delta >= min_gain_pct


def _select_top_n(pts, scores, sidx, pid_scores, top_n, min_pid_gain_pct):
  pseen, fsidx = set(), []
  for i in sidx:
    pt = pts[i]
    if pt.pid not in pseen:
      pseen.add(pt.pid)
      pscore = pid_scores.get(pt.pid, None)
      if pscore is None or _is_worth_gain(pscore, scores[i], min_pid_gain_pct):
        pid_scores[pt.pid] = scores[i]
        fsidx.append(i)
        # The sidx array contains indices mapping to a descending sort of the
        # scores, so once we have top_n of them, we know we have selected the
        # higher ones available.
        if len(fsidx) >= top_n:
          break

  return fsidx


def _register_scores(xparams, scores, scores_db):
  for params, score in zip(xparams, scores):
    alog.debug0(f'SCORE: {score} -- {params}')

    for k, v in params.items():
      scores_db[k].append(v)
    scores_db['SCORE'].append(score)


class Selector:

  def __init__(self, params, init_count=10, delta_spacek=None, delta_std=0.2,
               top_n=10, rnd_n=10, explore_pct=0.05, min_pid_gain_pct=0.01,
               max_blanks=10, n_jobs=None, mp_ctx=None):
    self.delta_spacek = delta_spacek
    self.delta_std = delta_std
    self.top_n = top_n
    self.rnd_n = rnd_n
    self.explore_pct = explore_pct
    self.min_pid_gain_pct = min_pid_gain_pct
    self.max_blanks = max_blanks
    self.n_jobs = n_jobs
    self.mp_ctx = mp_ctx

    self.processed = set()
    self.scores_db = collections.defaultdict(list)
    self.best_score, self.best_idx, self.best_param = None, None, None
    self.pid_scores = dict()
    self.blanks = 0

    self.nparams = _norm_params(params)
    self.skeys, self.space = _get_space(self.nparams)
    self.pts, self.cpid = _random_generate(self.space, init_count, 0)

  def __call__(self, score_fn, status_path=None):
    alog.debug0(f'{len(self.space)} parameters, {np.prod(self.space)} configurations')

    max_explore = int(np.prod(self.space) * self.explore_pct)

    while self.pts and len(self.processed) < max_explore and self.blanks < self.max_blanks:
      alog.debug0(f'{len(self.pts)} points, {len(self.processed)} processed ' \
                  f'(max {max_explore}), {self.blanks} blanks')

      scores, xparams = _get_scores(self.pts, self.skeys, self.nparams, score_fn,
                                    scores_db=self.scores_db,
                                    n_jobs=self.n_jobs,
                                    mp_ctx=self.mp_ctx)

      # The np.argsort() has no "reverse" option, so it's either np.flip() or negate
      # the scores.
      sidx = np.flip(np.argsort(scores))

      fsidx = _select_top_n(self.pts, scores, sidx, self.pid_scores, self.top_n,
                            self.min_pid_gain_pct)

      score = scores[fsidx[0]]
      if self.best_score is None or score > self.best_score:
        self.best_score = score
        self.best_idx = self.pts[fsidx[0]].idx
        self.best_param = _make_param(self.best_idx, self.skeys, self.nparams)
        self.blanks = 0

        alog.debug0(f'BestScore = {self.best_score:.5e}\tParam = {self.best_param}')
      else:
        self.blanks += 1

      # Add the current parameter points to the set of the processed ones.
      for pt in self.pts:
        self.processed.add(pt.idx.tobytes())

      # Sample around best points ...
      next_pts = []
      for i in fsidx:
        ds = _select_deltas(self.pts[i], self.space, self.delta_spacek, self.delta_std)
        _select_missing(ds, self.processed, next_pts)

      # And randomly add ones in search of better scores.
      rnd_pts, self.cpid = _random_generate(self.space, self.rnd_n, self.cpid)
      _select_missing(rnd_pts, self.processed, next_pts)
      self.pts = next_pts

      if status_path is not None:
        self.save_status(status_path)

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

