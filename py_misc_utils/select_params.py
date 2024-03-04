import collections
import functools
import multiprocessing as mp
import multiprocessing.pool as mp_pool
import os
import random
import re
import subprocess

import numpy as np

from . import alog
from . import utils as ut


_Point = collections.namedtuple('Point', 'pid, idx')


def _sarray(n):
  return np.empty((n,), dtype=np.int32)


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

  return [_Point(pt.pid, _mkdelta(pt.idx, space, delta_std)) for _ in range(num_deltas)]


def _random_generate(space, count, pid):
  rng = np.random.default_rng()
  low = np.zeros(len(space), dtype=np.int32)
  high = np.array(space)

  rpoints = []
  for n in range(count):
    ridx = rng.integers(low, high)
    rpoints.append(_Point(pid + n, ridx))

  return rpoints, pid + count


def _make_param(idx, skeys, params):
  param = dict()
  for i, k in enumerate(skeys):
    # We keep parameters as numpy arrays, but when we pluck values we want to
    # return them in Python scalar form.
    pvalue = params[k][idx[i]]
    param[k] = pvalue.item() if isinstance(pvalue, np.ndarray) else pvalue

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


def _add_to_selection(pts, gset, dest=None):
  for pt in pts:
    ptb = pt.idx.tobytes()
    if ptb not in gset:
      gset.add(ptb)
      if dest is not None:
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


def select_params(params, score_fn, init_count=10, delta_spacek=None, delta_std=0.2,
                  top_n=10, rnd_n=10, explore_pct=0.05, min_pid_gain_pct=0.01,
                  max_blanks=10, n_jobs=None, mp_ctx=None):
  nparams = _norm_params(params)
  skeys, space = _get_space(nparams)
  alog.debug0(f'{len(space)} parameters, {np.prod(space)} configurations')

  pts, cpid = _random_generate(space, init_count, 0)

  processed = set()
  _add_to_selection(pts, processed)

  scores_db = collections.defaultdict(list)
  best_score, best_idx = None, None
  max_explore, blanks, pid_scores = int(np.prod(space) * explore_pct), 0, dict()
  while pts and len(processed) < max_explore and blanks < max_blanks:
    alog.debug0(f'{len(pts)} points, {len(processed) - len(pts)} processed ' \
                f'(max {max_explore}), {blanks} blanks')

    scores, xparams = _get_scores(pts, skeys, nparams, score_fn,
                                  scores_db=scores_db,
                                  n_jobs=n_jobs,
                                  mp_ctx=mp_ctx)

    # The np.argsort() has no "reverse" option, so it's either np.flip() or negate
    # the scores.
    sidx = np.flip(np.argsort(scores))

    fsidx = _select_top_n(pts, scores, sidx, pid_scores, top_n, min_pid_gain_pct)

    score = scores[fsidx[0]]
    if best_score is None or score > best_score:
      best_score = score
      best_idx = pts[fsidx[0]].idx
      alog.debug0(f'BestScore = {best_score:.5e}\tParam = {_make_param(best_idx, skeys, nparams)}')
      blanks = 0
    else:
      blanks += 1

    next_pts = []
    for i in fsidx:
      ds = _select_deltas(pts[i], space, delta_spacek, delta_std)
      _add_to_selection(ds, processed, dest=next_pts)

    rnd_pts, cpid = _random_generate(space, rnd_n, cpid)
    _add_to_selection(rnd_pts, processed, dest=next_pts)
    pts = next_pts

  return best_score, _make_param(best_idx, skeys, nparams), scores_db


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

