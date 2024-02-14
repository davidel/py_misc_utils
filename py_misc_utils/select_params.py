import functools
import math
import multiprocessing as mp
import random

import numpy as np

from . import alog


class _Point(object):

  def __init__(self, pid, idx):
    self.pid = pid
    self.idx = idx


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


def _get_deltas(idx, space, dsize=1):
  deltas = []
  for i, v in enumerate(idx):
    dp = [v]
    if v >= dsize:
      dp.append(v - dsize)
    elif v > 0:
      dp.append(0)
    if v + dsize < space[i]:
      dp.append(v + dsize)
    elif v < space[i] - 1:
      dp.append(space[i] - 1)
    deltas.append(dp)

  return deltas


def _select_deltas(pt, space, sel_pct, dsize=1):
  deltas = _get_deltas(pt.idx, space, dsize=dsize)

  delta_pts = []
  idx = [0] * len(deltas)
  i = 0
  while i < len(deltas):
    didx = _sarray(len(idx))
    for j, n in enumerate(idx):
      didx[j] = deltas[j][n]
    delta_pts.append(_Point(pt.pid, didx))

    i = 0
    idx[i] += 1
    while idx[i] >= len(deltas[i]):
      idx[i] = 0
      i += 1
      if i < len(deltas):
        idx[i] += 1
      else:
        break

  random.shuffle(delta_pts)
  num_deltas = int(math.ceil(np.prod([len(x) for x in deltas]) * sel_pct))

  return delta_pts[: num_deltas]


def _random_generate(space, count, pid):
  rgen = []
  for n in range(0, count):
    ridx = _sarray(len(space))
    for i in range(0, len(ridx)):
      ridx[i] = min(int(space[i] * random.random()), space[i] - 1)
    rgen.append(_Point(pid + n, ridx))

  return rgen, pid + count


def _make_param(idx, skeys, params):
  param = dict()
  for i, k in enumerate(skeys):
    param[k] = params[k][idx[i]]

  return param


def _mp_score_fn(score_fn, params):
  return score_fn(**params)


def _get_scores(pts, skeys, params, score_fn, n_jobs=None, mp_ctx=None):
  xparams = [_make_param(pt.idx, skeys, params) for pt in pts]
  if n_jobs is None:
    scores = [score_fn(**p) for p in xparams]
  else:
    context = mp.get_context(mp_ctx if mp_ctx is not None else mp.get_start_method())
    fn = functools.partial(_mp_score_fn, score_fn)
    with mp.pool.Pool(processes=n_jobs if n_jobs > 0 else None,
                      context=context) as pool:
      scores = list(pool.map(fn, xparams))

  return scores


def _add_to_selection(pts, gsel, gset):
  for pt in pts:
    ptb = pt.idx.tobytes()
    if ptb not in gset:
      gset.add(ptb)
      gsel.append(pt)


def _is_worth_gain(pscore, score, min_gain_pct):
  if pscore is None:
    return True
  delta = 100 * (score - pscore) / abs(pscore) if abs(pscore) > 1e-6 else 1.0

  return delta >= min_gain_pct


def _select_top_n(pts, scores, sidx, pid_scores, top_n, min_pid_gain_pct):
  pseen = set()
  fsidx = []
  for i in sidx:
    pt = pts[i]
    if pt.pid not in pseen:
      pseen.add(pt.pid)
      pscore = pid_scores.get(pt.pid, None)
      if _is_worth_gain(pscore, scores[i], min_pid_gain_pct):
        pid_scores[pt.pid] = scores[i]
        fsidx.append(i)
        if len(fsidx) >= top_n:
          break

  return fsidx


def select_params(params, score_fn, init_count=10, sel_pct=0.1, dsize=1,
                  top_n=10, rnd_n=10, explore_pct=0.05, min_pid_gain_pct=0.01,
                  max_blanks=10, n_jobs=None, mp_ctx=None):
  nparams = _norm_params(params)
  skeys, space = _get_space(nparams)
  alog.debug0(f'{len(space)} parameters, {np.prod(space)} configurations')

  pts, cpid = _random_generate(space, init_count, 0)

  best_score, best_idx = None, None

  max_explore, blanks = int(np.prod(space) * explore_pct), 0
  processed = set()
  pid_scores = dict()
  while len(processed) < max_explore and blanks < max_blanks:
    alog.debug0(f'{len(pts)} points, {len(processed)} processed (max {max_explore})')

    scores = _get_scores(pts, skeys, nparams, score_fn, n_jobs=n_jobs, mp_ctx=mp_ctx)
    sidx = sorted(list(range(0, len(scores))), key=lambda i: scores[i], reverse=True)

    fsidx = _select_top_n(pts, scores, sidx, pid_scores, top_n, min_pid_gain_pct)

    score = scores[fsidx[0]] if fsidx else None
    if score is not None and (best_score is None or score > best_score):
      best_score = score
      best_idx = pts[fsidx[0]].idx
      alog.debug0(f'BestScore = {best_score:.5e}\tParam = {_make_param(best_idx, skeys, nparams)}')
      blanks = 0
    else:
      blanks += 1

    gtop = []
    for i in fsidx:
      ds = _select_deltas(pts[i], space, sel_pct, dsize=dsize)
      _add_to_selection(ds, gtop, processed)

    rnd_pts, cpid = _random_generate(space, rnd_n, cpid)
    _add_to_selection(rnd_pts, gtop, processed)
    pts = gtop
    if not pts:
      break

  return best_score, _make_param(best_idx, skeys, nparams)
