import collections
import functools
import multiprocessing as mp
import multiprocessing.pool as mp_pool
import random
import re
import subprocess

import numpy as np

from . import alog


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


def _generate_delta(idx, space, dstd):
  # Sample around the index.
  rng = np.random.default_rng()

  sstd = np.array(space) * dstd

  delta = np.array(idx) + rng.standard_normal(len(idx)) * sstd
  delta = np.rint(delta, dtype=np.int32)

  return np.clip(delta, np.zeros_like(delta), np.array(space) - 1)


def _select_deltas(pt, space, delta_spacek, dstd):
  num_deltas = int(np.ceil(len(space) * delta_spacek))

  return [_Point(pt.pid, _generate_delta(pt.idx, space, dstd)) for _ in range(num_deltas)]


def _random_generate(space, count, pid):
  rgen = []
  for n in range(count):
    ridx = _sarray(len(space))
    for i in range(len(ridx)):
      ridx[i] = random.randrange(space[i])

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
    with mp_pool.Pool(processes=n_jobs if n_jobs > 0 else None, context=context) as pool:
      scores = list(pool.map(fn, xparams))

  return scores, xparams


def _add_to_selection(pts, gsel, gset):
  for pt in pts:
    ptb = pt.idx.tobytes()
    if ptb not in gset:
      gset.add(ptb)
      gsel.append(pt)


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
        if len(fsidx) >= top_n:
          break

  return fsidx


def _register_scores(xparams, scores, scores_db):
  for params, score in zip(xparams, scores):
    alog.debug0(f'SCORE: {score} -- {params}')

    for k, v in params.items():
      scores_db[k].append(v)
    scores_db['SCORE'].append(score)


def select_params(params, score_fn, init_count=10, delta_spacek=2.0, dstd=0.1,
                  top_n=10, rnd_n=10, explore_pct=0.05, min_pid_gain_pct=0.01,
                  max_blanks=10, n_jobs=None, mp_ctx=None):
  nparams = _norm_params(params)
  skeys, space = _get_space(nparams)
  alog.debug0(f'{len(space)} parameters, {np.prod(space)} configurations')

  pts, cpid = _random_generate(space, init_count, 0)

  scores_db = collections.defaultdict(list)
  best_score, best_idx = None, None
  max_explore, blanks = int(np.prod(space) * explore_pct), 0
  processed, pid_scores = set(), dict()
  while len(processed) < max_explore and blanks < max_blanks:
    alog.debug0(f'{len(pts)} points, {len(processed)} processed (max {max_explore})')

    scores, xparams = _get_scores(pts, skeys, nparams, score_fn,
                                  n_jobs=n_jobs,
                                  mp_ctx=mp_ctx)

    _register_scores(xparams, scores, scores_db)

    # The np.argsort() has no "reverse" option, so it's either np.flip() or negate
    # the scores.
    sidx = np.flip(np.argsort(scores))

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
      ds = _select_deltas(pts[i], space, delta_spacek, dstd=dstd)
      _add_to_selection(ds, gtop, processed)

    rnd_pts, cpid = _random_generate(space, rnd_n, cpid)
    _add_to_selection(rnd_pts, gtop, processed)
    pts = gtop
    if not pts:
      break

  return best_score, _make_param(best_idx, skeys, nparams), scores_db


SCORE_TAG = 'SPSCORE'

def format_score(s):
  return f'[{SCORE_TAG}={s:f}]'


def match_score(data):
  matches = re.findall(f'\[{SCORE_TAG}' + r'=([^]]+)\]', data)

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

