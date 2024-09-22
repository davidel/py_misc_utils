import random
import threading

import numpy as np
import torch


_SEED = 153934223
_TLS = threading.local()


def _get_tls():
  if not getattr(_TLS, 'init', False):
    _TLS.torch_rnds = dict()
    _TLS.np_rdngen = None
    _TLS.init = True

  return _TLS


def manual_seed(seed):
  global _SEED

  _SEED = seed
  tls = _get_tls()

  for rndg in tls.torch_rnds.values():
    rndg.manual_seed(seed)
  torch.manual_seed(seed)

  tls.np_rdngen = np.random.default_rng(seed=seed)
  np.random.seed(seed)

  random.seed(seed)


def torch_gen(device='cpu'):
  tls = _get_tls()

  tdevice = torch.device(device)
  rndg = tls.torch_rnds.get(tdevice)
  if rndg is None:
    rndg = torch.Generator()
    rndg.manual_seed(_SEED)
    tls.torch_rnds[tdevice] = rndg

  return rndg


def torch_randn(*args, **kwargs):
  device = kwargs.get('device', 'cpu')
  kwargs.update(generator=torch_gen(device=device))

  return torch.randn(*args, **kwargs)


def numpy_gen():
  tls = _get_tls()

  if tls.np_rdngen is None:
    tls.np_rdngen = np.random.default_rng(seed=_SEED)

  return tls.np_rdngen


def choices(weights, n):
  return random.choices(range(len(weights)), weights=weights, k=n)

