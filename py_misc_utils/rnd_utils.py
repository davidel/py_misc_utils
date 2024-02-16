import random
import threading

import numpy as np
import torch


_LOCK = threading.Lock()
_TORCH_RNDS = dict()
_NP_RNDGEN = None
_SEED = 153934223


def manual_seed(seed):
  global _NP_RNDGEN, _SEED

  with _LOCK:
    _SEED = seed
    for rndg in _TORCH_RNDS.values():
      rndg.manual_seed(seed)
    torch.manual_seed(seed)

    _NP_RNDGEN = np.random.default_rng(seed=seed)
    np.random.seed(seed)

    random.seed(seed)


def torch_gen(device='cpu'):
  tdevice = torch.device(device)
  with _LOCK:
    rndg = _TORCH_RNDS.get(tdevice, None)
    if rndg is None:
      rndg = torch.Generator()
      rndg.manual_seed(_SEED)
      _TORCH_RNDS[tdevice] = rndg

    return rndg


def torch_randn(*args, **kwargs):
  device = kwargs.get('device', 'cpu')
  kwargs.update(generator=torch_gen(device=device))

  return torch.randn(*args, **kwargs)


def numpy_gen():
  global _NP_RNDGEN

  with _LOCK:
    if _NP_RNDGEN is None:
      _NP_RNDGEN = np.random.default_rng(seed=_SEED)

    return _NP_RNDGEN

