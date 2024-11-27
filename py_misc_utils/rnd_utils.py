import binascii
import os
import random
import string
import struct
import tempfile
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


def compute_seed(seed):
  if isinstance(seed, int):
    seed = binascii.crc32(struct.pack('=q', seed))
  elif isinstance(seed, float):
    seed = binascii.crc32(struct.pack('=d', seed))
  elif isinstance(seed, bytes):
    seed = binascii.crc32(seed)
  elif isinstance(seed, str):
    seed = binascii.crc32(seed.encode())
  else:
    seed = binascii.crc32(struct.pack('=Q', hash(seed)))

  return seed


def manual_seed(seed):
  global _SEED

  cseed = compute_seed(seed)

  _SEED = cseed
  tls = _get_tls()

  for rndg in tls.torch_rnds.values():
    rndg.manual_seed(cseed)
  torch.manual_seed(cseed)

  tls.np_rdngen = np.random.default_rng(seed=cseed)
  np.random.seed(cseed)

  random.seed(cseed)

  return cseed


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


def rand_string(n):
  rng = random.SystemRandom()

  return ''.join(rng.choices(string.ascii_lowercase + string.digits, k=n))


_TMPFN_RNDSIZE = int(os.getenv('TMPFN_RNDSIZE', 10))

def temp_path(nspath=None, nsdir=None, rndsize=None):
  rndsize = rndsize or _TMPFN_RNDSIZE

  if nspath is not None:
    return f'{nspath}.{rand_string(rndsize)}'

  nsdir = tempfile.gettempdir() if nsdir is None else nsdir

  return os.path.join(nsdir, f'{rand_string(rndsize)}.tmp')

