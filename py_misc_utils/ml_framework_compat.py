import collections
import os


_MODULES = dict()


def _register(name, mod, fromfn):
  _MODULES[name] = mod
  mod.__mlfc_name = name
  mod.__mlfc_from = fromfn


def _parse_priorities():
  prefs = os.getenv('MLFC_ORDER', 'torch,np,jax,tf').split(',')

  return {mod: len(prefs) - i for i, mod in enumerate(prefs)}


try:
  import numpy as np

  def _mlfc_np_from(mod, t, tref):
    if mod is torch or mod is tf:
      return t.numpy()
    if mod is jax:
      return np.asarray(t)

    return t

  _register('np', np, _mlfc_np_from)
except ImportError:
  np = None

try:
  import torch
  from torch.utils import dlpack as torch_dlpack

  def _mlfc_torch_from(mod, t, tref):
    if mod is np:
      return torch.from_numpy(t).to(tref.device)
    if mod is jax:
      return torch_dlpack.from_dlpack(jax_dlpack.to_dlpack(t)).to(tref.device)
    if mod is tf:
      return torch_dlpack.from_dlpack(tf_dlpack.to_dlpack(t)).to(tref.device)

    return t

  _register('torch', torch, _mlfc_torch_from)
except ImportError:
  torch = None

try:
  import jax
  from jax import dlpack as jax_dlpack
  import jax.numpy as jaxnp

  def _mlfc_jax_from(mod, t, tref):
    if mod is np:
      return jax.device_put(jax.asarray(t), tref.device)
    if mod is torch:
      return jax.device_put(jax_dlpack.from_dlpack(torch_dlpack.to_dlpack(t)), tref.device)
    if mod is tf:
      return jax.device_put(jax_dlpack.from_dlpack(tf_dlpack.to_dlpack(t)), tref.device)

    return t

  _register('jax', jaxnp, _mlfc_jax_from)
except ImportError:
  jaxnp = None

try:
  import tensorflow as tf
  import tf.experimental.dlpack as tf_dlpack
  import tensorflow.experimental.numpy as tfnp

  def _mlfc_tf_from(mod, t, tref):
    if mod is np:
      with tref.device:
        return tf.convert_to_tensor(t)
    if mod is torch:
      with tref.device:
        return tf_dlpack.from_dlpack(torch_dlpack.to_dlpack(t))
    if mod is jax:
      with tref.device:
        return tf_dlpack.from_dlpack(jax_dlpack.to_dlpack(t))

    return t

  tf.experimental_enable_numpy_behavior()
  _register('tf', tfnp, _mlfc_tf_from)
except ImportError:
  tfnp = None


_MODULES_PRIORITY = _parse_priorities()
_DEFAULT_MODULE = os.getenv('MLFC_DEFAULT', 'np')

if _DEFAULT_MODULE not in _MODULES:
  raise RuntimeError(f'Unable to find default ML module: {_DEFAULT_MODULE}')


def _get_module(t):
  # Let native Python types pass through, as it is expected that the module
  # function being used by the resolve() caller, handles those directly.
  if not isinstance(t, (int, float, str, bytes)):
    if np is not None and isinstance(t, np.ndarray):
      return np
    if torch is not None and isinstance(t, torch.Tensor):
      return torch
    if jax is not None and isinstance(t, jax.Tensor):
      return jaxnp
    if tf is not None and tf.is_tensor(t):
      return tfnp


def resolve(*args):
  mods = collections.defaultdict(list)

  for i, t in enumerate(args):
    mod = _get_module(t)
    if mod is not None:
      mods[mod].append(i)

  if not mods:
    return _MODULES[_DEFAULT_MODULE], args
  if len(mods) == 1:
    return next(iter(mods.keys())), args

  tprio, tmod, tref = -1, None, None
  for mod, indices in mods.items():
    prio = _MODULES_PRIORITY[mod.__mlfc_name]
    if prio > tprio:
      tmod = mod
      tprio = prio
      tref = args[indices[0]]

  rargs = list(args)
  for mod, indices in mods.items():
    if mod is not tmod:
      for i in indices:
        rargs[i] = tmod.__mlfc_from(mod, args[i], tref)

  return tmod, tuple(rargs)

