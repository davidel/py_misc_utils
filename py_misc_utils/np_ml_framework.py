import collections
import os


_MODULES = dict()


def _register(name, mod, checkfn, fromfn):
  _MODULES[name] = mod
  mod.__npml_name = name
  mod.__npml_check = checkfn
  mod.__npml_from = fromfn


def _parse_priorities():
  prefs = os.getenv('NPML_ORDER', 'torch,np,jax,tf').split(',')

  return {mod: len(prefs) - i for i, mod in enumerate(prefs)}


# Numpy
try:
  import numpy as np

  def _npml_np_from(mod, t, tref):
    if mod is torch or mod is tf:
      return t.numpy()
    if mod is jax:
      return np.asarray(t)

    return t

  def _npml_np_check(t):
    return isinstance(t, np.ndarray)

  _register('np', np, _npml_np_check, _npml_np_from)
except ImportError:
  np = None


# PyTorch
try:
  import torch
  from torch.utils import dlpack as torch_dlpack

  def _npml_torch_from(mod, t, tref):
    if mod is np:
      return torch.from_numpy(t).to(tref.device)
    if mod is jax:
      return torch_dlpack.from_dlpack(jax_dlpack.to_dlpack(t)).to(tref.device)
    if mod is tf:
      return torch_dlpack.from_dlpack(tf_dlpack.to_dlpack(t)).to(tref.device)

    return t

  def _npml_torch_check(t):
    return isinstance(t, torch.Tensor)

  _register('torch', torch, _npml_torch_check, _npml_torch_from)
except ImportError:
  torch = None


# JAX
try:
  import jax
  from jax import dlpack as jax_dlpack
  import jax.numpy as jaxnp

  def _npml_jax_from(mod, t, tref):
    if mod is np:
      return jax.device_put(jax.asarray(t), tref.device)
    if mod is torch:
      return jax.device_put(jax_dlpack.from_dlpack(torch_dlpack.to_dlpack(t)), tref.device)
    if mod is tf:
      return jax.device_put(jax_dlpack.from_dlpack(tf_dlpack.to_dlpack(t)), tref.device)

    return t

  def _npml_jax_check(t):
    return isinstance(t, jax.Tensor)

  _register('jax', jaxnp, _npml_jax_check, _npml_jax_from)
except ImportError:
  jaxnp = None


# Tensorflow
try:
  import tensorflow as tf
  import tf.experimental.dlpack as tf_dlpack
  import tensorflow.experimental.numpy as tfnp

  def _npml_tf_from(mod, t, tref):
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

  def _npml_tf_check(t):
    return tf.is_tensor(t)

  tfnp.experimental_enable_numpy_behavior()
  _register('tf', tfnp, _npml_tf_check, _npml_tf_from)
except ImportError:
  tfnp = None


_DEFAULT_MODULE = os.getenv('NPML_DEFAULT', 'np')

if _DEFAULT_MODULE not in _MODULES:
  raise RuntimeError(f'Unable to find default Numpy ML module: {_DEFAULT_MODULE}')


_MODULES_PRIORITY = _parse_priorities()
_MODULES_SEQ = tuple(_MODULES[x] for x in sorted(list(_MODULES.keys()),
                                                 key=lambda m: _MODULES_PRIORITY.get(m, -1),
                                                 reverse=True))


def _get_module(t):
  for mod in _MODULES_SEQ:
    if mod.__npml_check(t):
      return mod


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
    prio = _MODULES_PRIORITY[mod.__npml_name]
    if prio > tprio:
      tmod = mod
      tprio = prio
      tref = args[indices[0]]

  rargs = list(args)
  for mod, indices in mods.items():
    if mod is not tmod:
      for i in indices:
        rargs[i] = tmod.__npml_from(mod, args[i], tref)

  return tmod, tuple(rargs)

