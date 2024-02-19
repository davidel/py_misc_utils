import collections
import os


_MODULES = dict()


def _register(name, mod, checkfn, fromfn):
  _MODULES[name] = mod
  mod.__npml_name = name
  mod.__npml_check = checkfn
  mod.__npml_from = fromfn


def _register_attr(mod, name, attr):
  xa = getattr(mod, name, None)
  if xa is not None:
    raise RuntimeError(f'Attribute "{name}" already exists: {xa}')

  setattr(mod, name, attr)


def _parse_priorities():
  prefs = os.getenv('NPML_ORDER', 'torch,np,jax,tf').split(',')

  return {mod: len(prefs) - i for i, mod in enumerate(prefs)}


# Numpy
try:
  import numpy as np

  def _npml_np_from(mod, t, tref):
    if mod is not None:
      if mod is torch or mod is tfnp:
        return t.numpy()

    return np.asarray(t)

  def _npml_np_check(t):
    return isinstance(t, np.ndarray)

  _register('np', np, _npml_np_check, _npml_np_from)

  _register_attr(np, 'item', lambda t: t.item())
  _register_attr(np, 'tolist', lambda t: t.tolist())
except ImportError:
  np = None


# PyTorch
try:
  import torch
  from torch.utils import dlpack as torch_dlpack

  def _npml_torch_from(mod, t, tref):
    if mod is not None:
      if mod is np:
        return torch.from_numpy(t).to(tref.device)
      if mod is jaxnp:
        return torch_dlpack.from_dlpack(jax_dlpack.to_dlpack(t)).to(tref.device)
      if mod is tfnp:
        return torch_dlpack.from_dlpack(tf_dlpack.to_dlpack(t)).to(tref.device)

    return torch.tensor(t).to(tref.device)

  def _npml_torch_check(t):
    return isinstance(t, torch.Tensor)

  _register('torch', torch, _npml_torch_check, _npml_torch_from)

  _register_attr(torch, 'item', lambda t: t.item())
  _register_attr(torch, 'tolist', lambda t: t.tolist())
except ImportError:
  torch = None


# JAX
try:
  import jax
  from jax import dlpack as jax_dlpack
  import jax.numpy as jaxnp

  def _npml_jax_from(mod, t, tref):
    if mod is not None:
      if mod is torch:
        return jax.device_put(jax_dlpack.from_dlpack(torch_dlpack.to_dlpack(t)), tref.device)
      if mod is tfnp:
        return jax.device_put(jax_dlpack.from_dlpack(tf_dlpack.to_dlpack(t)), tref.device)

    return jax.device_put(jaxnp.asarray(t), tref.device)

  def _npml_jax_check(t):
    return isinstance(t, jax.Tensor)

  _register('jax', jaxnp, _npml_jax_check, _npml_jax_from)

  _register_attr(jaxnp, 'item', lambda t: t.item())
  _register_attr(jaxnp, 'tolist', lambda t: t.tolist())
except ImportError:
  jaxnp = None


# Tensorflow
try:
  import tensorflow as tf
  import tf.experimental.dlpack as tf_dlpack
  import tensorflow.experimental.numpy as tfnp

  def _npml_tf_from(mod, t, tref):
    if mod is not None:
      if mod is torch:
        with tref.device:
          return tf_dlpack.from_dlpack(torch_dlpack.to_dlpack(t))
      if mod is jaxnp:
        with tref.device:
          return tf_dlpack.from_dlpack(jax_dlpack.to_dlpack(t))

    with tref.device:
      return tf.convert_to_tensor(t)

  def _npml_tf_check(t):
    return tf.is_tensor(t)

  tfnp.experimental_enable_numpy_behavior()
  _register('tf', tfnp, _npml_tf_check, _npml_tf_from)

  _register_attr(tfnp, 'item', lambda t: t.item())
  _register_attr(tfnp, 'tolist', lambda t: t.tolist())
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
  mods = []
  tprio, tmod, tref = -1, None, None
  for i, t in enumerate(args):
    mod = _get_module(t)
    mods.append(mod)
    if mod is not None:
      prio = _MODULES_PRIORITY[mod.__npml_name]
      if prio > tprio:
        tmod = mod
        tprio = prio
        tref = t

  if tref is None:
    return _MODULES[_DEFAULT_MODULE], args

  rargs = list(args)
  for i, t in enumerate(args):
    if tmod is not mods[i]:
      rargs[i] = tmod.__npml_from(mods[i], t, tref)

  return tmod, tuple(rargs)

