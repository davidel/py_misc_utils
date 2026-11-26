"""Framework-agnostic Numpy-like array adapter for ML libraries.

This module allows code to operate on arrays interchangeably from Numpy,
PyTorch, JAX, and Tensorflow. Each available framework is registered with
check and conversion functions, and resolve() normalizes a set of arguments
to the arrays of the highest-priority framework (selected via the NPML_ORDER
and NPML_DEFAULT environment variables).

If a framework is not installed, it is simply skipped at import time.
"""

import os

_MODULES = dict()


def _register_attr(mod, name, attr):
  """Set an attribute on a registered module, guarding against overrides."""
  xa = getattr(mod, name, None)
  if xa is not None:
    raise RuntimeError(f'Attribute "{name}" already exists: {xa}')

  setattr(mod, name, attr)


def _register(name, mod, checkfn, fromfn, attrs):
  """Register a framework module with its check and conversion functions.

  Args:
    name: Framework name (e.g. 'np', 'torch', 'jax', 'tf').
    mod: The module object whose arrays are instances of this framework.
    checkfn: Function returning True if an array belongs to the framework.
    fromfn: Function converting an array of another framework to this one.
    attrs: Dictionary of helper attributes to set on the module, such as
      item() and tolist() accessors.
  """
  _MODULES[name] = mod
  mod.__npml_name = name
  mod.__npml_check = checkfn
  mod.__npml_from = fromfn

  for attr_name, attr_value in attrs.items():
    _register_attr(mod, attr_name, attr_value)


def _parse_priorities():
  """Compute a framework-name to priority map from the NPML_ORDER variable."""
  prefs = os.getenv('NPML_ORDER', 'torch,np,jax,tf').split(',')

  return {mod: len(prefs) - i for i, mod in enumerate(prefs)}


# Numpy
try:
  import numpy as np

  def _np_from(mod, t, tref):
    if mod is not None:
      if mod is torch or mod is tfnp:
        return t.numpy()

    return np.asarray(t)

  def _np_check(t):
    return isinstance(t, np.ndarray)

  _register('np', np, _np_check, _np_from, {
      'item': lambda t: t.item(),
      'tolist': lambda t: t.tolist(),
  })
except ImportError:
  np = None

# PyTorch
try:
  import torch
  from torch.utils import dlpack as torch_dlpack

  def _torch_from(mod, t, tref):
    if mod is not None:
      if mod is np:
        return torch.from_numpy(t).to(tref.device)
      if mod is jaxnp:
        return torch_dlpack.from_dlpack(jax_dlpack.to_dlpack(t)).to(tref.device)
      if mod is tfnp:
        return torch_dlpack.from_dlpack(tf_dlpack.to_dlpack(t)).to(tref.device)

    return torch.tensor(t).to(tref.device)

  def _torch_check(t):
    return isinstance(t, torch.Tensor)

  _register('torch', torch, _torch_check, _torch_from, {
      'item': lambda t: t.item(),
      'tolist': lambda t: t.tolist(),
  })
except ImportError:
  torch = None

# JAX
try:
  import jax
  import jax.numpy as jaxnp
  from jax import dlpack as jax_dlpack

  def _jaxdev(t):
    return next(iter(t.devices()))

  def _jax_from(mod, t, tref):
    if mod is not None:
      if mod is torch:
        return jax.device_put(jax_dlpack.from_dlpack(torch_dlpack.to_dlpack(t)),
                              _jaxdev(tref))
      if mod is tfnp:
        return jax.device_put(jax_dlpack.from_dlpack(tf_dlpack.to_dlpack(t)),
                              _jaxdev(tref))

    return jax.device_put(jaxnp.asarray(t), _jaxdev(tref))

  def _jax_check(t):
    return isinstance(t, jax.Array)

  _register('jax', jaxnp, _jax_check, _jax_from, {
      'item': lambda t: t.item(),
      'tolist': lambda t: t.tolist(),
  })
except ImportError:
  jaxnp = None

# Tensorflow
try:
  import tensorflow as tf
  import tensorflow.experimental.dlpack as tf_dlpack
  import tensorflow.experimental.numpy as tfnp

  def _tf_from(mod, t, tref):
    if mod is not None:
      if mod is torch:
        with tf.device(tref.device):
          return tf_dlpack.from_dlpack(torch_dlpack.to_dlpack(t))
      if mod is jaxnp:
        with tf.device(tref.device):
          return tf_dlpack.from_dlpack(jax_dlpack.to_dlpack(t))

    with tf.device(tref.device):
      return tf.convert_to_tensor(t)

  def _tf_check(t):
    return tf.is_tensor(t)

  tfnp.experimental_enable_numpy_behavior()
  _register('tf', tfnp, _tf_check, _tf_from, {
      'item': lambda t: t.item(),
      'tolist': lambda t: t.tolist(),
  })
except ImportError:
  tfnp = None

_DEFAULT_MODULE = os.getenv('NPML_DEFAULT', 'np')

if _DEFAULT_MODULE not in _MODULES:
  raise RuntimeError(f'Unable to find default Numpy ML module: {_DEFAULT_MODULE}')

_MODULES_PRIORITY = _parse_priorities()
_MODULES_SEQ = tuple(_MODULES[x] for x in sorted(
    list(_MODULES.keys()), key=lambda m: _MODULES_PRIORITY.get(m, -1), reverse=True))


def _get_module(t):
  """Return the registered module owning a value.

  Args:
    t: Value to check against the registered framework check functions.

  Returns:
    The module whose check function accepts the value, or None.
  """
  for mod in _MODULES_SEQ:
    if mod.__npml_check(t):
      return mod


def resolve(*args):
  """Normalize arguments to the arrays of the highest-priority framework.

  Each argument is checked against the registered frameworks. The framework
  with the highest priority (as per NPML_ORDER) having at least one argument
  becomes the target, and every argument belonging to a different framework
  is converted to it using the framework conversion function.

  Args:
    *args: Arrays from any of the supported frameworks (or other objects,
      which are passed through unchanged).

  Returns:
    A tuple with the resolved target module and the tuple of converted
    arguments. If no argument matches any framework, the default module
    (NPML_DEFAULT) is returned together with the original arguments.
  """
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

  if tmod is None:
    return _MODULES[_DEFAULT_MODULE], args

  rargs = list(args)
  for i, mod in enumerate(mods):
    if tmod is not mod:
      rargs[i] = tmod.__npml_from(mod, args[i], tref)

  return tmod, tuple(rargs)
