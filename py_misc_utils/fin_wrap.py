"""Finalizer wrapper for automatic cleanup of objects.

The fin_wrap class wraps an object stored on a parent object and registers a
weakref finalizer that calls a cleanup function when the wrapper is garbage
collected. This ensures resources are released when the wrapped object is no
longer referenced.

Classes:
  fin_wrap: Wraps an object on a parent with a weakref finalizer for cleanup.
  FinWrapper: Wraps an object with attribute delegation via fin_wrap.

Functions:
  _finalizer_name: Builds the attribute name for storing a fin_wrap.
  fin_wrap_np: Wraps an object in a simple namespace with a finalizer.
"""

import weakref

from . import assert_checks as tas


def _finalizer_name(name):
  """Builds the attribute name for storing a fin_wrap on a parent object.

  Args:
    name: The name of the wrapped attribute.

  Returns:
    A string like ``'_<name>_finalizer'``.
  """
  return f'__{name}_finalizer'


class fin_wrap:
  """Wrapper registering a weakref finalizer on a parent attribute.

  Args:
    parent: The object the wrapped attribute is stored on.
    name: The attribute name the wrapped object is stored under.
    obj: The object to wrap.
    finfn: A callable to invoke when the wrapper is garbage collected.
    cleanup: If True, the wrapped object is cleaned up when the wrapper
      attribute is removed before garbage collection.
  """

  def __init__(self, parent, name, obj, finfn=None, cleanup=False):
    """Initializes a fin_wrap and registers a finalizer.

    Args:
      parent: The object on which to store the wrapped object and finalizer.
      name: The attribute name on parent for the wrapped object.
      obj: The object to wrap. If None, no finalizer is registered.
      finfn: A callable to invoke when this wrapper is garbage collected.
      cleanup: If True and obj is None, clears the attribute from parent.
    """
    setattr(parent, name, obj)
    fname = _finalizer_name(name)
    if obj is not None:
      tas.check_is_not_none(finfn, msg=f'Missing finalize function argument')

      setattr(parent, fname, self)
      self._finalizer = weakref.finalize(self, finfn)
    else:
      fwrap = getattr(parent, fname, None)
      if fwrap is not None:
        delattr(parent, fname)
        if cleanup:
          fwrap._finalizer()
        else:
          fwrap._finalizer.detach()


class _Wrapper:
  """Trivial namespace object used as a parent by fin_wrap_np()."""
  pass


def fin_wrap_np(obj, finfn, name='v'):
  """Wrap an object into a fresh namespace object with a finalizer.

  Args:
    obj: The object to wrap.
    finfn: A callable to invoke when the wrapper is garbage collected.
    name: Attribute name under which the object is stored.

  Returns:
    The wrapper object holding the object and the finalizer.
  """
  wrapper = _Wrapper()
  fin_wrap(wrapper, name, obj, finfn=finfn)

  return wrapper


_OBJ_NAME = 'wrapped_obj'
_RESERVED_NAMES = {_OBJ_NAME, _finalizer_name(_OBJ_NAME)}


class FinWrapper:
  """Object wrapper delegating attribute access to the wrapped object.

  The wrapped object is stored under a reserved attribute name and the
  dunders below forward every other attribute access to it, so that the
  wrapper transparently behaves like the wrapped object while carrying
  a finalizer for cleanup.

  Args:
    obj: The object to wrap and delegate to.
    finfn: A callable to invoke when the wrapper is garbage collected.
  """

  def __init__(self, obj, finfn):
    """Initialize the wrapper, registering the finalizer.

    Args:
      obj: The object to wrap and delegate to.
      finfn: A callable to invoke when the wrapper is garbage collected.
    """
    fin_wrap(self, _OBJ_NAME, obj, finfn=finfn)

  def __getattribute__(self, name):
    """Return the attribute of the wrapped object."""
    pd = super().__getattribute__('__dict__')
    obj = pd[_OBJ_NAME]

    return getattr(obj, name)

  def __getattr__(self, name):
    """Return the missing attribute of the wrapped object."""
    pd = super().__getattribute__('__dict__')
    obj = pd[_OBJ_NAME]

    return getattr(obj, name)

  def __setattr__(self, name, value):
    """Set an attribute on the wrapper or on the wrapped object."""
    pd = super().__getattribute__('__dict__')
    if name in _RESERVED_NAMES:
      pd[name] = value
    else:
      obj = pd[_OBJ_NAME]

      setattr(obj, name, value)

  def __delattr__(self, name):
    """Delete an attribute of the wrapper or of the wrapped object."""
    pd = super().__getattribute__('__dict__')
    if name in _RESERVED_NAMES:
      pd.pop(name)
    else:
      obj = pd[_OBJ_NAME]

      delattr(obj, name)
