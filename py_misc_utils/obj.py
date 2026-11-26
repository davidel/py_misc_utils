"""Object base class with dict-like attribute access and recursive serialization.

The Obj class provides a convenient container where attributes can be set via
keyword arguments and accessed like dictionary keys. It supports recursive
conversion to plain dicts via as_dict(), shallow cloning, and equality
comparison based on attribute contents.

Classes:
  Obj: A flexible object container with dict-like attribute access.

Functions:
  str_value: Formats a value for string representation, escaping strings.
"""

import copy

from . import core_utils as cu


class Obj:
  """A flexible object container with dict-like attribute access.

  Attributes set via keyword arguments are stored in ``__dict__`` and can be
  accessed normally. Supports recursive conversion to plain dicts, shallow
  cloning with overrides, and equality comparison based on attribute contents.
  """

  def __init__(self, **kwargs):
    """Initializes an Obj with the given keyword attributes.

    Args:
      **kwargs: Attribute names and values to set on the instance.
    """
    self.update(**kwargs)

  def update(self, **kwargs):
    """Updates the object's attributes from keyword arguments.

    Args:
      **kwargs: Attribute names and values to set or override.

    Returns:
      ``self`` for chaining.
    """
    vars(self).update(kwargs)

    return self

  def update_from(self, obj):
    """Copies all attributes from another object into this one.

    Args:
      obj: An object whose ``__dict__`` contents will be copied.

    Returns:
      ``self`` for chaining.
    """
    vars(self).update(vars(obj))

    return self

  def clone(self, **kwargs):
    """Returns a shallow copy of this object with optional attribute overrides.

    Args:
      **kwargs: Attribute names and values to override on the copy.

    Returns:
      A new ``Obj`` instance that is a shallow copy of this object with the
      specified attributes updated.
    """
    nobj = copy.copy(self)
    nobj.update(**kwargs)

    return nobj

  def as_dict(self):
    """Recursively converts this object and nested Obj instances to a plain dict.

    Nested Obj instances, lists/tuples containing Obj instances, and dicts
    containing Obj instances are all recursively converted.

    Returns:
      A plain dictionary representation of this object's attributes.
    """
    ad = dict()
    for k, v in vars(self).items():
      if isinstance(v, Obj):
        v = v.as_dict()
      elif isinstance(v, (list, tuple)):
        vals = []
        for x in v:
          if isinstance(x, Obj):
            x = x.as_dict()
          vals.append(x)

        v = type(v)(vals)
      elif cu.isdict(v):
        vd = type(v)()
        for z, x in v.items():
          if isinstance(x, Obj):
            x = x.as_dict()
          vd[z] = x

        v = vd

      ad[k] = v

    return ad

  def __eq__(self, other):
    """Compares this Obj with another for equality based on attributes.

    Two Obj instances are equal if they have the same set of attributes with
    equal values.

    Args:
      other: The object to compare against.

    Returns:
      True if ``other`` has the same attributes with equal values, False
      otherwise.
    """
    missing = object()
    for k, v in vars(self).items():
      ov = getattr(other, k, missing)
      if ov is missing or v != ov:
        return False
    for k in vars(other):
      if not hasattr(self, k):
        return False

    return True

  def __repr__(self):
    """Returns a string representation of the object.

    Returns:
      A string like ``ClassName(key1=value1, key2=value2)`` with string values
      quoted and escaped.
    """
    values = ', '.join(f'{k}={str_value(v)}' for k, v in vars(self).items())

    return f'{type(self).__name__}({values})'


def str_value(v):
  """Formats a value for string representation.

  String values are quoted and have internal quotes escaped. Non-string values
  are converted via ``str()``.

  Args:
    v: The value to format.

  Returns:
    A string representation of ``v``.
  """
  return '"' + v.replace('"', '\\"') + '"' if isinstance(v, str) else str(v)
