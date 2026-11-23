"""Enumeration utilities for creating enum-like classes.

This module provides _EnumBase, a base class for creating simple enum-like
objects with named integer values, and make_enum() for dynamically creating
enum classes at runtime.
"""

import re
import types


class _EnumBase:
  """Base class for creating simple enum-like objects with named integer values.

  Fields are specified as a comma-separated string or a list of field names,
  optionally with explicit integer values. Values are auto-incremented starting
  from base, and can be accessed as attributes on the instance.

  Example:
    Color = _EnumBase('RED, GREEN=5, BLUE')
    # Color.RED == 0, Color.GREEN == 5, Color.BLUE == 6

  Attributes:
    _first: The first (base) value assigned.
    _last: The last value assigned.
    _values: Dict mapping field names to their integer values.
    _names: Dict mapping integer values to field names.
  """

  def __init__(self, fields, base=0):
    """Initialize the enum with field names and optional starting value.

    Args:
      fields: A comma-separated string of field names, or a list of field
        names. Each field can be a plain name, a (name, value) tuple, or
        'NAME=value' string.
      base: The starting integer value for auto-incremented fields.
        Defaults to 0.
    """
    enfields = re.split(r'\s*,\s*', fields) if isinstance(fields, str) else fields

    next_id, values = base, dict()
    for field in enfields:
      if isinstance(field, (list, tuple)):
        fname, fvalue = field
      else:
        m = re.match(r'(\w+)\s*=\s*(\d+)', field)
        if m:
          fname, fvalue = m.group(1), int(m.group(2))
        else:
          fname, fvalue = field, next_id

      assert fvalue >= next_id, f'{fvalue} < {next_id}'

      setattr(self, fname, fvalue)
      values[fname] = fvalue
      next_id = max(fvalue, next_id) + 1

    self._first = base
    self._last = max(base, next_id - 1)
    self._values = values
    self._names = {evalue: fname for fname, evalue in values.items()}

  def __len__(self):
    """Return the number of enum fields.

    Returns:
      The count of defined enum values.
    """
    return len(self._values)

  def __repr__(self):
    """Return a string representation of the enum.

    Returns:
      A string like 'ClassName(NAME1=0, NAME2=1, ...)'.
    """
    fields = [f'{k}={v}' for k, v in self._values.items()]

    return self.__class__.__name__ + '(' + ', '.join(fields) + ')'


def make_enum(name, fields, base=0):
  """Create a new enum class dynamically with the given name and fields.

  Args:
    name: The name for the new enum class.
    fields: A comma-separated string or list of field names/values.
    base: The starting integer value for auto-incremented fields.
      Defaults to 0.

  Returns:
    A new _EnumBase subclass instance with the specified fields.
  """
  cls = types.new_class(name, bases=(_EnumBase,))

  return cls(fields, base=base)
