"""Extensions for namedtuple types.

This module provides utilities to derive an extended namedtuple from an
existing one, adding new fields while preserving the original fields and
their defaults.
"""

import collections
import re

from . import assert_checks as tas


def extend(base_nt, name, fields, defaults=None):
  """Create a new namedtuple type extending an existing one.

  The new type preserves the base fields (in order, with their defaults)
  and appends the extra fields. Fields can be added with or without
  defaults; adding a defaulted field after fields without defaults is
  rejected, since the resulting namedtuple would not be constructible
  positionally.

  Args:
    base_nt: Base namedtuple type to extend.
    name: Name of the new namedtuple type.
    fields: Field specification, as a space/comma separated string or an
      iterable of field names.
    defaults: Optional sequence of default values applied to the rightmost
      new fields, following the collections.namedtuple defaults semantics:
      the last len(defaults) fields get the given values in order.

  Returns:
    A new namedtuple type with the base and the extra fields.
  """
  if isinstance(fields, str):
    fields = re.split(r'\s*[ ,]\s*', fields)

  ext_fields, ext_def_fields, ext_defaults = [], [], []
  missing = object()
  for field in base_nt._fields:
    defval = base_nt._field_defaults.get(field, missing)
    if defval is missing:
      ext_fields.append(field)
    else:
      ext_def_fields.append(field)
      ext_defaults.append(defval)

  defaults = defaults or ()
  for i, field in enumerate(fields):
    defidx = i - (len(fields) - len(defaults))
    if defidx >= 0:
      tas.check(field not in ext_def_fields,
                msg=f'Field already exists: {field} in {ext_def_fields}')
      tas.check(field not in ext_fields,
                msg=f'Field already exists: {field} in {ext_fields}')
      ext_def_fields.append(field)
      ext_defaults.append(defaults[defidx])
    else:
      tas.check(field not in ext_fields,
                msg=f'Field already exists: {field} in {ext_fields}')
      ext_fields.append(field)

  return collections.namedtuple(name,
                                tuple(ext_fields + ext_def_fields),
                                defaults=tuple(ext_defaults))
