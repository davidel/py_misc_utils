import collections
import re

from . import assert_checks as tas


def extend(base_nt, name, fields, defaults=None):
  if isinstance(fields, str):
    fields = re.split(r'\s*[ ,]\s*', fields)

  base_fields, base_def_fields, base_defaults = [], [], []
  missing = object()
  for field in base_nt._fields:
    defval = base_nt._field_defaults.get(field, missing)
    if defval is missing:
      base_fields.append(field)
    else:
      base_def_fields.append(field)
      base_defaults.append(defval)

  defaults = defaults or ()
  for i, field in enumerate(fields):
    defidx = i - (len(fields) - len(defaults))
    if defidx >= 0:
      tas.check(field not in base_def_fields,
                msg=f'Field already exists: {field} in {base_def_fields}')
      tas.check(field not in base_fields,
                msg=f'Field already exists: {field} in {base_fields}')
      base_def_fields.append(field)
      base_defaults.append(defaults[defidx])
    else:
      assert field not in base_fields
      base_fields.append(field)

  return collections.namedtuple(name, tuple(base_fields + base_def_fields),
                                defaults=tuple(base_defaults))

