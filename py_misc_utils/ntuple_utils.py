import collections
import re

from . import assert_checks as tas


def extend(base_nt, name, fields, defaults=None):
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

  return collections.namedtuple(name, tuple(ext_fields + ext_def_fields),
                                defaults=tuple(ext_defaults))

