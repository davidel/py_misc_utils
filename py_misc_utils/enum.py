import re
import types


class _EnumBase:

  def __init__(self, fields, base=0):
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
    self._last = base + len(enfields) - 1
    self._values = values
    self._names = {evalue: fname for fname, evalue in values.items()}

  def __len__(self):
    return len(self._values)

  def __repr__(self):
    fields = [f'{k}={v}' for k, v in self._values.items()]

    return self.__class__.__name__ + '(' + ', '.join(fields) + ')'


def make_enum(name, fields, base=0):
  cls = types.new_class(name, bases=(_EnumBase,))

  return cls(fields, base=base)

