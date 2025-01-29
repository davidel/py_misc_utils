import array
import collections
import re
import struct
import types


class Record(types.SimpleNamespace):
  pass


class RecordArray:

  Field = collections.namedtuple('Field', 'fmt, size')

  def __init__(self, fields, asarray=False):
    rfields, rfmt = dict(), ''
    for name, fmt in fields.items():
      m = re.match(r'(\d+)', fmt)
      size = int(m.group(1)) if m else 1
      rfields[name] = self.Field(fmt=fmt, size=size)
      rfmt += fmt

    self._rfields = rfields
    self._fmt = rfmt
    self._asarray = asarray
    self._data = array.array('B')
    self._recsize = struct.calcsize(rfmt)

  def append(self, *args):
    values = []
    for arg in args:
      if hasattr(arg, '__iter__'):
        values.extend(arg)
      else:
        values.append(arg)

    self._data.extend(struct.pack(self._fmt, *values))

  def __len__(self):
    return len(self._data) // self._recsize

  def __getitem__(self, i):
    offset = i * self._recsize
    data = self._data[offset: offset + self._recsize]
    values = struct.unpack(self._fmt, data)

    rpos, result = 0, Record()
    for name, field in self._rfields.items():
      if field.size == 1 and not self._asarray:
        setattr(result, name, values[rpos])
      else:
        setattr(result, name, values[rpos: rpos + field.size])

      rpos += field.size

    return result

