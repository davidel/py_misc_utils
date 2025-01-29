import array
import collections
import types

from . import assert_checks as tas


class Record(types.SimpleNamespace):
  pass


class RecordArray:

  Info = collections.namedtuple('Info', 'size, typecode, keepdim',
                                defaults=(None, False,))
  Field = collections.namedtuple('Field', 'base, end, size, keepdim')

  def __init__(self, fields, typecode=None, keepdim=False):
    arrays = dict()
    for name, info in fields.items():
      if isinstance(info, int):
        size, ftypecode, fkeepdim = info, typecode, keepdim
      else:
        size, ftypecode, fkeepdim = info.size, info.typecode, info.keepdim

      tas.check_is_not_none(ftypecode, msg=f'Missing typecode for "{name}"')

      if (varray := arrays.get(ftypecode)) is None:
        varray = types.SimpleNamespace(array=array.array(ftypecode),
                                       offset=0,
                                       fields=dict())
        arrays[ftypecode] = varray

      varray.fields[name] = self.Field(base=varray.offset,
                                       end=varray.offset + size,
                                       size=size,
                                       keepdim=fkeepdim)
      varray.offset += size

    self._arrays = arrays
    self._count = 0

  def append(self, **kwargs):
    for varray in self._arrays.values():
      # Dictionaries are ordered in modern Python, so append/expand follows
      # the constructor order defined in the progressive offset field.
      for name, field in varray.fields.items():
        value = kwargs[name]
        if field.size == 1:
          varray.array.append(value)
        else:
          tas.check_eq(len(value), field.size, msg=f'Wrong value size')
          varray.array.extend(value)

    self._count += 1

  def __len__(self):
    return self._count

  def __getitem__(self, i):
    result = Record()
    for varray in self._arrays.values():
      offset = i * varray.offset

      for name, field in varray.fields.items():
        data = varray.array[offset + field.base: offset + field.end]
        setattr(result, name, data if len(data) > 1 or field.keepdim else data[0])

    return result

