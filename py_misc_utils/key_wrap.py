"""Dynamically generate a wrapper class with rich comparison operators based on a key field.

This module provides the :func:`key_wrap` function which creates a new class
that stores a designated key attribute and implements all standard comparison
methods (__lt__, __le__, __gt__, __ge__, __eq__, __ne__) and __hash__ based
on that key. The generated class also includes a custom __repr__ that displays
the key value prominently.

Note that the generated comparison operators do not check the type of the
other operand: comparing against an unrelated type raises AttributeError
(or TypeError for __hash__-only operations) instead of returning
NotImplemented.

Example:
  from py_misc_utils import key_wrap

  PKey = key_wrap.key_wrap('PKey', 'id')
  a = PKey(id=1, name='x')
  b = PKey(id=1, name='y')

  # Instances compare and hash by 'id' only.
  assert a == b
  assert a < PKey(id=2)
  assert repr(a) == "id=[1] : {'name': 'x'}"
"""

from . import utils as ut

_KW_TEMPLATE = '''
class $CLASS:

  def __init__(self, $KEY, **kwargs):
    self.$KEY = $KEY
    for k, v in kwargs.items():
      setattr(self, k, v)

  def __lt__(self, other):
    return self.$KEY < other.$KEY

  def __le__(self, other):
    return self.$KEY <= other.$KEY

  def __gt__(self, other):
    return self.$KEY > other.$KEY

  def __ge__(self, other):
    return self.$KEY >= other.$KEY

  def __eq__(self, other):
    return self.$KEY == other.$KEY

  def __ne__(self, other):
    return self.$KEY != other.$KEY

  def __hash__(self):
    return hash(self.$KEY)

  def __repr__(self):
    args = self.__dict__.copy()
    args.pop('$KEY', None)

    return f'$KEY=[{self.$KEY}] : {args}'
'''


def key_wrap(cname, key_name):
  """Generate a wrapper class that compares instances by a designated key attribute.

  Args:
    cname: The name to give the generated class.
    key_name: The attribute name used as the key for comparisons and hashing.

  Returns:
    A newly created class whose instances compare by key_name and expose all
    remaining kwargs as instance attributes.
  """
  replaces = dict(CLASS=cname, KEY=key_name)

  results, = ut.compile(_KW_TEMPLATE, cname, vals=replaces)

  return results
