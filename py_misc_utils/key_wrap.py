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
  replaces = dict(CLASS=cname, KEY=key_name)

  results, = ut.compile(_KW_TEMPLATE, cname, vals=replaces)

  return results

