from . import utils as ut

_KW_TEMPLATE = '''
class $CLASS:

  def __init__(self, $KEY, $VALUE):
    self.$KEY = $KEY
    self.$VALUE = $VALUE

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

  def __str__(self):
    return f'$KEY="{$KEY}", $VALUE="{$VALUE}"'
'''


def key_wrap(cname, key_name, value_name):
  replaces = dict(CLASS=cname, KEY=key_name, VALUE=value_name)

  results = ut.compile(_KW_TEMPLATE, (cname,), vals=replaces)

  return results[0]

