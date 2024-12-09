import functools


# Copied from fsstat.
# Used to forward calls from  a class, to a memeber of the class itself.
# Example:
#   @mirror_from(
#     'stream',
#     [
#        'read',
#        'seek',
#        'write',
#     ],
#   )
#   class MyStream:
#     def __init__(self, stream):
#       self.stream = stream
#
def mirror_from(origin_name, methods):

  def origin_getter(method, obj):
    origin = getattr(obj, origin_name)

    return getattr(origin, method)

  def wrapper(cls):
    for method in methods:
      wrapped_method = functools.partial(origin_getter, method)
      setattr(cls, method, property(wrapped_method))

    return cls

  return wrapper


def mirror_attributes(src, dest, attributes):
  for attr in attributes:
    setattr(dest, attr, getattr(src, attr))


def _mirrored_field(name):
  return f'_mirrored_{name}'


def mirror_all(src, dest, excludes=None, name=None):
  excludes = set(excludes or [])
  for attr in dir(dest):
    if not attr.startswith('_'):
      excludes.add(attr)

  mirrored = []
  for attr in dir(src):
    if not attr.startswith('_') and attr not in excludes:
      setattr(dest, attr, getattr(src, attr))
      mirrored.append(attr)

  if name is not None:
    setattr(dest, _mirrored_field(name), tuple(mirrored))

  return tuple(mirrored)


def unmirror(dest, attributes=None, name=None):
  if name is not None:
    mfield = _mirrored_field(name)
    attributes = getattr(dest, mfield, None)
    delattr(dest, mfield)

  for attr in attributes or ():
    delattr(dest, attr)

