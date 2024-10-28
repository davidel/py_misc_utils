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

