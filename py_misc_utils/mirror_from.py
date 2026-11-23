"""Utility functions for mirroring attributes and methods between objects.

This module provides :func:`mirror_from` as a class decorator to forward
attribute access to a member object, along with :func:`mirror_attributes`,
:func:`mirror_all`, and :func:`unmirror` for copying and removing attributes
between object instances.
"""

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
  """Return a class decorator that mirrors methods from a named origin attribute.

  Args:
    origin_name: The name of the attribute on each instance that holds the
      origin object whose methods will be mirrored.
    methods: A list of method names to mirror onto the decorated class.

  Returns:
    A class decorator that sets each listed method as a property forwarding
    calls to the corresponding method on the origin attribute.
  """

  def origin_getter(method, obj):
    """Get the mirrored method of the origin object of an instance.

    Args:
      method: The name of the method to retrieve.
      obj: The instance whose origin attribute holds the origin object.

    Returns:
      The bound method of the origin object.
    """
    origin = getattr(obj, origin_name)

    return getattr(origin, method)

  def wrapper(cls):
    """Decorate a class, mirroring the requested methods.

    Args:
      cls: The class to decorate.

    Returns:
      The class itself, with one read-only property per mirrored method.
    """
    for method in methods:
      wrapped_method = functools.partial(origin_getter, method)
      setattr(cls, method, property(wrapped_method))

    return cls

  return wrapper


def mirror_attributes(src, dest, attributes):
  """Copy named attributes from one object to another.

  Args:
    src: The source object to read attributes from.
    dest: The destination object to set attributes on.
    attributes: A list of attribute names to copy.
  """
  for attr in attributes:
    setattr(dest, attr, getattr(src, attr))


def _mirrored_field(name):
  """Return the internal attribute name used to store mirrored attribute lists.

  Args:
    name: The identifier used to namespace the mirrored field.

  Returns:
    A string of the form '_mirrored_<name>'.
  """
  return f'_mirrored_{name}'


def mirror_all(src, dest, excludes=(), includes=(), name=None):
  """Mirror all public attributes from src to dest, optionally recording them.

  Args:
    src: The source object to read attributes from.
    dest: The destination object to set attributes on.
    excludes: Optional collection of attribute names to skip.
    includes: Optional collection of attribute names to include even if they
      start with an underscore.
    name: Optional identifier used to store the list of mirrored attributes
      on dest for later unmirroring.

  Returns:
    A tuple of attribute names that were mirrored.
  """
  excludes = set(excludes)
  for attr in dir(dest):
    if not attr.startswith('_'):
      excludes.add(attr)

  includes = set(includes)
  mirrored = []
  for attr in dir(src):
    if (not attr.startswith('_') or attr in includes) and attr not in excludes:
      setattr(dest, attr, getattr(src, attr))
      mirrored.append(attr)

  if name is not None:
    setattr(dest, _mirrored_field(name), tuple(mirrored))

  return tuple(mirrored)


def unmirror(dest, attributes=None, name=None):
  """Remove previously mirrored attributes from an object.

  Args:
    dest: The object to remove attributes from.
    attributes: Optional collection of attribute names to remove. If None and
      name is provided, the stored mirrored list is used.
    name: Optional identifier used to retrieve the stored mirrored attribute
      list from dest.
  """
  if name is not None:
    mfield = _mirrored_field(name)
    attributes = getattr(dest, mfield, None)
    delattr(dest, mfield)

  for attr in attributes or ():
    delattr(dest, attr)
