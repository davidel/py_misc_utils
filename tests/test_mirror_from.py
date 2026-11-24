"""Tests for py_misc_utils.mirror_from.

mirror_from is a class decorator forwarding attribute access to a member
object; mirror_attributes copies named attributes; mirror_all copies all
public attributes (with excludes/includes and optional bookkeeping for
unmirror); unmirror removes previously mirrored attributes.
"""

from py_misc_utils import mirror_from as mf


def test_mirror_forwards_calls():
  """Mirrored names forward to the origin object's methods."""

  class Origin:

    def read(self, size=-1):
      return b'data'[:size] if size >= 0 else b'data'

    def seek(self, pos):
      self.last_pos = pos
      return pos

  @mf.mirror_from('stream', ['read', 'seek'])
  class Wrapper:

    def __init__(self, stream):
      self.stream = stream

  origin = Origin()
  w = Wrapper(origin)

  assert w.read(2) == b'da'
  assert w.read() == b'data'
  assert w.seek(5) == 5
  assert origin.last_pos == 5


def test_mirror_is_property_bound_late():
  """The mirror resolves through the instance attribute at call time."""

  @mf.mirror_from('stream', ['value'])
  class Wrapper:

    def __init__(self, stream):
      self.stream = stream

  class A:

    def value(self):
      return 'A'

  class B:

    def value(self):
      return 'B'

  w = Wrapper(A())
  assert w.value() == 'A'
  w.stream = B()
  assert w.value() == 'B'


def test_mirror_attributes():
  """mirror_attributes copies only the requested attributes."""

  class Src:
    pass

  src = Src()
  src.alpha = 1
  src.beta = 2
  src.gamma = 3

  dest = Src()
  mf.mirror_attributes(src, dest, ['alpha', 'gamma'])

  assert dest.alpha == 1
  assert dest.gamma == 3
  assert not hasattr(dest, 'beta')


def test_mirror_all_public():
  """mirror_all copies src public attrs missing on dest; underscored skipped.

  dest's own public attributes are always excluded (never overwritten).
  """

  class Src:

    def __init__(self):
      self.one = 1
      self.two = 2
      self._hidden = 3

  class Dest:
    pass

  src = Src()
  dest = Dest()

  mirrored = mf.mirror_all(src, dest)

  assert mirrored == ('one', 'two')
  assert dest.one == 1
  assert dest.two == 2


def test_mirror_all_keeps_dest_public_attrs():
  """Attributes already public on dest are not overwritten."""

  class Src:

    def __init__(self):
      self.one = 1
      self.two = 2

  class Dest:

    def __init__(self):
      self.one = 'kept'

  src = Src()
  dest = Dest()

  mirrored = mf.mirror_all(src, dest)

  assert mirrored == ('two',)
  assert dest.one == 'kept'
  assert dest.two == 2


def test_mirror_all_excludes():
  """excludes keeps attributes off the destination."""

  class Src:

    def __init__(self):
      self.one = 1
      self.two = 2

  class Dest:
    pass

  src = Src()
  dest = Dest()

  mirrored = mf.mirror_all(src, dest, excludes=['two'])

  assert mirrored == ('one',)
  assert dest.one == 1
  assert not hasattr(dest, 'two')


def test_mirror_all_includes_underscored():
  """includes brings underscore-prefixed attributes over."""

  class Src:

    def __init__(self):
      self._special = 'x'
      self.normal = 'y'

  class Dest:
    pass

  src = Src()
  dest = Dest()

  mirrored = mf.mirror_all(src, dest, includes=['_special'])

  assert '_special' in mirrored
  assert dest._special == 'x'


def test_mirror_all_bookkeeping_and_unmirror():
  """The mirrored names can be recorded under a name and unmirrored."""

  class Src:

    def __init__(self):
      self.one = 1
      self.two = 2

  class Dest:
    pass

  src = Src()
  dest = Dest()

  mf.mirror_all(src, dest, name='batchb')

  assert dest.one == 1
  assert dest.two == 2

  mf.unmirror(dest, name='batchb')

  assert not hasattr(dest, 'one')
  assert not hasattr(dest, 'two')
  assert not hasattr(dest, '_mirrored_batchb')


def test_unmirror_explicit_list():
  """unmirror removes the explicitly listed attributes."""

  class Src:
    pass

  src = Src()
  src.a = 1
  src.b = 2

  dest = Src()
  mf.mirror_all(src, dest)

  mf.unmirror(dest, attributes=['a'])

  assert not hasattr(dest, 'a')
  assert dest.b == 2
