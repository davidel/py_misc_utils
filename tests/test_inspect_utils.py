"""Tests for py_misc_utils.inspect_utils.

Covers qualified-name resolution, caller-frame helpers, argument
extraction from locals, and class slot enumeration.
"""

import pytest

from py_misc_utils import inspect_utils as iu
from py_misc_utils import traceback as tb

# ---------------------------------------------------------------------------
# classof / moduleof / cname / func_name
# ---------------------------------------------------------------------------


class _Base:
  pass


class _Derived(_Base):
  pass


def test_classof():
  obj = _Derived()
  assert iu.classof(obj) is _Derived
  assert iu.classof(_Derived) is _Derived


def test_classof_none():
  assert iu.classof(None) is type(None)


def test_classof_no_class_attr():

  class _Weird:
    __class__ = property(lambda self: None)

  obj = _Weird()
  assert iu.classof(obj) is None


class _NoCls:

  def __getattribute__(self, name):
    if name == '__class__':
      raise AttributeError(name)

    return object.__getattribute__(self, name)


def test_classof_missing_class_attr():
  obj = _NoCls()
  assert iu.classof(obj) is None


def test_moduleof_cname():
  assert iu.moduleof(_Derived()) == __name__
  assert iu.moduleof(_Derived) == __name__
  assert iu.cname(_Derived()) == '_Derived'
  assert iu.cname(_Derived) == '_Derived'
  assert iu.cname(_Derived()) is not None


def test_func_name():

  def _fn():
    pass

  assert iu.func_name(_fn) == '_fn'
  assert iu.func_name(_Derived) == '_Derived'
  assert iu.func_name(len) == 'len'
  assert iu.func_name(_Derived()) == '_Derived'


def test_moduleof_none():
  # None is not a class, so classof() resolves NoneType, whose module is
  # 'builtins' and whose name is 'NoneType'.
  assert iu.moduleof(None) == 'builtins'
  assert iu.cname(None) == 'NoneType'


def test_qual_name_and_mro():
  assert iu.qual_name(_Derived) == f'{__name__}._Derived'
  assert iu.qual_name(_Derived()) == f'{__name__}._Derived'
  assert iu.qual_name(len, builtin_strip=True) == 'len'
  assert iu.qual_name(len) == 'builtins.len'
  assert iu.qual_name(_Base) == f'{__name__}._Base'


def test_qual_mro():
  names = list(iu.qual_mro(_Derived))
  assert names == [
      f'{__name__}._Derived',
      f'{__name__}._Base',
      'builtins.object',
  ]
  stripped = list(iu.qual_mro(_Derived, builtin_strip=True))
  assert stripped[-1] == 'object'
  assert stripped[:-1] == names[:-1]


def test_is_subclass():
  assert iu.is_subclass(_Derived, _Base)
  assert not iu.is_subclass(_Base, _Derived)
  assert iu.is_subclass(_Derived, (_Base, dict))
  assert not iu.is_subclass(3, _Base)
  assert not iu.is_subclass(_Derived, int)


# ---------------------------------------------------------------------------
# Caller-frame helpers
# ---------------------------------------------------------------------------


def _caller_fn(back=0):
  return iu.get_caller_function(back=back)


def _caller_fn_host():
  return _caller_fn(back=1)


def test_get_caller_function_back0():
  fn = _caller_fn()
  assert fn is _caller_fn


def test_get_caller_function_back1():
  fn = _caller_fn_host()
  assert fn is _caller_fn_host


def test_get_caller_function_explicit_frame():
  frame = _frame_grabber()
  fn = iu.get_caller_function(frame=frame)
  assert fn is _frame_grabber


def _frame_grabber():
  return tb.get_frame(0)


def _current_module_host():
  return iu.current_module()


def test_current_module():
  mod = _current_module_host()
  assert mod is not None
  assert mod.__name__ == __name__


def _parent_locals_host():
  keeper = 42

  # parent_locals(level=0) skips the direct caller, so the grabber must be
  # nested one level below the host for the host's frame to be observed.
  def _grab():
    return iu.parent_locals()

  return keeper, _grab()


def test_parent_locals():
  _keeper, locs = _parent_locals_host()
  assert locs.get('keeper') == 42


def _parent_globals_host():
  return iu.parent_globals()


def test_parent_globals():
  glb = _parent_globals_host()
  assert glb.get('__name__') == __name__


def _parent_coords_host():
  return iu.parent_coords()


def test_parent_coords():
  fname, lineno = _parent_coords_host()
  assert fname.endswith('test_inspect_utils.py')
  # parent_coords() reports the frame TWO levels up from itself: here that is
  # this test function's own first statement (the call into the host).
  assert lineno == test_parent_coords.__code__.co_firstlineno + 1


# ---------------------------------------------------------------------------
# fetch_args / fetch_call
# ---------------------------------------------------------------------------


def _fa(a, b, c=30):
  return a, b, c


def _fa_pos_only(a, b, /, c=30):
  return a, b, c


def test_fetch_args_full():
  args, kwargs = iu.fetch_args(_fa, {'a': 1, 'b': 2, 'c': 3})
  assert args == [1, 2]
  assert kwargs == {'c': 3}


def test_fetch_args_from_input_args():
  args, kwargs = iu.fetch_args(_fa, {'b': 2, 'c': 3}, input_args=(1,))
  assert args == [1, 2]
  assert kwargs == {'c': 3}


def test_fetch_args_missing_required():
  with pytest.raises(RuntimeError, match='Missing argument: a'):
    iu.fetch_args(_fa, {'b': 2, 'c': 3})


def test_fetch_args_missing_optional_kept_default():
  args, kwargs = iu.fetch_args(_fa, {'a': 1, 'b': 2})
  assert args == [1, 2]
  assert kwargs == {'c': 30}


def test_fetch_args_none_is_a_value():
  args, kwargs = iu.fetch_args(_fa, {'a': 1, 'b': None})
  assert args == [1, None]
  # 'c' is absent from locals, so it is assigned its declared default.
  assert kwargs == {'c': 30}


def test_fetch_args_positional_only():
  args, kwargs = iu.fetch_args(_fa_pos_only, {'a': 1, 'b': 2, 'c': 5})
  assert args == [1, 2]
  assert kwargs == {'c': 5}


def test_fetch_call():
  assert iu.fetch_call(_fa, {'a': 10, 'b': 20, 'c': 3}) == (10, 20, 3)
  assert iu.fetch_call(_fa, {'b': 20}, input_args=(10,)) == (10, 20, 30)


# ---------------------------------------------------------------------------
# get_fn_kwargs / get_defaulted_params / class_slots
# ---------------------------------------------------------------------------


def _gk(self, x, y=5, **extra):
  pass


def _gk_kwonly(*, x, y=10):
  pass


def _gk_req(a, b, c=3):
  pass


def test_get_fn_kwargs_basic():
  # Only defaulted params (at/after the defaults boundary) are collected;
  # required params are skipped unless roffset covers them, and **kwargs
  # entries are not gathered.
  assert iu.get_fn_kwargs({'self': 1, 'x': 2, 'y': 3, 'z': 4}, _gk) == {'y': 3}


def test_get_fn_kwargs_prefix():
  assert iu.get_fn_kwargs({'cfg.x': 2, 'cfg.y': 7}, _gk, prefix='cfg') == {'y': 7}


def test_get_fn_kwargs_roffset():
  res = iu.get_fn_kwargs({'a': 1, 'b': 2, 'c': 9}, _gk_req, roffset=1)
  assert res == {'b': 2, 'c': 9}


def test_get_fn_kwargs_roffset_missing():
  with pytest.raises(AssertionError, match='must be present'):
    iu.get_fn_kwargs({'a': 1, 'c': 9}, _gk_req, roffset=1)


def test_get_fn_kwargs_kwonly():
  assert iu.get_fn_kwargs({'x': 1}, _gk_kwonly) == {'x': 1, 'y': 10}


def test_get_defaulted_params():

  def _fn(a, b=2, *, c=3):
    pass

  names = [p.name for p in iu.get_defaulted_params(_fn)]
  assert names == ['b', 'c']


def test_get_defaulted_params_none():

  def _fn(a, b):
    pass

  assert iu.get_defaulted_params(_fn) == ()


def test_class_slots():

  class _A:
    __slots__ = ('x',)

  class _B(_A):
    __slots__ = ('y', 'z')

  class _C(_B):
    pass

  # Inherited __slots__ remains visible through a slotless subclass, so the
  # parent's slots reappear per ancestor level (pinned as intended).
  assert iu.class_slots(_C) == ('y', 'z', 'y', 'z', 'x')
  assert iu.class_slots(_A) == ('x',)
  assert iu.class_slots(dict) == ()
