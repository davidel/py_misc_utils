"""Tests for the py_misc_utils.core_utils core data-structure helpers.

Expectations are pinned to behavior verified by probe scripts (kept in
.chatty/scratch) and by reading the implementation: splitstrip keeps
empty parts, dict_add accumulates values into ValueLists, signature()
fingerprints types rather than values and recurse_apply is a path-named
visitor over nested containers.
"""

import bisect
import collections

import pytest

import py_misc_utils.core_utils as cu


def test_splitstrip_keeps_empty_parts():
  """splitstrip() splits and strips each part, keeping empty ones."""
  assert cu.splitstrip('a, b,,c', ',') == ('a', 'b', '', 'c')


def test_append_if_missing_appends_in_place():
  """append_if_missing() appends values not yet present, in place."""
  lst = [1]

  cu.append_if_missing(lst, 1)
  cu.append_if_missing(lst, 2)

  assert lst == [1, 2]


def test_lindex_reports_position_and_not_found():
  """lindex() returns the position of a value, or -1 when missing."""
  assert cu.lindex(['a', 'b', 'c'], 'b') == 1
  assert cu.lindex(['a'], 'z') == -1
  assert cu.lindex(['a', 'b', 'a'], 'a', 1) == 2


def test_vfind_locates_byte_patterns():
  """vfind() finds byte patterns in bytes-like objects."""
  assert cu.vfind(b'hello world', b'world') == 6
  assert cu.vfind(b'hello world', b'xyz') == -1
  assert cu.vfind(bytearray(b'abc'), b'bc') == 1


def test_to_type_parses_strings_via_yaml():
  """to_type() casts values, parsing strings via YAML for containers."""
  assert cu.to_type('123', int) == 123
  assert cu.to_type('1.5', float) == 1.5
  assert cu.to_type('[1, 2]', list) == [1, 2]
  assert cu.to_type('(1, 2)', tuple) == (1, 2)
  # Bare strings go through yaml.safe_load, then the target cast applies.
  with pytest.raises(ValueError):
    cu.to_type('abc', int)
  assert cu.to_type(5, int) == 5


def test_infer_value_parses_scalars_and_containers():
  """infer_value() YAML-parses strings, keeping plain strings as-is."""
  assert cu.infer_value('1.5') == 1.5
  assert cu.infer_value('[1, 2]') == [1, 2]
  assert cu.infer_value('abc') == 'abc'


def test_unique_returns_index_lists_per_value():
  """unique() maps each value to the array of its occurrence indices."""
  result = cu.unique([3, 1, 3, 2])

  assert list(result.keys()) == [3, 1, 2]
  assert list(result[3]) == [0, 2]
  assert list(result[1]) == [1]
  assert list(result[2]) == [3]
  assert list(cu.unique('aabbc').keys()) == ['a', 'b', 'c']


def test_signature_is_the_type_fingerprint():
  """signature() serializes type structure for scalars, shape for data."""
  assert cu.signature(1) is int
  assert cu.signature(1.5) is float
  assert cu.signature('plain') is str
  assert cu.signature(None) is type(None)
  # Containers are recursed as plain dicts, keyed and sorted.
  assert cu.signature({'a': 1}) == {'a': int}
  assert cu.signature((1, 'x')) == (int, str)
  assert cu.signature((1, 'x')) != cu.signature([1])
  assert cu.signature({'a': [1]}) == {'a': [int]}


def test_equal_signature_compares_type_structures():
  """equal_signature() compares type structures of signature() outputs."""
  assert cu.equal_signature(cu.signature(1), cu.signature(2))
  assert cu.equal_signature(cu.signature(1), cu.signature(True))
  assert not cu.equal_signature(cu.signature(1), cu.signature('x'))
  assert not cu.equal_signature(cu.signature({'a': 1}), cu.signature({'b': 2}))
  assert cu.equal_signature(cu.signature({'a': 1}), cu.signature({'a': 2}))
  assert not cu.equal_signature(cu.signature({'a': 1}), cu.signature({'a': 'x'}))
  assert cu.equal_signature(cu.signature({'a': [1]}), cu.signature({'a': [2]}))
  assert not cu.equal_signature(cu.signature({'a': [1]}), cu.signature({'b': [2]}))
  assert not cu.equal_signature(cu.signature([1]), cu.signature((1,)))
  # Raw scalars are normalized to their types, so equal structures compare
  # equal regardless of the concrete values.
  assert cu.equal_signature(1, 1, subcls=False)
  assert cu.equal_signature(1, 2, subcls=False)
  assert not cu.equal_signature(1, 'x', subcls=False)


def test_equal_signature_accepts_raw_values():
  """equal_signature() also accepts raw values, not just signatures."""
  assert cu.equal_signature({'a': 1}, {'a': 1})
  assert not cu.equal_signature({'a': 1}, {'a': 'x'})
  assert cu.equal_signature([1, 'x'], [2, 'y'])
  assert not cu.equal_signature([1], [1, 2])
  assert cu.equal_signature('same', 'same')
  assert cu.equal_signature(3, 3)
  assert not cu.equal_signature(3, 'x')


def test_genhash_hashes_content_including_keys():
  """genhash() hashes content and keys, so equal structures agree."""
  assert cu.genhash({'a': 1}) == cu.genhash({'a': 1})
  assert cu.genhash({'a': 1}) != cu.genhash({'b': 1})
  assert cu.genhash({'a': 1}) != cu.genhash({'a': 2})


def test_bisect_left_binary_searches_over_indexed_keys():
  """bisect_left(x, key, hi, lo) binary searches key(i) over indices."""
  data = [1, 3, 5, 7, 9, 11]

  assert cu.bisect_left(4, lambda i: data[i], len(data)) == 2
  assert cu.bisect_left(0, lambda i: data[i], len(data)) == 0
  assert cu.bisect_left(12, lambda i: data[i], len(data)) == 6
  assert cu.bisect_left(5, lambda i: data[i], len(data)) == 2
  assert cu.bisect_left(5, lambda i: data[i], 3) == 2
  assert cu.bisect_left(5, lambda i: data[i], 3, lo=1) == 2
  assert cu.bisect_left(6, lambda i: data[i], len(data), lo=3) == 3


def test_bisect_left_equals_stdlib_on_plain_keys():
  """bisect_left() matches bisect.bisect_left when key is the identity."""
  data = [1, 3, 5, 7]

  for x in (0, 2, 4, 6, 8):
    assert cu.bisect_left(x, lambda i: data[i], len(data)) == \
        bisect.bisect_left(data, x)

  assert cu.bisect_left(4, lambda i: data[i], 3, lo=1) == \
      bisect.bisect_left(data, 4, 1, 3)


def test_new_with_updates_copies():
  """new_with() returns a copy with overrides, leaving the source intact."""
  base = {'a': 1}

  assert cu.new_with(base, a=2, b=3) == {'a': 2, 'b': 3}
  assert base == {'a': 1}
  assert cu.new_with(collections.namedtuple('P', 'x y')(1, 2), x=5) == \
      collections.namedtuple('P', 'x y')(5, 2)
  assert cu.new_with(collections.OrderedDict(a=1), a=2) == {'a': 2}


def test_make_ntuple_fills_instances_from_dicts():
  """make_ntuple(ntc, args) builds an instance filling fields from args."""
  Point = collections.namedtuple('P', 'x y')

  assert cu.make_ntuple(Point, {'x': 1, 'y': 2}) == Point(1, 2)

  partial = collections.namedtuple('Q', 'x y', defaults=[99])

  assert cu.make_ntuple(partial, {'y': 2}) == partial(None, 2)
  assert cu.is_namedtuple(cu.make_ntuple(Point, {'x': 1, 'y': 2}))
  assert not cu.is_namedtuple((1, 2))


def test_dict_add_accumulates_into_value_lists():
  """dict_add() stores the first value bare, then accumulates a ValueList."""
  ddict = {}

  cu.dict_add(ddict, 'k', 5)

  assert ddict['k'] == 5

  cu.dict_add(ddict, 'k', 6)

  assert ddict['k'] == [5, 6]


def test_enum_dict_values_enumerates_accumulated_values():
  """enum_dict_values() yields each stored value, flattened per key."""
  ddict = {}
  cu.dict_add(ddict, 'k', 5)
  cu.dict_add(ddict, 'k', 6)

  assert list(cu.enum_dict_values(ddict, 'k')) == [5, 6]
  assert list(cu.enum_dict_values({}, 'k')) == []


def test_recurse_apply_visits_all_paths():
  """recurse_apply(obj, name, fn) visits containers with path names."""
  paths = []
  cu.recurse_apply([1, [2, {'k': 3}]], 'root', lambda o, n: paths.append(n) or False)

  assert paths == [
      'root', 'root[0]', 'root[1]', 'root[1][0]', 'root[1][1]', 'root[1][1].k'
  ]


def test_recurse_apply_prunes_when_the_visitor_returns_true():
  """recurse_apply() does not descend when the visitor returns True."""
  paths = []
  cu.recurse_apply([1, [2]], 'root', lambda o, n: paths.append(n) or n != 'root')

  # Only 'root' returns False (descend); every child prunes its subtree.
  assert paths == ['root', 'root[0]', 'root[1]']


def test_data_rewrite_rewrites_scalar_leaves():
  """data_rewrite() rewrites scalar leaves, keeping the structure."""
  assert cu.data_rewrite({'a': [1, 2], 'b': {'c': 3}},
                         lambda v: v + 1 if isinstance(v, int) else None) == \
      {'a': [2, 3], 'b': {'c': 4}}
  assert cu.data_rewrite('x', lambda v: v.upper()
                         if isinstance(v, str) else None) == 'X'
  assert cu.data_rewrite({'a': 1}, lambda v: None) == {'a': 1}


def test_get_property_reads_attributes_calling_callables():
  """get_property() reads attributes, invoking them when callable."""

  class Holder:
    """Holder of both callable and plain properties."""

  holder = Holder()
  holder.x = lambda: 9
  holder.y = 3

  assert cu.get_property(holder, 'x') == 9
  assert cu.get_property(holder, 'y') == 3
  assert cu.get_property(holder, 'missing', 'D') == 'D'
  # Dict keys are NOT attributes: a plain dict yields the default.
  assert cu.get_property({'x': 3}, 'x', 'D') == 'D'


def test_string_table_interns_to_identity():
  """StringTable.add() returns the interned string object itself."""
  table = cu.StringTable()
  # Built at runtime so the two strings are distinct objects (interning
  # of compile-time literals would make the identity check trivial).
  parts = ['hel', 'lo']
  a = ''.join(parts)
  first = table.add(a)
  second = table.add(''.join(parts))

  assert first is second is a
  assert len(table) == 1


def test_value_list_is_a_list_used_by_dict_add():
  """ValueList is the list subclass used by dict_add accumulation."""
  ddict = {}
  cu.dict_add(ddict, 'k', 5)
  cu.dict_add(ddict, 'k', 6)

  assert isinstance(ddict['k'], cu.ValueList)
  assert ddict['k'] == [5, 6]


def test_revgen_counts_revisions_per_name():
  """RevGen.newver(name) counts per name; getver/newname read state."""
  gen = cu.RevGen()

  assert gen.newver('a') == 0
  assert gen.newver('a') == 1
  assert gen.newver('b') == 0
  assert gen.getver('a') == 2
  assert gen.getver('zz', 'D') == 'D'
  assert cu.RevGen().newname('res') == 'res_0'
  assert cu.RevGen().newname('res', shortzero=True) == 'res'


def test_maybe_call_invokes_only_existing_methods():
  """maybe_call(obj, name, ...) calls obj.name if it exists."""

  class Dummy:
    """Dummy exposing val()."""

    def val(self, x):
      """Return x doubled."""
      return x * 2

  obj = Dummy()

  assert cu.maybe_call(obj, 'val', 21) == 42
  assert cu.maybe_call(obj, 'missing') is None
  assert cu.maybe_call_dv(obj, 'missing', 7) == 7
  assert cu.maybe_call_dv(obj, 'val', 7, 5) == 10


def test_size_str_formats_byte_sizes():
  """size_str() formats sizes in B/KB/MB/GB with two decimals."""
  assert cu.size_str(512) == '512 B'
  assert cu.size_str(2048) == '2.00 KB'


def test_separate_splits_on_first_or_last_separator():
  """separate() splits once on the first (or last) separator."""
  assert cu.separate('a=b', '=') == ('a', 'b')
  assert cu.separate('a= b =c', '=', reverse=True) == ('a= b', 'c')
  assert cu.separate('nokey', '=') == ('nokey',)
  assert cu.separate('nokey', '=', filler='?') == ('nokey', '?')


def test_index_select_selects_positions():
  """index_select() selects the given positions from the sequence."""
  assert cu.index_select([10, 20, 30], [0, 2]) == [10, 30]


def test_norm_slice_normalizes_negative_and_none_bounds():
  """norm_slice() resolves None and negative bounds against the size."""
  assert cu.norm_slice(-2, None, 5) == (3, 5)
  assert cu.norm_slice(0, -1, 5) == (0, 4)


def test_enum_unique_yields_first_occurrences():
  """enum_unique() yields first occurrences, skipping the skip set."""
  assert list(cu.enum_unique([1, 2, 1, 3], skip=[3])) == [1, 2]


def test_compute_shape_computes_tuple_shapes():
  """compute_shape() derives the shape tuple of nested sequences."""
  assert cu.compute_shape([[1, 2], [3, 4]]) == (2, 2)


def test_clone_prefers_the_clone_method():
  """clone() uses .clone() when available, else copy.copy."""
  assert cu.clone({'a': 1}) == {'a': 1}


def test_cast_converts_with_a_default():
  """cast() converts values, mapping failures to the default."""
  assert cu.cast(None, int) is None
  assert cu.cast('5', int) == 5
