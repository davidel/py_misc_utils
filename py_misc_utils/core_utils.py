"""Core utility APIs with no local dependencies.

This module provides general-purpose helper functions and classes for common
operations such as object cloning, sequence manipulation, dictionary merging,
signature comparison, revision tracking, and type casting. These utilities have
no dependencies on other py_misc_utils modules.

Classes:
  RevDb: Tracks version numbers for named resources.

Functions:
  ident, noop, object_context, args, is_builtin_function, isdict,
  refcount, denone, vfind, seqfirst, iter_next, enum_values,
  idx_expand, is_sequence, expand, range_split, partition, is_iterator,
  as_iterator, dmerge, dict_extract, str_to_type, cast, infer_value,
  splitstrip, separate, root_module, parent_module, ns_lookup,
  index_select, lindex, append_if_missing, size_str, maybe_call,
  maybe_call_dv, unique, enum_max, enum_bits, signature, equal_signature,
  genhash, equal, build_object, clone, is_namedtuple, new_with,
  make_ntuple, splice_iter, get_property, data_rewrite
"""

# NOTE: This module is for APIs which has no local dependecies!
import array
import collections
import copy
import inspect
import re
import sys
import threading
import types

import yaml

from . import assert_checks as tas

_NONE = object()


def ident(x):
  """Returns the input value unchanged.

  Args:
    x: Any object to return as-is.

  Returns:
    The same object passed as ``x``.
  """
  return x


def noop(*args, **kwargs):
  """Does nothing, accepts any arguments.

  Useful as a placeholder callback or default function.
  """


def object_context(sobj, **kwargs):
  """Creates a SimpleNamespace from an object's attributes plus extra kwargs.

  Args:
    sobj: An object with a ``__dict__`` attribute.
    **kwargs: Additional key-value pairs to merge into the namespace.

  Returns:
    A ``types.SimpleNamespace`` combining the object's attributes and kwargs.
  """
  ctx_args = vars(sobj).copy()
  ctx_args.update(**kwargs)

  return types.SimpleNamespace(**ctx_args)


def args(*uargs, **kwargs):
  """Returns positional and keyword arguments as a tuple.

  Args:
    *uargs: Positional arguments.
    **kwargs: Keyword arguments.

  Returns:
    A tuple of ``(uargs, kwargs)``.
  """
  return uargs, kwargs


def is_builtin_function(obj):
  """Checks whether an object is a built-in function type.

  Args:
    obj: The object to check.

  Returns:
    True if ``obj`` is a ``types.BuiltinFunctionType``, False otherwise.
  """
  return isinstance(obj, types.BuiltinFunctionType)


def isdict(value):
  """Checks whether a value is a mapping type.

  Args:
    value: The object to check.

  Returns:
    True if ``value`` is a ``collections.abc.Mapping``, False otherwise.
  """
  return isinstance(value, collections.abc.Mapping)


def refcount(obj):
  """Returns the reference count of an object, adjusted for temporary frames.

  Args:
    obj: The object whose reference count is requested.

  Returns:
    The reference count minus 2 (to account for the internal call frame
    and ``sys.getrefcount``'s own reference).
  """
  # Discard 2 frame references (our own, and the sys.getrefcount() one).
  return sys.getrefcount(obj) - 2


def denone(**kwargs):
  """Returns kwargs filtered to exclude None values.

  Args:
    **kwargs: Keyword arguments to filter.

  Returns:
    A dict containing only entries whose value is not None.
  """
  return {k: v for k, v in kwargs.items() if v is not None}


def vfind(mem, b):
  """Searches a memoryview/bytes for the first occurrence of a byte pattern.

  Args:
    mem: A bytes-like object to search within.
    b: The byte pattern to search for.

  Returns:
    The index of the first match, or -1 if not found.
  """
  # Hmmm, a memoryview() should really have a find() API...
  m = re.search(b, mem)

  return m.start() if m is not None else -1


def seqfirst(s):
  """Returns the first element of an iterable.

  Args:
    s: An iterable object.

  Returns:
    The first item yielded by ``iter(s)``.

  Raises:
    StopIteration: If the iterable is empty.
  """
  return next(iter(s))


def iter_next(it, defval=None):
  """Returns the next item from an iterator, or a default if exhausted.

  Args:
    it: An iterator object.
    defval: Value to return when the iterator is exhausted (default None).

  Returns:
    The next item from ``it``, or ``defval`` if StopIteration is raised.
  """
  try:
    return next(it)
  except StopIteration:
    return defval


def enum_values(obj):
  """Yields (key, value) pairs from a mapping or object attributes.

  Args:
    obj: A mapping (dict-like) or an object with attributes.

  Yields:
    Tuples of ``(key, value)`` for each entry in the object.
  """
  if isdict(obj):
    for k, v in obj.items():
      yield k, v
  else:
    for k, v in vars(obj):
      yield k, v


def idx_expand(data, idx, filler=None):
  """Ensures a list is long enough to index at position ``idx``.

  Args:
    data: A list to expand in place if needed.
    idx: The target index that must be valid after expansion.
    filler: Value used to fill new slots (default None).

  Returns:
    The (possibly expanded) ``data`` list.
  """
  if idx >= len(data):
    data = data + [filler] * (idx + 1 - len(data))

  return data


_SEQUENCE_TYPES = (list, tuple, types.GeneratorType)


def is_sequence(v):
  """Checks whether a value is a sequence type (list, tuple, or generator).

  Args:
    v: The object to check.

  Returns:
    True if ``v`` is a list, tuple, or generator type, False otherwise.
  """
  return isinstance(v, _SEQUENCE_TYPES)


def expand(data, n):
  """Returns data as-is if it is a sequence, otherwise repeats it n times.

  Args:
    data: A value or sequence to check.
    n: Number of repetitions if ``data`` is not a sequence.

  Returns:
    ``data`` if it is a sequence, otherwise a tuple of ``data`` repeated n times.
  """
  return data if is_sequence(data) else (data,) * n


def range_split(n, split, minsize, reverse=False):
  """Splits a range [0, n) into chunks of size ``split``.

  Args:
    n: Total size of the range.
    split: Chunk size.
    minsize: Minimum size for the last chunk; if smaller, it is merged.
    reverse: If True, returns chunks in reverse order.

  Returns:
    A tuple of (start, end) tuples describing each chunk.
  """
  splits = list(range(0, n, split))
  if len(splits) > 1 and (n - splits[-1]) < minsize:
    splits.pop()

  rsplits = []
  for i, base in enumerate(splits):
    top = splits[i + 1] if (i + 1) < len(splits) else n
    rsplits.append((base, top))

  return tuple(reversed(rsplits)) if reverse else tuple(rsplits)


def partition(data, n):
  """Partitions data into n interleaved parts.

  Args:
    data: A sequence to partition.
    n: Number of parts.

  Returns:
    A tuple of n tuples, where part i contains elements at indices i, i+n, i+2n, ...
  """
  parts = []
  for i in range(n):
    parts.append(tuple(data[j] for j in range(i, len(data), n)))

  return tuple(parts)


def is_iterator(obj):
  """Checks whether an object is an iterator (has __iter__ and __next__).

  Args:
    obj: The object to check.

  Returns:
    True if ``obj`` is an iterator, False otherwise.
  """
  return hasattr(obj, '__iter__') and hasattr(obj, '__next__')


def as_iterator(obj):
  """Returns obj if it is an iterator, otherwise wraps it in a single-item iterator.

  Args:
    obj: An object to convert to an iterator.

  Returns:
    ``obj`` if it is already an iterator, otherwise ``iter([obj])``.
  """
  return obj if is_iterator(obj) else iter([obj])


def dmerge(*args):
  """Merges multiple dictionaries into one.

  Args:
    *args: Dictionaries to merge. Later dictionaries override earlier ones.

  Returns:
    A single dict containing all key-value pairs from the inputs.
  """
  mdict = dict()
  for d in args:
    mdict.update(d)

  return mdict


def dict_extract(d, prefix=None, rx=None):
  """Extracts entries from a dict whose keys match a regex pattern.

  Args:
    d: The source dictionary.
    prefix: A string prefix used to build the regex if ``rx`` is not given.
    rx: An optional regex pattern with one capture group. If None, defaults
      to ``'{prefix}(.*)'``.

  Returns:
    A new dict with captured groups as keys and original values as values.
  """
  if rx is None:
    rx = f'{prefix}(.*)'
  xd = dict()
  for k, v in d.items():
    m = re.match(rx, k)
    if m:
      xd[m.group(1)] = v

  return xd


def dget(sdict, name, defval, dtype=None):
  """Gets a value from a dict with optional type coercion.

  Args:
    sdict: The source dictionary.
    name: The key to look up.
    defval: Default value returned if the key is missing.
    dtype: Optional type to cast the value to. If None and defval is not None,
      the type of defval is used.

  Returns:
    The value for ``name`` (cast to dtype if applicable), or ``defval`` if
    the key is missing.
  """
  v = sdict.get(name, _NONE)
  if v is _NONE:
    return defval

  if dtype is None and defval is not None:
    dtype = type(defval)

  return dtype(v) if v is not None and dtype is not None else v


def to_type(v, vtype):
  """Casts a value to a given type, parsing strings via YAML when needed.

  Args:
    v: The value to cast.
    vtype: The target type (e.g., int, float, list, tuple).

  Returns:
    The value cast to ``vtype``.
  """
  if isinstance(v, str):
    if vtype in (list, tuple):
      m = re.match(r'\s*\((.*)\)\s*$', v)
      if m:
        v = f'[{m.group(1)}]'
      elif not re.match(r'\s*\[.*\]\s*$', v):
        v = f'[{v}]'

    v = yaml.safe_load(v)

  return vtype(v)


def cast(v, vtype):
  """Casts a value to a type, returning None if the value is None.

  Args:
    v: The value to cast.
    vtype: The target type.

  Returns:
    The value cast to ``vtype``, or None if ``v`` is None.
  """
  return to_type(v, vtype) if v is not None else None


def infer_value(v, vtype=None):
  """Infers and casts a value using YAML parsing or an explicit type.

  Args:
    v: The value to interpret.
    vtype: Optional explicit type to cast to. If None, YAML parsing is used.

  Returns:
    The parsed or cast value.
  """
  return yaml.safe_load(v) if vtype is None else to_type(v, vtype)


def splitstrip(data, sep):
  """Splits a string by a separator and strips whitespace from each part.

  Args:
    data: The string to split.
    sep: The separator string.

  Returns:
    A tuple of stripped substrings.
  """
  return tuple(s.strip() for s in data.split(sep))


def separate(data, sep, reverse=False, filler=_NONE):
  """Splits a string at the first (or last) occurrence of a separator.

  Args:
    data: The string to split.
    sep: The separator substring.
    reverse: If True, splits at the last occurrence (rfind). Otherwise splits
      at the first (find).
    filler: Value returned as the second element if sep is not found. If _NONE
      and sep is not found, returns only the stripped string.

  Returns:
    A tuple ``(before, after)`` with stripped parts. If sep is not found and
    filler is _NONE, returns ``(stripped_data,)``.
  """
  pos = data.rfind(sep) if reverse else data.find(sep)
  if pos < 0:
    if filler is not _NONE:
      return data.strip(), filler
    else:
      return data.strip(),

  return data[:pos].strip(), data[pos + 1:].strip()


def root_module(modname):
  """Returns the root (top-level) module name from a dotted module path.

  Args:
    modname: A dotted module name string.

  Returns:
    The first component before the first dot.
  """
  return separate(modname, '.')[0]


def parent_module(modname):
  """Returns the parent module name from a dotted module path.

  Args:
    modname: A dotted module name string.

  Returns:
    The module name without the last component after the final dot.
  """
  return separate(modname, '.', reverse=True)[0]


def ns_lookup(key, mappings):
  """Looks up a dotted key across a sequence of namespaces.

  Args:
    key: A dot-separated key string.
    mappings: A sequence of dict-like or object namespaces to search.

  Returns:
    The value found in the first namespace where the full key path resolves,
    or None if not found in any namespace.
  """
  kparts = key.split('.')
  for ns in mappings:
    for part in kparts:
      if isdict(ns):
        ns = ns.get(part)
      else:
        ns = getattr(ns, part, None)
      if ns is None:
        break

    if ns is not None:
      return ns


def index_select(arr, idx):
  """Selects elements from an array using a slice or list of indices.

  Args:
    arr: A sequence to index into.
    idx: A slice object or a list/tuple of integer indices.

  Returns:
    The sliced subsequence, or a list of selected elements.
  """
  return arr[idx] if isinstance(idx, slice) else [arr[i] for i in idx]


def lindex(l, e, start=0, end=None):
  """Returns the index of an element in a list, or -1 if not found.

  Args:
    l: The list to search.
    e: The element to find.
    start: The starting index (inclusive).
    end: The ending index (exclusive). If None, searches to the end.

  Returns:
    The index of the first occurrence of ``e``, or -1 if not found.
  """
  try:
    return l.index(e, start, end if end is not None else len(l))
  except ValueError:
    return -1


def append_if_missing(arr, elem):
  """Appends an element to a list if it is not already present.

  Args:
    arr: The list to modify in place.
    elem: The element to append if missing.
  """
  if elem not in arr:
    arr.append(elem)


def size_str(size):
  """Formats a byte size into a human-readable string.

  Args:
    size: The size in bytes.

  Returns:
    A string like ``'1024 KB'`` or ``'5.00 MB'`` using the largest unit
    that keeps the value under 1024.
  """
  syms = ('B', 'KB', 'MB', 'GB', 'TB')

  for i, sym in enumerate(syms):
    if size < 1024:
      return f'{size} {sym}' if i == 0 else f'{size:.2f} {sym}'

    size /= 1024

  return f'{size * 1024:.2f} {syms[-1]}'


def maybe_call(obj, name, *args, **kwargs):
  """Calls a method on an object if it exists and is callable.

  Args:
    obj: The object to call the method on.
    name: The method name.
    *args: Positional arguments for the method.
    **kwargs: Keyword arguments for the method.

  Returns:
    The result of the method call, or None if the method does not exist
    or is not callable.
  """
  fn = getattr(obj, name, None)

  return fn(*args, **kwargs) if callable(fn) else None


def maybe_call_dv(obj, name, defval, *args, **kwargs):
  """Calls a method on an object if it exists and is callable, with a default.

  Args:
    obj: The object to call the method on.
    name: The method name.
    defval: The default value returned if the method is missing or not callable.
    *args: Positional arguments for the method.
    **kwargs: Keyword arguments for the method.

  Returns:
    The result of the method call, or ``defval`` if the method is not available.
  """
  fn = getattr(obj, name, None)

  return fn(*args, **kwargs) if callable(fn) else defval


def unique(data):
  """Groups indices of equal values in a sequence.

  Args:
    data: A sequence of hashable values.

  Returns:
    A defaultdict mapping each unique value to an array.array of indices
    where that value occurs.
  """
  udata = collections.defaultdict(lambda: array.array('L'))
  for i, v in enumerate(data):
    udata[v].append(i)

  return udata


def enum_max(cls):
  """Returns the maximum value of an enum class.

  Args:
    cls: An enum class (iterable of values).

  Returns:
    The largest value in ``cls``.
  """
  return max(x for x in cls)


def enum_bits(cls):
  """Returns the number of bits needed to represent the max enum value.

  Args:
    cls: An enum class.

  Returns:
    The bit length of ``enum_max(cls)``.
  """
  return enum_max(cls).bit_length()


def signature(v):
  """Computes a structural signature of a value for comparison purposes.

  Args:
    v: A value (dict, list, tuple, or scalar).

  Returns:
    A normalized representation: dicts become sorted dicts of signatures,
    lists/tuples become typed sequences of signatures, scalars become their
    type.
  """
  if isdict(v):
    vdata = dict()
    for k in sorted(v.keys()):
      vdata[k] = signature(v[k])

    return vdata
  elif isinstance(v, (list, tuple)):
    vdata = [signature(e) for e in v]

    return type(v)(vdata)

  return type(v)


def equal_signature(a, b, subcls=True):
  """Compares two structural signatures for equality.

  Args:
    a: First signature/value to compare.
    b: Second signature/value to compare.
    subcls: If True, uses issubclass checks for scalars (allows subclass
      matches). If False, uses exact equality.

  Returns:
    True if the signatures are structurally equal, False otherwise.
  """
  if isdict(a):
    if not isdict(b) or len(a) != len(b):
      return False

    for k, t in a.items():
      tb = b.get(k)
      if tb is None or not equal_signature(t, tb, subcls=subcls):
        return False

    return True
  elif isinstance(a, (list, tuple)):
    if type(a) != type(b) or len(a) != len(b):
      return False

    for ea, eb in zip(a, b):
      if not equal_signature(ea, eb, subcls=subcls):
        return False

    return True

  a = a if isinstance(a, type) else signature(a)
  b = b if isinstance(b, type) else signature(b)

  return issubclass(a, b) or issubclass(b, a) if subcls else a == b


def genhash(v):
  """Computes a hash for nested data structures (dicts, lists, tuples, scalars).

  Args:
    v: A value to hash (dict, list, tuple, or scalar).

  Returns:
    An integer hash incorporating type and nested structure.
  """
  if isdict(v):
    vdata = []
    for k in sorted(v.keys()):
      vdata.append(genhash(k))
      vdata.append(genhash(v[k]))

    return hash((type(v), tuple(vdata)))
  elif isinstance(v, (list, tuple)):
    vdata = [genhash(e) for e in v]

    return hash((type(v), tuple(vdata)))

  return hash((type(v), v))


def norm_slice(start, stop, size):
  """Normalizes slice start/stop indices, handling None and negative values.

  Args:
    start: The start index (None or negative allowed).
    stop: The stop index (None or negative allowed).
    size: The sequence size used to resolve negative indices.

  Returns:
    A tuple ``(start, stop)`` with normalized non-negative indices.
  """
  if start is None:
    start = 0
  elif start < 0:
    start = size + start
  if stop is None:
    stop = size
  elif stop < 0:
    stop = size + stop

  return start, stop


def run_async(fn, *args, **kwargs):
  """Runs a function in a daemon thread and returns the thread object.

  Args:
    fn: The callable to run asynchronously.
    *args: Positional arguments for fn.
    **kwargs: Keyword arguments for fn.

  Returns:
    The started ``threading.Thread`` instance (daemon=True).
  """
  thread = threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
  thread.start()

  return thread


def bisect_right(x, key, hi, lo=0):
  """Binary search: returns rightmost insertion point for x in key-sorted range.

  Args:
    x: The target value.
    key: A function mapping index to comparable value.
    hi: Upper bound (exclusive) of the search range.
    lo: Lower bound (inclusive) of the search range (default 0).

  Returns:
    The index where x would be inserted to maintain order (right side).
  """
  tas.check_ge(lo, 0)
  while lo < hi:
    mid = (lo + hi) // 2
    if x < key(mid):
      hi = mid
    else:
      lo = mid + 1

  return lo


def bisect_left(x, key, hi, lo=0):
  """Binary search: returns leftmost insertion point for x in key-sorted range.

  Args:
    x: The target value.
    key: A function mapping index to comparable value.
    hi: Upper bound (exclusive) of the search range.
    lo: Lower bound (inclusive) of the search range (default 0).

  Returns:
    The index where x would be inserted to maintain order (left side).
  """
  tas.check_ge(lo, 0)
  while lo < hi:
    mid = (lo + hi) // 2
    if key(mid) < x:
      lo = mid + 1
    else:
      hi = mid

  return lo


def rewrited_exception(ex, msg):
  """Rewrites an exception message while preserving the original traceback.

  Args:
    ex: The original exception instance.
    msg: Additional message text to append.

  Returns:
    A new exception of the same class with the combined message and original
    traceback attached.
  """
  xmsg = f'{ex}{msg}'

  return ex.__class__(xmsg).with_traceback(ex.__traceback__)


def obj_from_dict(cls, data):
  """Constructs an object of cls from a dictionary of attribute values.

  Args:
    cls: The class to instantiate.
    data: A dict mapping parameter names to values.

  Returns:
    A new instance of ``cls`` constructed from ``data``.
  """
  args, kwargs = [], dict()
  slots = getattr(cls, '__slots__', None)
  if slots is not None:
    kwargs = {k: data.get(k) for k in slots}
  else:
    sig = inspect.signature(cls)

    for n, p in sig.parameters.items():
      if p.kind == p.POSITIONAL_ONLY:
        args.append(data.get(n))
      else:
        kwargs[n] = data.get(n)

  return cls(*args, **kwargs)


def clone(obj):
  """Returns a shallow copy of an object, using its clone() method if available.

  Args:
    obj: The object to clone.

  Returns:
    A shallow copy of ``obj``.
  """
  clone_fn = getattr(obj, 'clone', None)

  return clone_fn() if callable(clone_fn) else copy.copy(obj)


def is_namedtuple(obj):
  """Checks whether an object is a named tuple.

  Args:
    obj: The object to check.

  Returns:
    True if obj is a tuple with _asdict and _fields attributes.
  """
  return isinstance(obj, tuple) and hasattr(obj, '_asdict') and hasattr(obj, '_fields')


def new_with(obj, **kwargs):
  """Returns a copy of an object with specified attributes updated.

  Args:
    obj: The object to copy and modify.
    **kwargs: Attributes to set on the copy.

  Returns:
    A copy of ``obj`` with the given attributes updated. Named tuples use
    ``_replace``, dicts use ``update``, other objects use setattr.
  """
  if is_namedtuple(obj):
    return obj._replace(**kwargs)

  nobj = copy.copy(obj)
  if isdict(nobj):
    nobj.update(kwargs)
  else:
    for k, v in kwargs.items():
      setattr(nobj, k, v)

  return nobj


def make_ntuple(ntc, args):
  """Creates a named tuple instance from a dict of field values.

  Args:
    ntc: The named tuple class.
    args: A dict mapping field names to values. Missing fields use defaults.

  Returns:
    A new instance of ``ntc``.
  """
  targs = []
  for f in ntc._fields:
    fv = args.get(f, _NONE)
    if fv is _NONE:
      fv = ntc._field_defaults.get(f)

    targs.append(fv)

  return ntc._make(targs)


class StringTable:
  """Interns strings, returning a canonical instance for each unique string.

  The same string object is returned for repeated calls with equal content,
  enabling fast identity comparison.
  """

  def __init__(self):
    """Initializes an empty string table."""
    self._tbl = dict()

  def __len__(self):
    """Return the number of interned strings."""
    return len(self._tbl)

  def add(self, s):
    """Interns a string, returning the canonical instance.

    Args:
      s: The string to intern.

    Returns:
      The canonical string instance for ``s``.
    """
    x = self._tbl.get(s)
    if x is None:
      self._tbl[s] = x = s

    return x


class ValueList(list):
  """A list subclass used to hold multiple values under a single dict key.

  Used by dict_add to accumulate values when the same key is added multiple times.
  """


def dict_add(ddict, name, value):
  """Adds a value to a dict, accumulating multiple values in a ValueList.

  Args:
    ddict: The dictionary to modify.
    name: The key under which to store the value.
    value: The value to add. If the key already exists, the value is appended
      to a ValueList (creating one if needed).
  """
  ivalue = ddict.get(name, _NONE)
  if ivalue is not _NONE:
    if isinstance(ivalue, ValueList):
      ivalue.append(value)
    else:
      ddict[name] = ValueList((ivalue, value))
  else:
    ddict[name] = value


def dict_update_append(d, **kwargs):
  """Updates a dict with multiple values, accumulating via dict_add.

  Args:
    d: The dictionary to update.
    **kwargs: Key-value pairs to add using dict_add semantics.
  """
  for k, v in kwargs.items():
    dict_add(d, k, v)


def enum_dict_values(ddict, name):
  """Yields all values associated with a key, including accumulated ValueList entries.

  Args:
    ddict: The dictionary to search.
    name: The key whose values should be enumerated.

  Yields:
    Each value stored under ``name``. If the value is a ValueList, yields
    each element; otherwise yields the single value.
  """
  ivalue = ddict.get(name, _NONE)
  if ivalue is not _NONE:
    if isinstance(ivalue, ValueList):
      for value in ivalue:
        yield value
    else:
      yield ivalue


def enum_unique(data, skip=None):
  """Yields unique elements from an iterable, optionally skipping a set.

  Args:
    data: An iterable of hashable items.
    skip: An optional iterable of items to skip (default empty).

  Yields:
    Each unique item from ``data`` that is not in the skip set.
  """
  uskip = set(skip or ())
  for x in data:
    if x not in uskip:
      uskip.add(x)
      yield x


def get_property(obj, name, defval=None):
  """Gets an attribute from an object, calling it if it is callable.

  Args:
    obj: The object to query.
    name: The attribute name.
    defval: Default value returned if the attribute is missing (default None).

  Returns:
    The attribute value (called if callable), or ``defval`` if missing.
  """
  p = getattr(obj, name, _NONE)
  if p is _NONE:
    return defval

  return p() if callable(p) else p


def data_rewrite(v, rwfn):
  """Recursively rewrites a data structure using a rewrite function.

  Args:
    v: The value to rewrite (scalar, list, tuple, dict, or object with __dict__).
    rwfn: A function taking a value and returning a replacement, or None to
      keep the original.

  Returns:
    The rewritten structure with the same types as the input. If no changes
    were made, the original object is returned unchanged.
  """
  rwv = rwfn(v)
  if rwv is not None:
    return rwv
  elif isinstance(v, (list, tuple)):
    new_values, rewritten = [], False
    for x in v:
      new_obj = data_rewrite(x, rwfn)
      new_values.append(new_obj)
      rewritten = rewritten or new_obj is not x

    return type(v)(new_values) if rewritten else v
  elif isdict(v):
    new_values, rewritten = [], False
    for k, x in v.items():
      new_k = data_rewrite(k, rwfn)
      new_x = data_rewrite(x, rwfn)
      new_values.append((new_k, new_x))
      rewritten = rewritten or new_k is not k or new_x is not x

    return type(v)(new_values) if rewritten else v
  elif hasattr(v, '__dict__'):
    new_values, rewritten = [], False
    for k, x in v.__dict__.items():
      new_k = data_rewrite(k, rwfn)
      new_x = data_rewrite(x, rwfn)
      new_values.append((new_k, new_x))
      rewritten = rewritten or new_k is not k or new_x is not x

    if rewritten:
      new_obj = copy.copy(v)
      new_obj.__dict__.update(**dict(new_values))

      return new_obj

    return v
  else:
    return v


def recurse_apply(obj, name, fn):
  """Recursively applies a function to all elements of a nested structure.

  Args:
    obj: The object to traverse (list, tuple, dict, or object with __dict__).
    name: A path string used to identify the current element (for fn context).
    fn: A callable taking (obj, name) and returning True to stop recursion
      at this node, or False to continue into children.
  """
  if not fn(obj, name):
    if isinstance(obj, (list, tuple)):
      for i, v in enumerate(obj):
        recurse_apply(v, f'{name}[{i}]', fn)
    elif isdict(obj):
      for k, v in obj.items():
        recurse_apply(v, f'{name}.{k}', fn)
    elif hasattr(obj, '__dict__'):
      for k, v in obj.__dict__.items():
        recurse_apply(v, f'{name}.{k}', fn)


def compute_shape(data):
  """Computes the shape of a nested data structure.

  Args:
    data: A nested list/tuple/object with an optional ``shape`` attribute.

  Returns:
    A tuple of dimension lengths. If data has a ``shape`` property, that is
    returned directly; otherwise the shape is computed by recursing into
    the first element.
  """
  sp = get_property(data, 'shape')
  if sp is not None:
    return tuple(sp)
  shape = []
  if hasattr(data, '__len__'):
    shape.append(len(data))
    if shape[0] > 0 and hasattr(data, '__getitem__'):
      shape.extend(compute_shape(data[0]))

  return tuple(shape)


class RevGen:
  """Generator of monotonically increasing revisions per named resource.

  Names are stored in an in-memory RevDb, so the revision numbers are
  process-local unless a user-provided database is given at init time.

  Args:
    fmt: Format string of the generated names (default: '{name}_{ver}').
    revbase: Starting revision number (default: 0).
  """

  def __init__(self, fmt=None, revbase=0):
    """Initializes a RevGen instance.

    Args:
      fmt: Format string for generated names. Defaults to '{name}_{ver}'.
      revbase: Starting version number (default 0).
    """
    self._fmt = fmt or '{name}_{ver}'
    self._revbase = revbase
    self._revdb = dict()

  def getver(self, name, defval=None):
    """Returns the current version number for a name.

    Args:
      name: The resource name.
      defval: Value returned if the name has no version recorded yet.

    Returns:
      The current version number for ``name``, or ``defval`` if not found.
    """
    return self._revdb.get(name, defval)

  def newver(self, name):
    """Increments and returns the version number for a name.

    Args:
      name: The resource name.

    Returns:
      The new (incremented) version number for ``name``.
    """
    ver = self._revdb.get(name, self._revbase)
    self._revdb[name] = ver + 1

    return ver

  def newname(self, name, shortzero=False):
    """Generates a versioned name for a resource.

    Args:
      name: The base resource name.
      shortzero: If True and the version is 0, returns just ``name`` without
        a version suffix.

    Returns:
      A formatted name using ``_fmt``, or just ``name`` if shortzero and
      version is 0.
    """
    ver = self.newver(name)

    return self._fmt.format(name=name, ver=ver) if ver != 0 or not shortzero else name
