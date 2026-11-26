"""Utility functions for common operations.

This module provides a collection of general-purpose utility functions including:
- String formatting and manipulation
- Dictionary and data structure operations
- Configuration file handling (YAML)
- Environment variable parsing
- Object creation and inspection
- Code compilation and execution helpers
- Shape and array utilities
- State persistence
"""

import array
import datetime
import math
import os
import pickle
import re
import time
import types

import numpy as np
import yaml

from . import alog
from . import assert_checks as tas
from . import core_utils as cu
from . import file_overwrite as fow
from . import gfs
from . import inspect_utils as iu
from . import mmap as mm
from . import obj
from . import split as sp
from . import template_replace as tr
from . import traceback as tb


class _None:
  """Sentinel class representing a 'none' value distinct from None."""

  def __repr__(self):
    """Return the NONE marker string."""
    return 'NONE'


_NONE = _None()


def pickle_proto():
  """Get the pickle protocol version from environment or use highest.

  Returns:
    int: Pickle protocol version (default: pickle.HIGHEST_PROTOCOL).
  """
  return getenv('PICKLE_PROTO', dtype=int, defval=pickle.HIGHEST_PROTOCOL)


def fname():
  """Get the name of the calling function.

  Returns:
    str: Name of the function that called this function.
  """
  return tb.get_frame(1).f_code.co_name


def _stri(obj, seen, ffmt, dexp):
  """Recursively convert an object to a string representation with cycle detection.

  Args:
    obj: Object to convert to string.
    seen: Dictionary mapping object IDs to their string representations (for cycle detection).
    ffmt: Float format string (e.g., '.3e').
    dexp: Whether to expand object __dict__.

  Returns:
    str: String representation of the object.
  """
  oid = id(obj)
  sres = seen.get(oid, _NONE)
  if sres is None:
    return '...'
  elif sres is not _NONE:
    return sres

  seen[oid] = None
  if isinstance(obj, str):
    obj_str = obj.replace('\"', '\\"')
    result = f'"{obj_str}"'
  elif isinstance(obj, float):
    result = f'{obj:{ffmt}}'
  elif isinstance(obj, bytes):
    result = obj.decode()
  elif cu.is_namedtuple(obj):
    result = str(obj)
  elif cu.is_sequence(obj):
    sl = ', '.join(_stri(x, seen, ffmt, dexp) for x in obj)

    result = '[' + sl + ']' if isinstance(obj, list) else '(' + sl + ')'
  elif cu.isdict(obj):
    result = '{' + ', '.join(
        f'{k}={_stri(v, seen, ffmt, dexp)}' for k, v in obj.items()) + '}'
  elif dexp and hasattr(obj, '__dict__'):
    # Drop the braces around the __dict__ output, and use the "Classname(...)" format.
    drepr = _stri(obj.__dict__, seen, ffmt, dexp)
    result = f'{iu.cname(obj)}({drepr[1: -1]})'
  else:
    result = str(obj)

  seen[oid] = result

  return result


def stri(l, float_fmt=None, dict_expand=False):
  """Convert an object to a detailed string representation.

  Args:
    l: Object to convert to string.
    float_fmt: Format string for floating point numbers (default: '.3e').
    dict_expand: Whether to expand object __dict__ attributes (default: False).

  Returns:
    str: Detailed string representation of the object.
  """
  return _stri(l, dict(), float_fmt or '.3e', dict_expand)


def repr_fmt(obj, *fields, repr_none=False, sep=', '):
  """Format an object's attributes into a string representation.

  Args:
    obj: Object whose attributes to format.
    *fields: Attribute names to include. Can use '!attr' to force inclusion
      even if None, or 'attr=name' to use a custom display name.
    repr_none: Whether to include attributes that are None (default: False).
    sep: Separator between fields (default: ', ').

  Returns:
    str: Formatted string representation.
  """
  parts = []
  for fname in expand_strings(*fields):
    rnone, xname = repr_none, fname

    m = re.match(r'([!])?(\w+)\s*(=\s*(\w*))?', fname)
    if m:
      fname = m.group(2)
      rnone = m.group(1) == '!'
      xname = m.group(4)
      if xname is None:
        xname = fname

    data = getattr(obj, fname, None)
    if data is not None or rnone:
      parts.append(f'{xname}={data}' if xname else str(data))

  return sep.join(parts)


def mget(d, *args, as_dict=False):
  """Get multiple keys from a dictionary.

  Args:
    d: Dictionary to query.
    *args: Keys to retrieve (can be comma-separated strings).
    as_dict: Return as dictionary instead of tuple (default: False).

  Returns:
    Tuple of values or dictionary mapping keys to values.
  """
  margs = expand_strings(*args)
  if as_dict:
    return {f: d.get(f) for f in margs}
  else:
    return tuple(d.get(f) for f in margs)


def getvar(obj, name, defval=None):
  """Get an attribute or dictionary key with default.

  Args:
    obj: A dictionary or an object supporting attribute access.
    name: Key or attribute name to retrieve.
    defval: Value returned when name is missing (default: None).

  Returns:
    The value of the key or attribute, or defval.
  """
  return obj.get(name, defval) if cu.isdict(obj) else getattr(obj, name, defval)


def dict_subset(d, *keys):
  """Build a new dictionary holding only the specified keys.

  Args:
    d: Dictionary to extract the keys from.
    *keys: Keys to extract; comma-separated strings are expanded.

  Returns:
    A new dictionary with the key/value pairs present in d.
  """
  mkeys = expand_strings(*keys)
  subd = dict()
  for k in mkeys:
    v = d.get(k, _NONE)
    if v is not _NONE:
      subd[k] = v

  return subd


def dict_setmissing(d, **kwargs):
  """Fill a dictionary with entries whose keys are still missing.

  Args:
    d: Dictionary to add the missing entries to.
    **kwargs: Candidate key/value pairs, used only when the key is not
      already present in d.

  Returns:
    The d dictionary updated in place.
  """
  kwargs.update(d)

  return kwargs


def pop_kwargs(kwargs, names, args_key=None):
  """Pop arguments from a kwargs dictionary, with an extra-arguments sidecar.

  Args:
    kwargs: Dictionary to pop the values from.
    names: Names of the arguments to retrieve; comma-separated strings
      are expanded.
    args_key: Name of an optional sidecar dictionary holding per-name
      values; when present it takes precedence over kwargs.

  Returns:
    A tuple with the retrieved values, None for the missing ones.
  """
  xargs = kwargs.pop(args_key or '_', None)
  if xargs is not None:
    args = [xargs.get(name) for name in expand_strings(names)]
  else:
    args = [kwargs.pop(name, None) for name in expand_strings(names)]

  return tuple(args)


def resplit(csstr, sep, unescape=False):
  """Split a string on a separator, trimming surrounding whitespace.

  Args:
    csstr: String to split.
    sep: Separator, treated as a regular expression.
    unescape: If True, remove escape characters from the parts.

  Returns:
    A tuple of the parts of the string. Empty parts between separators
    are preserved.
  """
  return sp.split(csstr, r'\s*' + sep + r'\s*', unescape=unescape)


def comma_split(csstr, unescape=False):
  """Split a string on commas, trimming surrounding whitespace.

  Args:
    csstr: String to split.
    unescape: If True, remove escape characters from the parts.

  Returns:
    A tuple of the parts of the string. Empty parts between separators
    are preserved.
  """
  return sp.split(csstr, r'\s*,\s*', unescape=unescape)


def ws_split(data, unescape=False):
  """Split a string on whitespace.

  Args:
    data: String to split.
    unescape: If True, remove escape characters from the parts.

  Returns:
    A tuple of the parts of the string. Empty parts between separators
    are preserved.
  """
  return sp.split(data, r'\s+', unescape=unescape)


def expand_strings(*args):
  """Expand comma-separated strings and sequences into a flat tuple of strings.

  Args:
    *args: Strings or sequences. Strings are split by commas, sequences are
      flattened. Whitespace is stripped.

  Returns:
    Tuple of non-empty strings.
  """
  margs = []
  for arg in args:
    if cu.is_sequence(arg):
      margs.extend(arg)
    else:
      margs.extend(comma_split(arg))

  return tuple(margs)


def name_values(base_name, values):
  """Create named value pairs from a base name and values.

  Args:
    base_name: Base name for the values.
    values: Single value or sequence of values.

  Returns:
    Tuple of (name, value) pairs. If multiple values, names are
    'base_name.0', 'base_name.1', etc.
  """
  names = []
  if isinstance(values, (list, tuple)):
    if len(values) == 1:
      names.append((base_name, values[0]))
    else:
      for i, v in enumerate(values):
        names.append((f'{base_name}.{i}', v))
  else:
    names.append((base_name, values))

  return tuple(names)


def write_config(cfg, dest, **kwargs):
  """Write a configuration dictionary to a YAML file atomically.

  Args:
    cfg: Configuration dictionary to write.
    dest: Destination file path.
    **kwargs: Additional arguments passed to yaml.dump().
  """
  default_flow_style = kwargs.get('default_flow_style', False)

  with fow.FileOverwrite(dest, mode='wt') as df:
    yaml.dump(cfg, df, default_flow_style=default_flow_style, **kwargs)


def config_to_string(cfg, **kwargs):
  """Convert a configuration dictionary to a YAML string.

  Args:
    cfg: Configuration dictionary.
    **kwargs: Additional arguments passed to yaml.dump().

  Returns:
    str: YAML representation of the configuration.
  """
  default_flow_style = kwargs.get('default_flow_style', False)

  return yaml.dump(cfg, default_flow_style=default_flow_style, **kwargs)


def parse_config(cfg):
  """Parse a configuration from YAML string or file.

  Args:
    cfg: YAML string, or path to a YAML file (detected by not starting with [ or {).

  Returns:
    Parsed configuration dictionary.
  """
  if not re.match(r'[\[\{]', cfg):
    # It must be either a dictionary in YAML format, or a valid path.
    with gfs.open(cfg, mode='r') as fd:
      data = fd.read()
  else:
    data = cfg

  return yaml.safe_load(data)


def load_config(path, extra=None):
  """Load configuration from a path, optionally merging extra key-value pairs.

  Args:
    path: Path to configuration file or YAML string.
    extra: Optional dictionary of extra key-value pairs to merge into config.

  Returns:
    Parsed configuration dictionary with extra keys merged.
  """
  cfgd = parse_config(path)

  if extra:
    for k, v in extra.items():
      if v is not None:
        cfgd[k] = v

  return cfgd


def fatal(msg, exc=RuntimeError):
  """Raise an exception with a formatted message.

  Args:
    msg: Error message.
    exc: Exception class to raise (default: RuntimeError).

  Raises:
    exc: The specified exception with the given message.
  """
  alog.xraise(exc, msg, stacklevel=2)


def assert_instance(msg, t, ta):
  """Assert that an object is an instance of a type (or one of several types).

  Args:
    msg: Error message prefix.
    t: Object to check.
    ta: Type or tuple of types to check against.

  Raises:
    ValueError: If t is not an instance of ta.
  """
  if not isinstance(t, ta):
    parts = [msg, f': {iu.cname(t)} is not ']
    if isinstance(ta, (list, tuple)):
      parts.append('one of (')
      parts.append(', '.join(iu.cname(x) for x in ta))
      parts.append(')')
    else:
      parts.append(f'a {iu.cname(ta)}')

    alog.xraise(ValueError, ''.join(parts))


def make_object(**kwargs):
  """Create a simple object with attributes from keyword arguments.

  Args:
    **kwargs: Attributes to set on the object.

  Returns:
    Obj: A simple object with the given attributes.
  """
  return obj.Obj(**kwargs)


def make_object_recursive(**kwargs):
  """Create a nested object structure from keyword arguments.

  Args:
    **kwargs: Attributes to set. Dictionary values are recursively converted
      to objects.

  Returns:
    Obj: A nested object structure.
  """
  for k, v in kwargs.items():
    if cu.isdict(v):
      kwargs[k] = make_object_recursive(**v)

  return make_object(**kwargs)


def locals_capture(locs, exclude='self'):
  """Capture local variables into an object, excluding specified names.

  Args:
    locs: Dictionary of local variables (typically from locals()).
    exclude: Comma-separated string or sequence of names to exclude
      (default: 'self').

  Returns:
    Obj: Object with captured local variables as attributes.
  """
  exclude = set(expand_strings(exclude, 'self'))

  return make_object(**{k: v for k, v in locs.items() if k not in exclude})


def sreplace(rex, data, mapfn, nmapfn=None, join=True):
  """Rewrite the parts of a string matching a regular expression.

  The pattern must define a capture group: matched groups are passed to
  mapfn, while the unmatched text is passed to nmapfn.

  Args:
    rex: Regular expression whose first group drives the replacement.
    data: String to rewrite.
    mapfn: Callable mapping a matched group to its replacement.
    nmapfn: Optional callable mapping the non-matching text (default:
      identity).
    join: If True, return the rewritten string; if False return the list
      of the rewritten parts.

  Returns:
    The rewritten string, or the list of its parts when join is False.
  """
  nmapfn = nmapfn if nmapfn is not None else cu.ident

  lastpos, parts = 0, []
  for m in re.finditer(rex, data):
    start, end = m.span()
    if start > lastpos:
      parts.append(nmapfn(data[lastpos:start]))

    lastpos = end
    mid = mapfn(m.group(1))
    parts.append(mid)

  if lastpos < len(data):
    parts.append(nmapfn(data[lastpos:]))

  return ''.join(parts) if join else parts


def as_sequence(v, t=tuple):
  """Convert a value into a sequence of the given type.

  Single elements are wrapped into a one-element sequence, while
  sequences and generators are converted as-is.

  Args:
    v: Value to convert.
    t: Target sequence type, or a tuple of types tried in order when v
      already is a sequence (default: tuple).

  Returns:
    The value as a sequence of type t.
  """
  if isinstance(t, (list, tuple)):
    for st in t:
      if isinstance(v, st):
        return v

    return t[0]([v]) if not isinstance(v, types.GeneratorType) else t[0](v)

  if isinstance(v, t):
    return v

  return t(v) if cu.is_sequence(v) else t([v])


def format(seq, fmt):
  """Format the elements of a sequence with a format spec.

  Args:
    seq: Sequence of values to format.
    fmt: Format specification, as per the format mini-language.

  Returns:
    A sequence of the same type of seq with the formatted elements.
  """
  sfmt = f'{{:{fmt}}}'

  return type(seq)(sfmt.format(x) for x in seq)


def value_or(v, defval):
  """Return v, or defval when v is None."""
  return v if v is not None else defval


def dict_rget(sdict, path, defval=None, sep='/'):
  """Get a value nested inside dictionaries using a path.

  Args:
    sdict: Dictionary to query.
    path: Slash-separated path of keys, or an already split sequence of
      keys.
    defval: Value returned when the path cannot be resolved.
    sep: Separator of the path components (default: '/').

  Returns:
    The value at the path, or defval.
  """
  dpath = path.strip(sep).split(sep) if isinstance(path, str) else path

  result = sdict
  for key in dpath:
    if not cu.isdict(result):
      return defval

    result = result.get(key, defval)

  return result


def dict_rset(sdict, path, value, sep='/', extend=False):
  """Set a value nested inside dictionaries using a path.

  Missing intermediate dictionaries are created, and existing ones must
  be dictionaries.

  Args:
    sdict: Dictionary to modify.
    path: Slash-separated path of keys, or an already split sequence of
      keys.
    value: Value to set at the path.
    sep: Separator of the path components (default: '/').
    extend: If True, extend the existing value with value instead of
      replacing it.

  Raises:
    ValueError: When an intermediate entry is not a dictionary.
  """
  dpath = path.strip(sep).split(sep) if isinstance(path, str) else path

  dest = sdict
  for skey in dpath[:-1]:
    subd = dest.get(skey)
    if subd is None:
      subd = dict()
      dest[skey] = subd
    else:
      tas.check(cu.isdict(subd), msg=f'Not a folder (dict): {skey} = {type(subd)}')

    dest = subd

  skey = dpath[-1]
  if not extend:
    dest[skey] = value
  else:
    xvalue = dest.get(skey)
    if xvalue is None:
      dest[skey] = value
    else:
      xvalue.extend(value)


def make_index_dict(vals):
  """Build a value-to-index dictionary from a sequence of values.

  Args:
    vals: Sequence of hashable values.

  Returns:
    A dict mapping each value to its ordinal position in vals.
  """
  return {v: i for i, v in enumerate(vals)}


def append_index_dict(xlist, xdict, value):
  """Append a value to a list and record its index in a dict.

  Args:
    xlist: List to append the value to.
    xdict: Dict mapping values to list positions, updated in place.
    value: Value to append.
  """
  xlist.append(value)
  xdict[value] = len(xlist) - 1


def compile(code, syms, env=None, vals=None, lookup_fn=None, delim=None):
  """Compile and execute code, extracting specified symbols.

  Note that objects compiled with this API cannot be pickled.
  If that is a requirement, use the dynamod module.

  Args:
    code: Source code string to compile and execute.
    syms: Symbol names to extract from the executed code (comma-separated or sequence).
    env: Execution environment dictionary (default: new dict).
    vals: Values for template replacement in code (default: None).
    lookup_fn: Function for template value lookup (default: None).
    delim: Template delimiter (default: None).

  Returns:
    Tuple of extracted symbol values in the order specified by syms.
  """
  # Note that objects compiled with this API cannot be pickled.
  # If that is a requirement, use the dynamod module.
  env = value_or(env, dict())
  if vals is not None or lookup_fn is not None:
    code = tr.template_replace(code, vals=vals, lookup_fn=lookup_fn, delim=delim)

  exec(code, env)  # noqa: S102 -- deliberate dynamic-code API

  return tuple(env.get(s) for s in expand_strings(syms))


def run(path, fnname, *args, **kwargs):
  """Run a function from a Python file.

  Args:
    path: Path to the Python file.
    fnname: Name of the function to run.
    *args: Positional arguments to pass to the function.
    **kwargs: Keyword arguments. Supports 'compile_args' for compilation options.

  Returns:
    The return value of the called function.
  """
  compile_args, = pop_kwargs(kwargs, 'compile_args')

  fn, = compile(mm.file_view(path), fnname, **(compile_args or dict()))

  return fn(*args, **kwargs)


def unpack_n(l, n, defval=None):
  """Unpack a sequence into exactly n elements.

  Args:
    l: Sequence to unpack; single values are treated as one-element
      sequences.
    n: Number of elements to return.
    defval: Value used to pad the result when l has less than n elements.

  Returns:
    A tuple with the first n elements of l, padded with defval.
  """
  l = as_sequence(l)

  return tuple(l[:n] if len(l) >= n else l + [defval] * (n - len(l)))


def getenv(name, dtype=None, defval=None):
  """Get an environment variable with type conversion.

  Args:
    name: Environment variable name.
    dtype: Type to convert the value to (default: None, returns string).
    defval: Default value if not set. Can be a callable for lazy evaluation.

  Returns:
    The environment variable value converted to dtype, or default value.
  """
  # os.getenv expects the default value to be a string, so cannot be passed in there.
  env = os.getenv(name)
  if env is None:
    env = defval() if callable(defval) else defval
  if env is not None:
    return cu.to_type(env, dtype) if dtype is not None else env


def env(name, defval, vtype=None):
  """Get an environment variable with a mandatory default value.

  Args:
    name: Environment variable name.
    defval: Default value returned when the variable is not set.
    vtype: Type to convert the value to.

  Returns:
    The environment variable value converted to vtype, or defval.
  """
  return getenv(name, dtype=vtype, defval=defval)


def envs(*args, as_dict=False):
  """Get multiple environment variables.

  Args:
    *args: Variable names; comma-separated strings are expanded.
    as_dict: If True, return a dict instead of a tuple (default: False).

  Returns:
    A tuple or dict with the values of the variables, None when missing.
  """
  return mget(os.environ, *args, as_dict=as_dict)


def import_env(dest, *args):
  """Import environment variables into a dictionary, inferring value types.

  Args:
    dest: Dictionary to add the variables to.
    *args: Variable names; comma-separated strings are expanded.

  Returns:
    The dest dictionary updated in place.
  """
  ivars = envs(*args, as_dict=True)
  for k, v in ivars.items():
    dest[k] = cu.infer_value(v)

  return dest


def map_env(g, prefix=''):
  """Override dictionary values from environment variables.

  For every key of g, the PREFIXkey environment variable overrides the
  corresponding value when set, converting it to the type of the current
  value.

  Args:
    g: Dictionary to update in place.
    prefix: Prefix of the environment variable names (default: '').

  Returns:
    The g dictionary updated in place.
  """
  ovr = dict()
  for k, v in g.items():
    ev = getenv(f'{prefix}{k}', dtype=type(v))
    if ev is not None:
      ovr[k] = ev

  g.update(ovr)

  return g


MAJOR = 1
MINOR = -1


def squeeze(shape, keep_dims=0, sdir=MAJOR):
  """Remove dimensions of size 1 from a shape.

  Args:
    shape: Input shape as a sequence.
    keep_dims: Minimum number of dimensions to keep (default: 0).
    sdir: Direction to squeeze - MAJOR (leading) or MINOR (trailing) (default: MAJOR).

  Returns:
    Shape with leading or trailing 1-dimensions removed, preserving type.

  Raises:
    ValueError: If sdir is not MAJOR or MINOR.
  """
  sshape = list(shape)
  if sdir == MAJOR:
    while len(sshape) > keep_dims and sshape[0] == 1:
      sshape = sshape[1:]
  elif sdir == MINOR:
    while len(sshape) > keep_dims and sshape[-1] == 1:
      sshape = sshape[:-1]
  else:
    alog.xraise(ValueError, f'Unknown squeeze direction: {sdir}')

  return type(shape)(sshape)


def flat2shape(data, shape):
  """Reshape flat data into a nested list structure matching the given shape.

  Args:
    data: Flat sequence of data elements.
    shape: Target shape as a sequence of dimensions.

  Returns:
    Nested list structure matching the shape.

  Raises:
    AssertionError: If data length doesn't match shape product.
  """
  tas.check_eq(len(data),
               np.prod(shape),
               msg=f'Shape {shape} is unsuitable for a {len(data)} long array')

  # For an Mx...xK input shape, return a M elements (nested) list.
  for n in reversed(shape[1:]):
    data = [data[i:i + n] for i in range(len(data), n)]

  return data


def shape2flat(data, shape):
  """Flatten a nested data structure into a flat tuple.

  Args:
    data: Nested iterable structure.
    shape: Expected shape of the data.

  Returns:
    Flat tuple of all elements.

  Raises:
    AssertionError: If data structure doesn't match shape.
  """
  for _ in range(len(shape) - 1):
    tas.check(hasattr(data, '__iter__'), msg=f'Wrong data type: {type(data)}')
    ndata = []
    for av in data:
      tas.check(hasattr(av, '__iter__'), msg=f'Wrong data type: {type(data)}')
      ndata.extend(av)

    data = ndata

  tas.check_eq(len(data),
               np.prod(shape),
               msg=f'Shape {shape} is unsuitable for a {len(data)} long array')

  return tuple(data)


def binary_reduce(parts, reduce_fn):
  """Reduce a sequence pairwise, halving it at every step.

  Args:
    parts: Sequence of values to reduce; its length does not need to be
      a power of two.
    reduce_fn: Callable combining two values into one.

  Returns:
    The single reduced value.
  """
  while len(parts) > 1:
    nparts, base = [], 0
    if len(parts) % 2 != 0:
      nparts.append(parts[0])
      base = 1

    nparts.extend(
        reduce_fn(parts[i], parts[i + 1]) for i in range(base,
                                                         len(parts) - 1, 2))

    parts = nparts

  return parts[0]


def stringify(s):
  """Convert every leaf value of a nested structure into a string.

  Args:
    s: Nested structure of lists, tuples and dictionaries.

  Returns:
    The structure with every non-container leaf converted with str().
  """

  def rwfn(v):
    """Return the string form of a non-container leaf value."""
    if not (isinstance(v, (list, tuple)) or cu.isdict(v)):
      return str(v)

  return cu.data_rewrite(s, rwfn)


def seq_rewrite(seq, sd):
  """Rewrite the elements of a sequence using a substitution dict.

  Args:
    seq: Sequence whose elements are rewritten.
    sd: Dict mapping original elements to their replacement; elements
      not in the dict are kept as-is.

  Returns:
    A sequence of the same type of seq with the rewritten elements.
  """
  return type(seq)(sd.get(s, s) for s in seq)


def dfetch(d, *args):
  """Fetch multiple keys from a dictionary, requiring them to be present.

  Args:
    d: Dictionary to query.
    *args: Keys to retrieve.

  Returns:
    A tuple with the values of the keys.

  Raises:
    KeyError: When a key is missing.
  """
  return tuple(d[n] for n in args)


def numel(t):
  """Return the number of elements of a tensor-like object.

  Args:
    t: Object with a shape attribute or a sized sequence.

  Returns:
    The product of the shape entries, or the length of t when the shape
    is missing.
  """
  sp = cu.get_property(t, 'shape')

  return np.prod(sp) if sp is not None else len(t)


def scale_data(data, base_data, scale):
  """Compute scaled differences against a base sequence.

  Args:
    data: Sequence of values to scale.
    base_data: Sequence of reference values the differences are computed
      against.
    scale: Multiplier applied to the relative differences.

  Returns:
    The sequence of scaled values.
  """
  return ((data - base_data) / base_data) * scale


_ARRAY_SIZES = tuple((array.array(c).itemsize, c) for c in ['B', 'H', 'I', 'L', 'Q'])


def array_code(size):
  """Return the array module typecode for a byte size.

  Args:
    size: Number of bytes to represent.

  Returns:
    The typecode of the smallest unsigned integer array type able to hold
    size bytes.

  Raises:
    ValueError: When size does not fit any of the supported types.
  """
  nbytes = math.ceil(math.log2(size)) / 8
  for cb, code in _ARRAY_SIZES:
    if cb >= nbytes:
      return code

  alog.xraise(ValueError, f'Size {size} too big to fit inside any array integer types')


def checked_remove(l, o):
  """Remove an element from a list, tolerating missing elements.

  Args:
    l: List to remove the element from.
    o: Element to remove.

  Returns:
    True when the element was removed, False when it was not present.
  """
  try:
    l.remove(o)
  except ValueError:
    return False

  return True


def sleep_until(date, msg=None):
  """Sleep until a target datetime is reached.

  Args:
    date: Timezone-aware target datetime.
    msg: Optional message logged before sleeping.
  """
  now = datetime.datetime.now(tz=date.tzinfo)
  if date > now:
    if msg:
      alog.info(msg)
    time.sleep(date.timestamp() - now.timestamp())


def parse_dict(data):
  """Parse a YAML string into a dictionary.

  Args:
    data: YAML string to parse.

  Returns:
    The parsed dictionary.
  """
  return yaml.safe_load(data)


def parse_args(in_args):
  """Parse comma-separated arguments into positional args and kwargs.

  Args:
    in_args: Comma-separated string, or sequence of strings; entries
      containing '=' become keyword arguments and their values are
      parsed as YAML, the remaining entries become positional arguments.

  Returns:
    A tuple with the list of positional arguments and the dict of keyword
    arguments.

  Raises:
    ValueError: When an entry has more than one '='.
  """
  seq_args = comma_split(in_args) if isinstance(in_args, str) else in_args

  args, kwargs = [], dict()
  for arg in seq_args:
    parts = cu.separate(arg, '=')
    if len(parts) == 2:
      kwargs[parts[0]] = yaml.safe_load(parts[1])
    elif len(parts) == 1:
      args.append(yaml.safe_load(parts[0]))
    else:
      alog.xraise(ValueError, f'Syntax error: {arg}')

  return args, kwargs


def state_update(path, **kwargs):
  """Load, update and persist a pickled state dictionary.

  Args:
    path: Path or URL of the file holding the pickled state; missing
      files yield an empty state.
    **kwargs: Key/value pairs merged into the state; when no pair is
      given the state is only loaded.

  Returns:
    The updated state dictionary.
  """
  if sfile := gfs.maybe_open(path, mode='rb'):
    with sfile as fd:
      state = pickle.load(fd)
  else:
    state = dict()

  if kwargs:
    state.update(kwargs)
    with fow.FileOverwrite(path, mode='wb') as f:
      pickle.dump(state, f, protocol=pickle_proto())

  return state


def copy_inplace(dest, src):
  """Copy the attributes of an object into another one in place.

  Args:
    dest: Object to copy the attributes into; objects without __dict__
      copy their slots instead.
    src: Object to copy the attributes from.

  Returns:
    The dest object updated in place.
  """
  obj_dict = getattr(dest, '__dict__', None)
  if obj_dict is not None:
    obj_dict.update(src.__dict__)
  else:
    for slot in iu.class_slots(dest):
      setattr(dest, slot, getattr(src, slot, None))

  return dest
