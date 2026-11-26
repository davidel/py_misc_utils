"""Value assertions with detailed failure reporting.

This module provides the tas.* check functions - a set of assertion
helpers used all over the library and its tests to validate arguments,
results and invariants.

Each check raises an AssertionError when its condition is not satisfied,
logging a detailed failure message - including the source location of the
failing check, the performed operation and the involved values - before
raising.
"""
import inspect
import logging
import os

# Note: This is a core module, which cannot have local imports!
# Using the logging module above to not add the alog module dependency here,
# since that requires more dependencies to be pulled.
# At the end, alog.log(...) is exactly the same as logging.log(...) in substance.


def _get_loc(path, lineno):
  """Returns the source line at a given location, if available."""
  if os.path.isfile(path):
    with open(path, mode='r') as fd:
      lines = fd.read().splitlines()

    return lines[lineno - 1] if len(lines) > lineno else None


def _get_caller_info(n_back):
  """Builds the caller location description of a failing check."""
  frame = inspect.stack()[n_back + 1][0]
  caller = inspect.getframeinfo(frame)
  loc = _get_loc(caller.filename, caller.lineno)
  if loc:
    return f'{caller.filename}:{caller.lineno}: {loc.lstrip()}'

  return f'{caller.filename}:{caller.lineno}'


def _report_fail(level, op, *args, exception=AssertionError, **kwargs):
  """Logs a check failure message and raises an exception.

  Args:
    level: The logging level used to log the failure message.
    op: The check operation: None for plain checks, a callable for
      function checks, or an operator string - like '==' - for value
      checks.
    *args: The values involved in the failed check.
    exception: The exception class to raise (default: AssertionError).
    **kwargs: The failure description ('msg'), and - for function checks -
      the expected result ('res') and its operator ('res_op').

  Raises:
    exception: Always (default AssertionError).
  """
  fmsg = kwargs.get('msg')
  cinfo = _get_caller_info(2)
  if fmsg:
    cinfo = f'{cinfo}; {fmsg}'
  if op:
    if callable(op):
      fname = getattr(op, '__name__', str(op))
      arg_list = ', '.join([str(arg) for arg in args])
      res = kwargs.get('res')
      if res:
        res_op = kwargs['res_op']
        msg = f'{fname}({arg_list}) {res_op} {res} failed from {cinfo}'
      else:
        msg = f'{fname}({arg_list}) failed from {cinfo}'
    else:
      assert len(args) == 2, len(args)
      msg = f'{args[0]} {op} {args[1]} failed from {cinfo}'
  else:
    msg = f'Check failed from {cinfo}'

  logging.log(level, msg)

  raise exception(msg)


def check(a, level=logging.ERROR, msg=None, exception=AssertionError):
  """Checks that a value is true.

  Args:
    a: The value to check.
    level: The logging level used to log the failure message.
    msg: An optional failure description added to the failure message.
    exception: The exception class to raise (default: AssertionError).

  Raises:
    exception: If the value is not true.
  """
  if not a:
    _report_fail(level, None, msg=msg, exception=exception)


def check_fn(fn, *args, level=logging.ERROR, msg=None, exception=AssertionError):
  """Checks that a predicate function returns a true value.

  Args:
    fn: The predicate function to call.
    *args: The arguments to pass to the predicate function.
    level: The logging level used to log the failure message.
    msg: An optional failure description added to the failure message.
    exception: The exception class to raise (default: AssertionError).

  Raises:
    exception: If the predicate function returns a false value.
  """
  if not fn(*args):
    _report_fail(level, fn, *args, msg=msg, exception=exception)


def check_fnres_eq(res,
                   fn,
                   *args,
                   level=logging.ERROR,
                   msg=None,
                   exception=AssertionError):
  """Checks that a function returns an expected result.

  Args:
    res: The expected result.
    fn: The function to call.
    *args: The arguments to pass to the function.
    level: The logging level used to log the failure message.
    msg: An optional failure description added to the failure message.
    exception: The exception class to raise (default: AssertionError).

  Raises:
    exception: If the function result differs from the expected result.
  """
  if fn(*args) != res:
    _report_fail(level, fn, *args, res=res, res_op='==', msg=msg, exception=exception)


def check_fnres_ne(res,
                   fn,
                   *args,
                   level=logging.ERROR,
                   msg=None,
                   exception=AssertionError):
  """Checks that a function does not return a given result.

  Args:
    res: The result the function must not return.
    fn: The function to call.
    *args: The arguments to pass to the function.
    level: The logging level used to log the failure message.
    msg: An optional failure description added to the failure message.
    exception: The exception class to raise (default: AssertionError).

  Raises:
    exception: If the function result equals the given result.
  """
  if fn(*args) == res:
    _report_fail(level, fn, *args, res=res, res_op='!=', msg=msg, exception=exception)


def check_is_none(a, level=logging.ERROR, msg=None, exception=AssertionError):
  """Checks that a value is None.

  Args:
    a: The value to check.
    level: The logging level used to log the failure message.
    msg: An optional failure description added to the failure message.
    exception: The exception class to raise (default: AssertionError).

  Raises:
    exception: If the value is not None.
  """
  if a is not None:
    _report_fail(level, '==', a, 'None', msg=msg, exception=exception)


def check_is_not_none(a, level=logging.ERROR, msg=None, exception=AssertionError):
  """Checks that a value is not None.

  Args:
    a: The value to check.
    level: The logging level used to log the failure message.
    msg: An optional failure description added to the failure message.
    exception: The exception class to raise (default: AssertionError).

  Raises:
    exception: If the value is None.
  """
  if a is None:
    _report_fail(level, '!=', a, 'None', msg=msg, exception=exception)


def check_in(a, b, level=logging.ERROR, msg=None, exception=AssertionError):
  """Checks that a value is contained in a container.

  Args:
    a: The value to check.
    b: The container which must contain the value.
    level: The logging level used to log the failure message.
    msg: An optional failure description added to the failure message.
    exception: The exception class to raise (default: AssertionError).

  Raises:
    exception: If the value is not contained in the container.
  """
  if a not in b:
    _report_fail(level, 'in', a, b, msg=msg, exception=exception)


def check_eq(a, b, level=logging.ERROR, msg=None, exception=AssertionError):
  """Checks that two values are equal.

  Args:
    a: The first value.
    b: The second value.
    level: The logging level used to log the failure message.
    msg: An optional failure description added to the failure message.
    exception: The exception class to raise (default: AssertionError).

  Raises:
    exception: If the values are not equal.
  """
  if a != b:
    _report_fail(level, '==', a, b, msg=msg, exception=exception)


def check_ne(a, b, level=logging.ERROR, msg=None, exception=AssertionError):
  """Checks that two values are different.

  Args:
    a: The first value.
    b: The second value.
    level: The logging level used to log the failure message.
    msg: An optional failure description added to the failure message.
    exception: The exception class to raise (default: AssertionError).

  Raises:
    exception: If the values are equal.
  """
  if a == b:
    _report_fail(level, '!=', a, b, msg=msg, exception=exception)


def check_le(a, b, level=logging.ERROR, msg=None, exception=AssertionError):
  """Checks that a value is less than, or equal to, another one.

  Args:
    a: The value to check.
    b: The reference value.
    level: The logging level used to log the failure message.
    msg: An optional failure description added to the failure message.
    exception: The exception class to raise (default: AssertionError).

  Raises:
    exception: If the value is greater than the reference value.
  """
  if not (a <= b):
    _report_fail(level, '<=', a, b, msg=msg, exception=exception)


def check_ge(a, b, level=logging.ERROR, msg=None, exception=AssertionError):
  """Checks that a value is greater than, or equal to, another one.

  Args:
    a: The value to check.
    b: The reference value.
    level: The logging level used to log the failure message.
    msg: An optional failure description added to the failure message.
    exception: The exception class to raise (default: AssertionError).

  Raises:
    exception: If the value is less than the reference value.
  """
  if not (a >= b):
    _report_fail(level, '>=', a, b, msg=msg, exception=exception)


def check_lt(a, b, level=logging.ERROR, msg=None, exception=AssertionError):
  """Checks that a value is less than another one.

  Args:
    a: The value to check.
    b: The reference value.
    level: The logging level used to log the failure message.
    msg: An optional failure description added to the failure message.
    exception: The exception class to raise (default: AssertionError).

  Raises:
    exception: If the value is not less than the reference value.
  """
  if not (a < b):
    _report_fail(level, '<', a, b, msg=msg, exception=exception)


def check_gt(a, b, level=logging.ERROR, msg=None, exception=AssertionError):
  """Checks that a value is greater than another one.

  Args:
    a: The value to check.
    b: The reference value.
    level: The logging level used to log the failure message.
    msg: An optional failure description added to the failure message.
    exception: The exception class to raise (default: AssertionError).

  Raises:
    exception: If the value is not greater than the reference value.
  """
  if not (a > b):
    _report_fail(level, '>', a, b, msg=msg, exception=exception)
