import inspect
import logging
import os

from . import alog


def get_loc(path, lineno):
  if os.path.isfile(path):
    with open(path, mode='r') as f:
      lines = f.read().splitlines()
      return lines[lineno - 1] if len(lines) > lineno else None
      

def get_caller_info(n_back):
  frame = inspect.stack()[n_back + 1][0]
  caller = inspect.getframeinfo(frame)
  loc = get_loc(caller.filename, caller.lineno)
  if loc:
    return f'{caller.filename}:{caller.lineno}: {loc.lstrip()}'
  
  return f'{caller.filename}:{caller.lineno}'


def report_fail(level, op, *args, **kwargs):
  fmsg = kwargs.get('msg', None)
  cinfo = get_caller_info(2)
  if fmsg:
    cinfo = f'{cinfo}; {fmsg}'
  if op:
    if callable(op):
      fname = getattr(op, '__name__', str(op))
      arg_list = ', '.join([str(arg) for arg in args])
      res = kwargs.get('res', None)
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
  alog.log(level, msg)
  raise AssertionError(msg)


def check(a, level=logging.ERROR, msg=None):
  if not a:
    report_fail(level, None, msg=msg)


def check_fn(fn, *args, level=logging.ERROR, msg=None):
  if not fn(*args):
    report_fail(level, fn, *args, msg=msg)


def check_fnres_eq(res, fn, *args, level=logging.ERROR, msg=None):
  if not (fn(*args) == res):
    report_fail(level, fn, *args, res=res, res_op='==', msg=msg)


def check_fnres_ne(res, fn, *args, level=logging.ERROR, msg=None):
  if fn(*args) == res:
    report_fail(level, fn, *args, res=res, res_op='!=', msg=msg)


def check_is_none(a, level=logging.ERROR, msg=None):
  if a is not None:
    report_fail(level, '==', a, 'None', msg=msg)


def check_is_not_none(a, level=logging.ERROR, msg=None):
  if a is None:
    report_fail(level, '!=', a, 'None', msg=msg)


def check_eq(a, b, level=logging.ERROR, msg=None):
  if not (a == b):
    report_fail(level, '==', a, b, msg=msg)


def check_ne(a, b, level=logging.ERROR, msg=None):
  if not (a != b):
    report_fail(level, '!=', a, b, msg=msg)


def check_le(a, b, level=logging.ERROR, msg=None):
  if not (a <= b):
    report_fail(level, '<=', a, b, msg=msg)


def check_ge(a, b, level=logging.ERROR, msg=None):
  if not (a >= b):
    report_fail(level, '>=', a, b, msg=msg)


def check_lt(a, b, level=logging.ERROR, msg=None):
  if not (a < b):
    report_fail(level, '<', a, b, msg=msg)


def check_gt(a, b, level=logging.ERROR, msg=None):
  if not (a > b):
    report_fail(level, '>', a, b, msg=msg)

