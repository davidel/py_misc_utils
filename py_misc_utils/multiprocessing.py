import functools
import gc
import multiprocessing
import signal
import sys

from . import alog
from . import cleanups
from . import global_namespace as gns


class TerminationError(Exception):
  pass

def _sig_handler(sig, frame):
  if sig == signal.SIGINT:
    raise KeyboardInterrupt()
  elif sig == signal.SIGTERM:
    raise TerminationError()


def _signals_setup():
  signal.signal(signal.SIGINT, _sig_handler)
  signal.signal(signal.SIGTERM, _sig_handler)


def _cleanup():
  cleanups.run()
  gc.collect()
  # Ignore {INT, TERM} signals on exit.
  signal.signal(signal.SIGINT, signal.SIG_IGN)
  signal.signal(signal.SIGTERM, signal.SIG_IGN)


_GNS_KEY = 'gns'

def _wrap_procfn_parent(method):
  ctx = dict(method=method)
  ctx.update({_GNS_KEY: gns.parent_switch(method)})

  return ctx


def _wrap_procfn_child(method, pctx):
  parent_gns = pctx.pop(_GNS_KEY, None)
  if parent_gns is not None:
    gns.child_switch(method, parent_gns)

  return pctx


_CONTEXT_KEY = '_parent_context'

def _capture_parent_context(method, kwargs):
  pctx = _wrap_procfn_parent(method)
  kwargs.update({_CONTEXT_KEY: pctx})

  return kwargs


def _apply_child_context(kwargs):
  pctx = kwargs.pop(_CONTEXT_KEY)
  pctx = _wrap_procfn_child(pctx['method'], pctx)

  return kwargs


def procfn_wrap(procfn, *args, **kwargs):
  try:
    _signals_setup()

    return procfn(*args, **kwargs)
  except KeyboardInterrupt:
    sys.exit(1)
  except Exception as ex:
    alog.exception(ex, exmsg=f'Exception while running process function')
    raise
  finally:
    _cleanup()


def _wrapped_procfn(procfn, *args, **kwargs):
  kwargs = _apply_child_context(kwargs)

  return procfn(*args, **kwargs)


def create_process(procfn, args=None, kwargs=None, context=None, daemon=None):
  if context is None:
    mpctx = multiprocessing
  elif isinstance(context, str):
    mpctx = multiprocessing.get_context(method=context)
  else:
    mpctx = context

  args = () if args is None else args
  kwargs = {} if kwargs is None else kwargs

  kwargs = _capture_parent_context(mpctx.get_start_method(), kwargs)
  target = functools.partial(procfn_wrap, _wrapped_procfn, procfn, *args, **kwargs)

  return mpctx.Process(target=target, daemon=daemon)

