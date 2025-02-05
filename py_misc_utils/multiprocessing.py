import functools
import gc
import multiprocessing
import sys

from . import alog
from . import cleanups
from . import global_namespace as gns


def cleanup():
  cleanups.run()
  gc.collect()


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


def _wrapped_procfn(procfn, *args, **kwargs):
  try:
    kwargs = _apply_child_context(kwargs)

    return procfn(*args, **kwargs)
  except KeyboardInterrupt:
    sys.exit(1)
  except Exception as ex:
    alog.exception(ex, exmsg=f'Exception while running process function')
    raise
  finally:
    cleanup()


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
  target = functools.partial(_wrapped_procfn, procfn, *args, **kwargs)

  return mpctx.Process(target=target, daemon=daemon)

