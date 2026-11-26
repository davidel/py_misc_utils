"""Multiprocessing utilities for process creation with context propagation.

This module provides helpers for spawning child processes while preserving
global namespace state, signal handling, and cleanup behavior. It includes
the :class:`TerminationError` exception and functions such as
:func:`create_process` and :func:`procfn_wrap` for robust process management.
"""

import functools
import gc
import multiprocessing
import signal
import sys

from . import alog
from . import cleanups
from . import global_namespace as gns


class TerminationError(Exception):
  """Raised when a process receives SIGTERM and should terminate."""


def _sig_handler(sig, frame):
  """Signal handler that converts signals to Python exceptions.

  SIGINT is converted to KeyboardInterrupt, SIGTERM to TerminationError.

  Args:
    sig: The signal number received.
    frame: The current stack frame.
  """
  if sig == signal.SIGINT:
    raise KeyboardInterrupt()
  elif sig == signal.SIGTERM:
    raise TerminationError()


def _process_setup():
  """Install signal handlers for SIGINT and SIGTERM in the current process.

  SIGINT raises KeyboardInterrupt; SIGTERM raises TerminationError.
  """
  signal.signal(signal.SIGINT, _sig_handler)
  signal.signal(signal.SIGTERM, _sig_handler)


def _process_cleanup():
  """Run cleanup routines and ignore signals on process exit.

  Executes registered cleanups, performs garbage collection, and sets
  SIGINT/SIGTERM to SIG_IGN to prevent further interruptions during shutdown.
  """
  cleanups.run()
  gc.collect()
  # Ignore {INT, TERM} signals on exit.
  signal.signal(signal.SIGINT, signal.SIG_IGN)
  signal.signal(signal.SIGTERM, signal.SIG_IGN)


_GNS_KEY = 'gns'


def _wrap_procfn_parent(method):
  """Capture the parent-side global namespace context for propagation to a child.

  Args:
    method: The multiprocessing start method (e.g. 'fork', 'spawn').

  Returns:
    A context dictionary containing the start method and parent GNS state.
  """
  ctx = dict(method=method)
  ctx.update({_GNS_KEY: gns.parent_switch(method)})

  return ctx


def _wrap_procfn_child(method, pctx):
  """Restore the parent global namespace context in the child process.

  Args:
    method: The multiprocessing start method.
    pctx: The parent context dictionary captured by _wrap_procfn_parent.

  Returns:
    The context dictionary with parent GNS state applied.
  """
  parent_gns = pctx.pop(_GNS_KEY, None)
  if parent_gns is not None:
    gns.child_switch(method, parent_gns)

  return pctx


_CONTEXT_KEY = '_parent_context'


def _capture_parent_context(method, kwargs):
  """Capture parent-side context and inject it into kwargs for child propagation.

  Args:
    method: The multiprocessing start method.
    kwargs: Keyword arguments to augment with parent context.

  Returns:
    The updated kwargs dictionary containing the parent context.
  """
  pctx = _wrap_procfn_parent(method)
  kwargs.update({_CONTEXT_KEY: pctx})

  return kwargs


def _apply_child_context(kwargs):
  """Restore parent context in the child process before running the target function.

  Args:
    kwargs: Keyword arguments containing the captured parent context.

  Returns:
    The kwargs dictionary with parent context applied.
  """
  pctx = kwargs.pop(_CONTEXT_KEY)
  pctx = _wrap_procfn_child(pctx['method'], pctx)

  return kwargs


def procfn_wrap(procfn, *args, **kwargs):
  """Wrapper executed in the child process that sets up signals and cleanup.

  Installs signal handlers, runs the target function, and ensures cleanup
  runs even on exceptions.

  Args:
    procfn: The target function to execute.
    *args: Positional arguments for procfn.
    **kwargs: Keyword arguments for procfn.

  Returns:
    The return value of procfn.
  """
  try:
    _process_setup()

    return procfn(*args, **kwargs)
  except (KeyboardInterrupt, TerminationError):
    sys.exit(1)
  except Exception as ex:
    alog.exception(ex, exmsg='Exception while running process function')
    raise
  finally:
    _process_cleanup()


def _wrapped_procfn(procfn, *args, **kwargs):
  """Apply child context then delegate to the target function.

  Args:
    procfn: The target function to execute.
    *args: Positional arguments for procfn.
    **kwargs: Keyword arguments for procfn.

  Returns:
    The return value of procfn.
  """
  kwargs = _apply_child_context(kwargs)

  return procfn(*args, **kwargs)


def create_process(procfn, args=(), kwargs=None, context=None, daemon=None):
  """Create a multiprocessing.Process with context propagation and signal handling.

  The target function is wrapped to propagate the parent's global namespace
  context, install signal handlers, and run cleanup on exit.

  Args:
    procfn: The target function to execute in the child process.
    args: Positional arguments passed to procfn.
    kwargs: Keyword arguments passed to procfn.
    context: Optional multiprocessing context, method name string, or None
      to use the default context.
    daemon: Whether the process should be a daemon.

  Returns:
    A multiprocessing.Process instance (not yet started).
  """
  if context is None:
    mpctx = multiprocessing
  elif isinstance(context, str):
    mpctx = multiprocessing.get_context(method=context)
  else:
    mpctx = context

  kwargs = dict() if kwargs is None else kwargs.copy()

  kwargs = _capture_parent_context(mpctx.get_start_method(), kwargs)
  target = functools.partial(procfn_wrap, _wrapped_procfn, procfn, *args, **kwargs)

  return mpctx.Process(target=target, daemon=daemon)
