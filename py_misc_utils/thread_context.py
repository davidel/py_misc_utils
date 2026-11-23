"""Thread-local context stack support.

This module provides a mechanism to keep per-thread context objects: a
Context object pushes an object onto a thread-local stack when entered,
and pops it when exited, so that each thread can have its own current
context - identified by name - at any time.
"""
import threading

_TLS = threading.local()


class Context:
  """A thread-local context manager.

  Entering a Context pushes its object onto a thread-local stack, while
  exiting pops it back. The top-of-stack object of a named context can be
  retrieved with get_context().

  The same object can be shared by multiple threads, each of them having
  its own stack.
  """

  def __init__(self, name, obj):
    """Creates a thread-local context.

    Args:
      name: The name identifying the context stack.
      obj: The object to be pushed onto the stack when the context is
        entered, and returned by get_context().
    """
    self._name = name
    self._obj = obj

  def __enter__(self):
    """Pushes the context object onto the thread-local stack.

    Returns:
      The context object.
    """
    stack = getattr(_TLS, self._name, None)
    if stack is None:
      stack = []
      setattr(_TLS, self._name, stack)

    stack.append(self._obj)

    return self._obj

  def __exit__(self, *exc):
    """Pops the context object from the thread-local stack."""

    stack = getattr(_TLS, self._name)
    obj = stack.pop()

    return False


def get_context(name):
  """Returns the current object of a named context, for the calling thread.

  Args:
    name: The name identifying the context stack.

  Returns:
    The object pushed by the innermost Context being executed by the
    calling thread, or None if no such context is active.
  """
  stack = getattr(_TLS, name, None)

  return stack[-1] if stack else None
