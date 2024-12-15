import functools
import weakref


class _Gone:
  pass

GONE = _Gone()


def weak_caller(ref, name, *args, **kwargs):
  wobj = ref()

  return getattr(wobj, name)(*args, **kwargs) if wobj is not None else GONE


def weak_call(obj, name, *args, **kwargs):
  ref = weakref.ref(obj)

  return functools.partial(weak_caller, ref, name, *args, **kwargs)

