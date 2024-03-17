import weakref


class _Gone:
  pass

GONE = _Gone()


def weak_call(obj, name, *args, **kwargs):
  ref = weakref.ref(obj)

  def wfn():
    wobj = ref()

    return getattr(wobj, name)(*args, **kwargs) if wobj is not None else GONE

  return wfn

