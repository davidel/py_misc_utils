# When using multiprocessing, there are two distinct behaviours if fork/forkserver
# start methods are used, WRT spawn.  In the latter case the global context
# accumulated by the running process is not pickled-through the child, so the new
# process start with wiped out global namespace.
# Using this API, together with the app_main.create_process(), it is possible to
# have global data transfered to the child.
# All data stored in the global namespace must be pickle-able.

import collections


Var = collections.namedtuple('Var', 'name, parent_fn, child_fn, data, defval',
                             defaults=(None, None, None, None))

_NS = dict()


def parent_switch():
  ns = dict()
  for var in _NS.values():
    if var.parent_fn is not None:
      data = var.parent_fn(var.data)
      if data is not var.data:
        var = var._replace(data=data)

    ns[var.name] = var

  return ns


def child_switch(ns):
  global _NS

  cns = dict()
  for var in ns.values():
    if var.child_fn is not None:
      data = var.child_fn(var.data)
      if data is not var.data:
        var = var._replace(data=data)

    cns[var.name] = var

  _NS = cns


def get(var, force=True):
  value = _NS.get(var.name)
  if value is None and force:
    value = var._replace(data=var.defval)
    _NS[value.name] = value

  return value.data


def set(var, data):
  prev_value = _NS.get(var.name)
  value = var._replace(data=data)
  _NS[value.name] = value

  return prev_value.data if prev_value is not None else None

