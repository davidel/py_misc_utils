"""Object reference tracking and leak detection utilities.

This module provides :func:`track_objects` which scans all objects tracked by
the garbage collector, identifies which objects are tracked by a given tracker
callable, and produces a report of reference chains that keep objects alive.
"""

import collections
import gc

from . import alog
from . import core_utils as cu
from . import inspect_utils as iu


def _get_referred(obj):
  """Return the named references held by obj.

  Args:
    obj: The object to inspect.

  Returns:
    A tuple of (name, referent) pairs describing what obj references.
  """
  referred = []
  if cu.isdict(obj):
    for name, value in obj.items():
      referred.append((name, value))
  elif isinstance(obj, (list, tuple)):
    for i, value in enumerate(obj):
      referred.append((f'[{i}]', value))
  elif hasattr(obj, '__dict__'):
    for name, value in vars(obj).items():
      referred.append((name, value))

  return tuple(referred)


def _get_tracking_references(obj, tracked_by, max_references=None):
  """Trace reference chains that keep obj alive.

  Starting from obj, follows references backward through objects that
  reference it, building a chain of referrers up to max_references.

  Args:
    obj: The object to trace references for.
    tracked_by: A mapping of object id to list of (referrer_name, referrer_obj).
    max_references: Maximum number of reference chains to collect.

  Returns:
    A tuple of (qualified_name, reference_path) pairs for each tracked
    reference chain.
  """
  max_references = max_references or 8

  references = []
  to_track = [(obj, None)]
  while to_track:
    tobj, tname = to_track.pop()
    ntrack = 0
    for rname, robj in tracked_by.get(id(tobj), ()):
      suffix = ''
      if tname is not None:
        sep = '' if tname.startswith('[') else '.'
        suffix = f'{sep}{tname}'

      to_track.append((robj, f'{rname}{suffix}'))
      ntrack += 1

    if ntrack == 0:
      references.append((iu.qual_name(tobj), tname))
      if len(references) >= max_references:
        break

  return tuple(references)


def track_objects(tracker, max_references=None):
  """Scan garbage-collected objects and report those tracked by tracker.

  Runs gc.collect(), enumerates all GC-tracked objects, builds reference
  maps, and invokes tracker.track(obj) for each object. Returns a report
  of objects and their reference chains sorted by priority.

  Args:
    tracker: An object with a track(obj) method returning (priority, info)
      or None if the object should not be tracked.
    max_references: Maximum number of reference chains to report per object.

  Returns:
    A newline-separated string report of tracked objects and references.
  """
  gc.collect()

  gc_objs = gc.get_objects()

  all_objs = dict()
  tracking = collections.defaultdict(list)
  tracked_by = collections.defaultdict(list)
  for obj in gc_objs:
    all_objs[id(obj)] = obj

    referred = _get_referred(obj)
    for rname, robj in referred:
      tracking[id(obj)].append((rname, robj))
      tracked_by[id(robj)].append((rname, obj))
      all_objs[id(robj)] = robj

  report = []
  for obj in all_objs.values():
    try:
      if (trackres := tracker.track(obj)) is not None:
        prio, info = trackres

        refs = _get_tracking_references(obj, tracked_by, max_references=max_references)

        treport = [info]
        for r in refs:
          treport.append(f'  refby = {r[0]} ({r[1]})')

        report.append((prio, treport))
    except Exception as ex:  # noqa: BLE001 -- per-object isolation: a failing tracker callback skips that object, not the whole report
      alog.warning(f'Exception while tracking objects: {ex}')

  sreport = []
  for r in sorted(report, key=lambda r: r[0]):
    sreport.extend(r[1])

  return '\n'.join(sreport)
