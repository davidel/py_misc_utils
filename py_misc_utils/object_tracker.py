import collections
import gc

from . import alog
from . import core_utils as cu
from . import inspect_utils as iu


def _get_referred(obj):
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

        refs = _get_tracking_references(obj, tracked_by,
                                        max_references=max_references)

        treport = [info]
        for r in refs:
          treport.append(f'  refby = {r[0]} ({r[1]})')

        report.append((prio, treport))
    except Exception as ex:
      alog.warning(f'Exception while tracking objects: {ex}')

  sreport = []
  for r in sorted(report, key=lambda r: r[0]):
    sreport.extend(r[1])

  return '\n'.join(sreport)

