import datetime
import os
import pytz
import re

import dateutil
import dateutil.parser
import numpy as np
import pandas as pd

from . import assert_checks as tas


# Default timezone so code shows/works with such timezone.
DEFAULT_TZ = pytz.timezone(os.getenv('DEFAULT_TZ', 'America/New_York'))


def ny_market_timezone():
  return pytz.timezone('America/New_York')


def now(tz=None):
  return datetime.datetime.now(tz=tz or DEFAULT_TZ)


def from_timestamp(ts, tz=None):
  return datetime.datetime.fromtimestamp(ts, tz=tz if tz is not None else DEFAULT_TZ)


def make_datetime_from_epoch(s, tz=None):
  ds = pd.to_datetime(s, unit='s', origin='unix', utc=True)

  if tz is not None:
    return ds.dt.tz_convert(tz) if isinstance(ds, pd.Series) else ds.tz_convert(tz)

  return ds


def parse_date(dstr, tz=None):
  # Accept EPOCH values starting with @.
  m = re.match(r'@((\d+)(\.\d*)?)$', dstr)
  if m:
    return from_timestamp(float(m.group(1)), tz=tz)

  # ISO Format: 2011-11-17T00:05:23-04:00
  dt = dateutil.parser.isoparse(dstr)
  if dt.tzinfo is None and tz is not None:
    dt = tz.localize(dt)

  return dt


def day_offset(dt):
  ddt = dt.replace(hour=0, minute=0, second=0, microsecond=0)

  return dt.timestamp() - ddt.timestamp(), ddt


def np_datetime_to_epoch(dt, dtype=np.float64):
  tas.check_fn(np.issubdtype, dt.dtype, np.datetime64)
  u, c = np.datetime_data(dt.dtype)
  if u == 's':
    return dt.astype(dtype)

  dt = dt.astype(np.float64)
  if u == 'ns':
    dt = dt / 1e9
  elif u == 'us':
    dt = dt / 1e6
  elif u == 'ms':
    dt = dt / 1e3
  else:
    alog.xraise(RuntimeError, f'Unknown NumPy datetime64 unit: {u}')

  return dt if dtype == np.float64 else dt.astype(dtype)


def align(dt, step, ceil=False):
  secs = step.total_seconds() if isinstance(step, datetime.timedelta) else step
  fp, ip = np.modf(dt.timestamp() / secs)

  if ceil and not np.isclose(fp, 0):
    ip += 1

  return from_timestamp(ip * secs, tz=dt.tzinfo)

