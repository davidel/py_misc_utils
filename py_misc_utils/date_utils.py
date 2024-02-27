import datetime
import pytz
import re

import dateutil
import dateutil.parser
import numpy as np
import pandas as pd

from . import assert_checks as tas


def make_datetime_from_epoch(s, tz=None):
  ds = pd.to_datetime(s, unit='s', origin='unix', utc=True)

  if tz is not None:
    return ds.dt.tz_convert(tz) if isinstance(ds, pd.Series) else ds.tz_convert(tz)

  return ds


def us_eastern_timezone():
  return pytz.timezone('US/Eastern')


def now(tz=None):
  return datetime.datetime.now(tz=tz or us_eastern_timezone())


def from_timestamp(ts, tz=None):
  return datetime.datetime.fromtimestamp(ts, tz=tz or us_eastern_timezone())


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

