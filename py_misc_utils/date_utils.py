"""Datetime and timezone helpers built on pytz and dateutil.

This module centralizes the timezone handling of the package: it exposes
the DEFAULT_TZ resolved from the DEFAULT_TZ environment variable plus
helpers to get the current time, convert timestamps, parse date strings
and shift or floor datetimes.
"""

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
  """Returns the NYSE market timezone."""
  return pytz.timezone('America/New_York')


def now(tz=None):
  """Returns the current datetime, in a given timezone.

  Args:
    tz: An optional timezone - the default timezone if None.

  Returns:
    The current timezone-aware datetime.
  """
  return datetime.datetime.now(tz=tz or DEFAULT_TZ)


def from_timestamp(ts, tz=None):
  """Converts a unix timestamp to a datetime.

  Args:
    ts: The unix timestamp - in seconds.
    tz: An optional timezone to attach to the datetime - the default
      timezone if None.

  Returns:
    The timezone-aware datetime.
  """
  return datetime.datetime.fromtimestamp(ts, tz=tz or DEFAULT_TZ)


def make_datetime_from_epoch(s, tz=None):
  """Converts epoch-second values to datetimes.

  Args:
    s: The epoch-second values: a scalar, list, numpy array, or pandas
      object.
    tz: An optional timezone the values are converted to - UTC by
      default.

  Returns:
    The timezone-aware datetime(s), in the type of the input values.
  """
  ds = pd.to_datetime(s, unit='s', origin='unix', utc=True)

  if tz is not None:
    return ds.dt.tz_convert(tz) if isinstance(ds, pd.Series) else ds.tz_convert(tz)

  return ds


def parse_date(dstr, tz=None):
  """Parses a date-time string.

  Accepts epoch timestamps - prefixed with '@' - ISO-8601 strings, and
  the date formats best-effort parsed by dateutil.

  Args:
    dstr: The date-time string to parse.
    tz: An optional timezone to attach to the parsed datetime, when the
      string does not hold timezone information.

  Returns:
    The timezone-aware datetime.
  """
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
  """Splits a datetime into its time-of-day offset and day start.

  Args:
    dt: The datetime to split.

  Returns:
    A (offset, day start datetime) tuple, where offset is the number of
    seconds from the start of the day of the datetime, and the day start
    datetime has its time-of-day components zeroed.
  """
  ddt = dt.replace(hour=0, minute=0, second=0, microsecond=0)

  return dt.timestamp() - ddt.timestamp(), ddt


def np_datetime_to_epoch(dt, dtype=np.float64):
  """Converts numpy datetime64 values to epoch seconds.

  Args:
    dt: The numpy datetime64 array to convert.
    dtype: The numpy dtype of the returned values - float64 by default.

  Returns:
    The epoch-seconds values.
  """
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
  """Aligns a datetime to a time step grid.

  Args:
    dt: The datetime to align.
    step: The alignment step - in seconds, or as a timedelta.
    ceil: Whether to align to the next step boundary (ceiling) instead
      of the previous one (floor).

  Returns:
    The aligned datetime, in the timezone of the input datetime.
  """
  secs = step.total_seconds() if isinstance(step, datetime.timedelta) else step
  fp, ip = np.modf(dt.timestamp() / secs)

  if ceil and not np.isclose(fp, 0):
    ip += 1

  return from_timestamp(ip * secs, tz=dt.tzinfo)


def at_time(dt, hour=0, minute=0, second=0, microsecond=0):
  """Replaces the time-of-day components of a datetime.

  Args:
    dt: The datetime to modify.
    hour: The hour of the day - midnight by default.
    minute: The minute.
    second: The second.
    microsecond: The microsecond.

  Returns:
    The datetime with the replaced time-of-day components.
  """
  return dt.replace(hour=hour, minute=minute, second=second, microsecond=microsecond)
