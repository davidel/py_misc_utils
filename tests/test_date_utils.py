"""Tests for the py_misc_utils.date_utils datetime helpers.

Expectations follow the documented API contract: '@'-prefixed strings
parse as unix timestamps, explicit offsets are kept while naive strings
get the default timezone, alignment floors (or ceils) to the step grid
and conversions preserve exact epoch values across datetime64 units.
"""

import datetime

import numpy as np
import pandas as pd

import py_misc_utils.date_utils as du


def test_parse_date_accepts_epoch_at_prefix():
  """A '@<seconds>' string parses as a unix timestamp in the default TZ."""
  assert du.parse_date('@1700000000') == du.from_timestamp(1700000000)
  assert du.parse_date('@1700000000.5') == du.from_timestamp(1700000000.5)


def test_parse_date_keeps_explicit_offsets():
  """ISO strings carrying an offset keep their own timezone."""
  dt = du.parse_date('2011-11-17T00:05:23-04:00')

  assert dt.year == 2011
  assert dt.utcoffset() == datetime.timedelta(hours=-4)


def test_parse_date_attaches_tz_only_when_passed():
  """parse_date() attaches a timezone only when the caller passes one."""
  naive = du.parse_date('2011-11-17T00:05:23')

  assert naive.tzinfo is None

  localized = du.parse_date('2011-11-17T00:05:23', tz=du.DEFAULT_TZ)

  # pytz tzinfo objects are per-instant templates: compare zone and offset
  # (Nov 17 2011 is after the DST end, hence the -5h standard offset).
  assert localized.tzinfo.zone == du.DEFAULT_TZ.zone
  assert localized.utcoffset() == datetime.timedelta(hours=-5)


def test_day_offset_splits_time_of_day():
  """day_offset() returns the seconds from day start plus the day start."""
  dt = du.DEFAULT_TZ.localize(datetime.datetime(2024, 6, 1, 12, 30, 45))
  offset, day_start = du.day_offset(dt)

  assert offset == 12 * 3600 + 30 * 60 + 45
  assert day_start.hour == 0
  assert day_start.minute == 0
  assert day_start.date() == dt.date()


def test_align_floors_and_ceils_to_step_grid():
  """align() floors by default, ceils on demand, and accepts timedeltas."""
  dt = du.from_timestamp(1700000000)

  floor60 = du.align(dt, 60)
  ceil60 = du.align(dt, datetime.timedelta(seconds=60), ceil=True)

  assert floor60.timestamp() == 1699999980.0
  assert ceil60.timestamp() == 1700000040.0
  assert floor60.tzinfo.zone == 'America/New_York'


def test_np_datetime_to_epoch_converts_units_to_seconds():
  """datetime64 values convert to epoch seconds regardless of the unit."""
  for unit in ('s', 'ms', 'us', 'ns'):
    dt = np.array(['2024-01-01T00:00:00'], dtype=f'datetime64[{unit}]')

    assert du.np_datetime_to_epoch(dt)[0] == 1704067200.0


def test_make_datetime_from_epoch_builds_utc_datetimes():
  """Epoch values become UTC datetimes, convertible to target zones."""
  ts = du.make_datetime_from_epoch(1704067200)

  assert ts == pd.Timestamp('2024-01-01 00:00:00+00:00')

  series = du.make_datetime_from_epoch(pd.Series([1704067200]), tz='America/New_York')

  assert series.iloc[0] == pd.Timestamp('2023-12-31 19:00:00-05:00')


def test_at_time_replaces_time_of_day_components():
  """at_time() replaces time components, keeping date and timezone."""
  dt = du.from_timestamp(1700000000)
  at = du.at_time(dt, hour=9, minute=30)

  assert (at.hour, at.minute, at.second) == (9, 30, 0)
  assert at.date() == dt.date()
  assert at.tzinfo.zone == dt.tzinfo.zone
