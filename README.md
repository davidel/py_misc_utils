## Python Utility Code

I keep writing the same stuff in different projects, so I finally decided
to throw stuff into a common boilerplate and stop the cut&paste jobs.

## Install

The library can be installed using *PyPi*:

```Shell
$ pip install python-misc-utils
```

Or directly from the *Github* repository:

```Shell
$ pip install git+https://github.com/davidel/py_misc_utils.git
```

## Known issues

Bugs spotted while reviewing the codebase, kept here until they are fixed
or deliberately accepted:

- `archive_streamer.ArchiveStreamer`: archive URLs with a compression
  extension - like `data.tar.gz` or `data.parquet.gz` - do not work:
  `parse_specs()` reports the compression extension (`gz`, `xz`, `bz2`)
  as the archive `kind`, shadowing the archive type, so the streamer fails
  with `Unknown archive type "gz"`. Only the last extension is ever
  inspected as the kind, so plain `data.tar.gz` (a *tar* archive
  compressed with *gzip*, as the module docstring advertises) cannot be
  streamed.

- `object_tracker.track_objects()`: the object scan can crash with
  `NotImplementedError: Iteration over this class is unlikely to be
  threadsafe` when a `urllib3` `PoolManager` is alive in the process (for
  example after any `gfs.open()` call, which imports `requests`). The
  urllib3 module-level `RecentlyUsedContainer._DEFAULT_POOL` instance is
  matched by the mapping check in `_get_referred()`, and its `items()`
  raises - *outside* the per-object try/except block of
  `track_objects()`, aborting the whole scan instead of skipping the
  offending object.

- `pd_utils.load_dataframe(dtype=<single type>)`: crashes on pandas 3.x
  when the frame holds string columns - `get_typed_columns()` feeds the
  pandas `StringDtype` to `np.issubdtype()` (via
  `np_utils.is_numeric()`), which raises `TypeError: Cannot interpret
  '<StringDtype ...>'`. Passing a *dict* of per-column dtypes works as a
  workaround.

- `skl_utils.WeighedKNNClassifier.predict_proba()`: broadcast error when
  `sample_weight` is set at fit time - the aggregation indexes the
  training-set weights with the neighbor loop counter
  (`pp.sample_weight[:, k]`) instead of gathering them per prediction
  (`pp.sample_weight[pp.neigh_ind, k]`), as `predict()` correctly does.
  It can also fail on single-row predictions with `n_neighbors=1`.
  `predict()` is unaffected.
