"""Tests for py_misc_utils.uncompress.

Expectations pinned to the observed behavior: Uncompress is a context
manager that passes through files with unrecognized extensions
unchanged. NOTE: decompressing .gz/.bz2/.xz files is currently broken
because __enter__() calls the (processor, module) tuple returned by
comp.decompressor() as if it were callable (library bug — reported,
see SCRATCH/tools/probe_uncompress_bug.py).
"""

import os

from py_misc_utils import uncompress


def test_uncompress_passthrough_unrecognized(tmp_path):
  """Uncompress passes through files with unrecognized extensions."""
  src = os.path.join(tmp_path, 'data.xyz')
  original = b'plain data'
  with open(src, 'wb') as fd:
    fd.write(original)

  with uncompress.Uncompress(src) as path:
    assert path == src
    with open(path, 'rb') as fd:
      assert fd.read() == original


def test_uncompress_tempdir_not_created_for_passthrough(tmp_path):
  """Uncompress creates no temp directory for unrecognized extensions."""
  src = os.path.join(tmp_path, 'data.xyz')
  original = b'plain data'
  with open(src, 'wb') as fd:
    fd.write(original)

  with uncompress.Uncompress(src) as path:
    assert path == src
    # No tempdir should be created; the path is the original file.
