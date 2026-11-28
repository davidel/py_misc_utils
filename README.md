# py_misc_utils

[![CI](https://github.com/davidel/py_misc_utils/actions/workflows/tests.yml/badge.svg)](https://github.com/davidel/py_misc_utils/actions/workflows/tests.yml)
[![Docs](https://github.com/davidel/py_misc_utils/actions/workflows/docs.yml/badge.svg)](https://davidel.github.io/py_misc_utils/)
[![PyPI version](https://img.shields.io/badge/pypi-v0.30-blue.svg)](https://pypi.org/project/python-misc-utils/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)

A collection of Python utility APIs — a toolbox of battle-tested helpers
covering executors, async task management, object caching, filesystem
abstraction, data structures, HTTP clients and servers, type casting and
validation, logging, pipeline processing, and many more general-purpose
helpers.

I keep writing the same stuff in different projects, so I finally decided
to throw stuff into a common boilerplate and stop the cut&paste jobs.

## Features

- **Concurrency** — thread/process `Executor`s, tracking executors, async
  managers, pipeline processing, and work-result plumbing.
- **Data structures** — `OrderedSet`, named arrays, tensor streams, virtual
  arrays, varint coding, and more.
- **Filesystems** — a filesystem abstraction over local, GCS, S3, FTP, and
  archives (`py_misc_utils.fs`), plus streamed and write-back files.
- **HTTP** — a lightweight HTTP server, async URL fetcher, range-request
  helpers, and caching support.
- **Data / numeric** — numpy, pandas, and tensor conversion helpers,
  grouped splits, dtype inference, and casting utilities.
- **Infra** — a leveled async logger (`alog`), assert helpers, application
  main stubs, resource manager, and Git repo helpers.

## Install

The library can be installed using *PyPi*:

```Shell
$ pip install python-misc-utils
```

Or directly from the *Github* repository:

```Shell
$ pip install git+https://github.com/davidel/py_misc_utils.git
```

## Quick start

A few taste-of-the-API examples. See the
[full documentation](https://davidel.github.io/py_misc_utils/) for the
complete reference.

### Executor

```Python
import py_misc_utils.executor as xe

ex = xe.Executor(max_threads=4, min_threads=1, name_prefix='Demo')
res = ex.submit_result(lambda a, b: a + b, 2, 3)
assert res.wait() == 5
ex.submit(print, 'fire and forget')
ex.wait_for_idle()
ex.shutdown()
```

### Logging

The `alog` module extends the standard logging with extra verbosity levels
(*SPAM*, *VERBOSE*, *DEBUG0..3*) and compact log headers.

```Python
import py_misc_utils.alog as alog

alog.basic_setup(log_level=alog.INFO)
alog.info("processing started")
alog.debug2("verbose diagnostics")
```

### Ordered set

```Python
from py_misc_utils import ordered_set

oset = ordered_set.OrderedSet([3, 1, 3, 2])
list(oset.values())  # [3, 1, 2] -- insertion order kept
oset.add(5)
```

### Async HTTP fetcher

```Python
from py_misc_utils import http_async_fetcher as haf

with haf.HttpAsyncFetcher(num_workers=2) as fetcher:
  fetcher.enqueue("https://example.com/data.bin")
  results = dict(fetcher.iter_results())
```

### Pipeline

```Python
from py_misc_utils import pipeline

pline = pipeline.Pipeline(lambda x: x + 1, lambda x: x * 10)
print(pline(5))  # 60
```

## Documentation

The full API reference, auto-generated from the source docstrings, is
published at **[davidel.github.io/py_misc_utils](https://davidel.github.io/py_misc_utils/)**.

## Development

The codebase adheres to **Google Python Style** with **2-space indentation**,
formatted using `yapf` with the repository `.style.yapf` configuration file.
Docstrings follow the **Google style** (`Args:`, `Returns:`, `Raises:`,
sections), which is what the published API documentation is generated from.

API privacy: leading-underscore (`_` prefixed) modules, classes, members,
and functions are internal implementation details. Tests must never poke
into them, and neither should any other code outside the module owning
them.

```Shell
# Check formatting (picks up the .style.yapf config in the repo root)
yapf --diff --recursive py_misc_utils tests

# Apply formatting in-place
yapf --in-place --recursive py_misc_utils tests

# Run linter
ruff check .
```

To build the documentation locally (*MkDocs* + *Material*, with API pages
generated from the source docstrings by *mkdocstrings*):

```Shell
$ pip install -e '.[docs]'
$ mkdocs serve     # live preview at http://localhost:8000
$ mkdocs build     # static build into site/
```

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
