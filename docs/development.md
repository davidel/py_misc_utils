# Development

## Formatting and linting

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

## Building the documentation locally

The documentation is built with *MkDocs* using the *Material* theme, with
API pages generated from the source docstrings by *mkdocstrings*:

```Shell
$ pip install -e '.[docs]'
$ mkdocs serve     # live preview at http://localhost:8000
$ mkdocs build     # static build into site/
```
