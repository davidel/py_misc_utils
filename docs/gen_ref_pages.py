# Auto-generate the API reference pages: one page per module, plus a
# SUMMARY.md consumed by the literate-nav plugin.
#
# mkdocs_gen_files.open() resolves paths relative to the docs directory,
# while literate-nav links inside reference/SUMMARY.md are relative to
# that file's own directory (reference/).
from pathlib import Path

import mkdocs_gen_files

PACKAGE = "py_misc_utils"

nav = mkdocs_gen_files.Nav()

for path in sorted(Path(PACKAGE).rglob("*.py")):
  module_path = path.relative_to(PACKAGE).with_suffix("")
  parts = list(module_path.parts)

  # The package (or subpackage) root __init__ documents its own name.
  if parts[-1] == "__init__":
    parts = parts[:-1]
    if not parts:
      continue

  doc_rel = Path(*parts).with_suffix(".md")
  full_doc_path = Path("reference", doc_rel)

  with mkdocs_gen_files.open(full_doc_path, "w") as fd:
    fd.write(f"# `{parts[-1]}`\n\n::: {PACKAGE}.{'.'.join(parts)}\n")

  mkdocs_gen_files.set_edit_path(full_doc_path, path)

  nav[parts] = doc_rel.as_posix()

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as fd:
  fd.write(''.join(nav.build_literate_nav()))
