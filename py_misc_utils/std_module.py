"""Standard library module detection utilities.

This module provides :func:`is_std_module` to determine whether a module
belongs to the Python standard library, using :data:`sys.stdlib_module_names`
(Python 3.10+).
"""

import functools
import sys

from . import core_utils as cu


@functools.cache
def is_std_module(modname):
  """Check whether a module belongs to the Python standard library.

  Modules without an origin (built-in) are considered stdlib.

  Args:
    modname: The module name to check (dotted paths use the root module).

  Returns:
    True if the module is a stdlib module, False otherwise.

  Example:
    std_module.is_std_module('os')      # True
    std_module.is_std_module('numpy')   # False
  """
  modname = cu.root_module(modname)
  return modname in sys.stdlib_module_names
