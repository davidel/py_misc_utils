"""Environment-based configuration with command-line argument overrides.

This module provides the EnvConfig class which allows subclasses to define
configuration variables as class attributes. Values are populated from
environment variables (via ut.getenv) and can be overridden via command-line
arguments (--<name>). Subclasses simply define non-callable attributes with
desired types, and EnvConfig handles the rest.
"""

import argparse

from . import utils as ut


class EnvConfig:
  """Base class for environment-driven configuration with CLI overrides.

  Subclasses define configuration variables as non-callable class attributes.
  On initialization, EnvConfig reads each variable from an environment variable
  of the same name (using ut.getenv with the attribute's type) and registers
  a corresponding --<name> command-line argument. Command-line values override
  environment values.

  Example:
    class MyConfig(EnvConfig):
      timeout = 30
      retries = 3

    config = MyConfig()  # Reads TIMEOUT and RETRIES env vars, accepts
                         # --timeout and --retries CLI args.
  """

  def __init__(self):
    """Initialize the configuration by reading env vars and parsing CLI args.

    For each non-callable, non-private attribute on self, reads the value from
    the corresponding environment variable and adds a --<name> argument to an
    argparse parser. Known CLI args override env var values.
    """
    parser = argparse.ArgumentParser()
    state = dict()
    for name in dir(self):
      if not name.startswith('_'):
        value = getattr(self, name)
        # Do not try to override functions (even though there really should not
        # be in an EnvConfig derived object).
        if not callable(value):
          state[name] = ut.getenv(name, dtype=type(value))
          parser.add_argument(f'--{name}', type=type(value))

    args, _ = parser.parse_known_args()
    for name, value in state.items():
      avalue = getattr(args, name, None)
      avalue = value if avalue is None else avalue
      if avalue is not None:
        setattr(self, name, avalue)

  def __repr__(self):
    """Return a string representation of the configuration variables.

    Collects all non-callable, non-private attributes and their current values
    into a dict, then formats it using ut.stri().

    Returns:
      A string representation of the configuration state.
    """
    cvars = dict()
    for name in dir(self):
      if not name.startswith('_'):
        cvars[name] = getattr(self, name)

    return ut.stri(cvars)
