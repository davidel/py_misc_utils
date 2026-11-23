"""Infix operator wrapper for pipe-style function composition.

This module defines the InfixOp class, which turns a binary function into
an object usable as an infix operator, so that chains of data
transformations can be written with |, << and >> instead of nested calls.
"""


class InfixOp:
  """Infix operator wrapper for creating pipe-style function composition.

  Allows using binary operators (|, <<, >>) as infix notation for calling
  functions. For example, `InfixOp(add) | 5` creates an operator that adds 5
  to its operand.

  Attributes:
    _opfn: The underlying binary function to wrap.
  """

  def __init__(self, opfn):
    """Initialize with a binary function.

    Args:
      opfn: A callable taking two arguments.
    """
    self._opfn = opfn

  def __ror__(self, lhs):
    """Create a new InfixOp for right-hand operand composition.

    Allows `infix | value` syntax where value becomes the right operand
    of the underlying function.

    Args:
      lhs: The left-hand operand value.

    Returns:
      A new InfixOp that applies the function with lhs as first argument.
    """
    return InfixOp(lambda x, self=self, lhs=lhs: self._opfn(lhs, x))

  def __or__(self, rhs):
    """Apply the underlying function with rhs as the argument.

    Allows `infix | value` syntax to directly call the function.

    Args:
      rhs: The right-hand operand value.

    Returns:
      The result of calling _opfn(rhs).
    """
    return self._opfn(rhs)

  def __rlshift__(self, lhs):
    """Create a new InfixOp for right-hand operand composition via <<.

    Similar to __ror__ but uses the << operator instead of |.

    Args:
      lhs: The left-hand operand value.

    Returns:
      A new InfixOp that applies the function with lhs as first argument.
    """
    return InfixOp(lambda x, self=self, lhs=lhs: self._opfn(lhs, x))

  def __rshift__(self, rhs):
    """Apply the underlying function with rhs as the argument via >>.

    Allows `infix >> value` syntax to directly call the function.

    Args:
      rhs: The right-hand operand value.

    Returns:
      The result of calling _opfn(rhs).
    """
    return self._opfn(rhs)

  def __call__(self, value1, value2):
    """Call the underlying function with two arguments.

    Args:
      value1: The first argument.
      value2: The second argument.

    Returns:
      The result of calling _opfn(value1, value2).
    """
    return self._opfn(value1, value2)
