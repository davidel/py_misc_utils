"""Infix operator wrapper for pipe-style function composition.

This module defines the InfixOp class, which turns a binary function into
an object usable as an infix operator, so that chains of data
transformations can be written with |, << and >> instead of nested calls.
"""


class InfixOp:
  """Infix operator wrapper for creating pipe-style function composition.

  Allows using binary operators (|, <<, >>) as infix notation for calling
  functions.

  A binary function wrapped in an InfixOp is applied by writing the two
  operands around it: `x | op | y` binds x as the left operand and applies
  the function to y. `x << op` is a synonym of `x | op`, and `(x | op) >> y`
  is the >> form of the final application.

  Example:
    add = infix_op.InfixOp(lambda a, b: a + b)
    5 | add | 3  # 8 - equivalent to add(5, 3).
    (5 | add) >> 3  # Same result via the >> form.
    add(2, 3)  # 5 - an InfixOp is also directly callable.
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
