"""AST manipulation helpers.

This module provides helpers to manipulate Python ASTs: ifize() moves
statements following an IF into the IF branch not ending with a return -
making control flow linear for code generators - plus an AST dump
utility honoring an environment-driven indentation and a static AST
evaluator executing statements/expressions.

Example:
  import ast
  from py_misc_utils import ast_utils as au

  tree = ast.parse('a + b', mode='eval')

  assert 'BinOp' in au.dump(tree, indent=2)

  # Statements execute into the given namespaces...
  node = ast.parse('res = a + b').body[0]
  locs = dict()
  au.static_eval(node, dict(a=1, b=2), locs)
  assert locs['res'] == 3

  # ...and expressions evaluate to their value.
  v = au.static_eval(ast.parse('a * b', mode='eval').body, dict(a=3, b=4), {})
  assert v == 12
"""
import ast
import os

from . import alog


def _ends_with_return(slist):
  """Checks whether a statement list ends with a return statement."""
  return slist and isinstance(slist[-1], ast.Return)


def _ifize_stmt_list(slist):
  """Moves the statements following an IF into its open branch.

  If one branch of the first IF of a statement list ends with a RETURN,
  and the other does not, the statements following the IF are moved
  within the branch which does not end with the RETURN - recursively
  processing the moved statements.
  """
  ni = None
  for i, node in enumerate(slist):
    if isinstance(node, ast.If):
      ni = i
      break

  if ni is not None and ni + 1 < len(slist):
    ifnode = slist[ni]
    orelse = ifnode.orelse or []

    # If one branch of an IF ends with RETURN, and the other does not,
    # move the remaining of the statements after the IF, within the branch
    # which does not have the RETURN.
    # Example (open 'else'), turns:
    #
    #   op1
    #   if cond:
    #     op2
    #     return x
    #   else:
    #     op3
    #   op4
    #   return y
    #
    # Into:
    #   op1
    #   if cond:
    #     op2
    #     return x
    #   else:
    #     op3
    #     op4
    #     return y
    #
    # Example (open 'if'), turns:
    #
    #   op1
    #   if cond:
    #     op2
    #   else:
    #     op3
    #     return x
    #   op4
    #   return y
    #
    # Into:
    #   op1
    #   if cond:
    #     op2
    #     op4
    #     return y
    #   else:
    #     op3
    #     return x
    #
    # IOW, it makes sure that if within an AST instruction list, the first IF (if any),
    # will be the last instruction of the list.
    if _ends_with_return(ifnode.body):
      if not _ends_with_return(orelse):
        remlist = _ifize_stmt_list(slist[ni + 1:])
        orelse.extend(remlist)
        ifnode.orelse = orelse
        slist = slist[:ni + 1]

    elif _ends_with_return(orelse):
      remlist = _ifize_stmt_list(slist[ni + 1:])
      ifnode.body.extend(remlist)
      slist = slist[:ni + 1]

  return slist


def ifize(node):
  """Normalizes an AST so that each IF terminates its statement list.

  Recursively rearranges all the statement lists held by an AST,
  applying the _ifize_stmt_list() transformation. The transformation
  mutates the AST in place and returns None.

  Args:
    node: The AST node to transform - e.g. a module or a function
      definition.
  """
  for field, value in ast.iter_fields(node):
    if isinstance(value, list):
      for lvalue in value:
        if isinstance(lvalue, ast.AST):
          ifize(lvalue)

      xlist = _ifize_stmt_list(value)
      if xlist is not value:
        setattr(node, field, xlist)
    elif isinstance(value, ast.AST):
      ifize(value)


def dump(node, indent=None):
  """Returns the string representation of an AST node.

  Args:
    node: The AST node to dump.
    indent: An optional indentation of the dumped fields. If None, the
      AST_INDENT environment variable is used - if set - and the
      one-line ast.dump() representation otherwise.

  Returns:
    The dumped AST node.
  """
  if indent is None:
    indent = os.getenv('AST_INDENT')
    if indent is not None:
      indent = int(indent)

  return ast.dump(node, indent=indent)


def static_eval(node, eval_globals, eval_locals, filename=None):
  """Evaluates an AST statement or expression.

  Note that this is not a sandbox: the node is compiled and executed,
  so arbitrary code can run.

  Args:
    node: The AST node to evaluate: a statement or an expression.
    eval_globals: The globals used to evaluate the node.
    eval_locals: The locals used to evaluate the node.
    filename: An optional filename used to compile the node.

  Returns:
    The evaluation result of expression nodes. Statement nodes return
    no value - their effects are applied to the given globals/locals.
  """
  if isinstance(node, ast.stmt):
    mod = ast.Module(body=[node], type_ignores=[])
    cmod = compile(mod, filename=filename or '<ast_eval>', mode='exec')
    # ast_eval exists to execute caller-supplied AST nodes: exec is its
    # documented contract (the expression branch below uses eval likewise).
    exec(cmod, eval_globals, eval_locals)  # noqa: S102
  elif isinstance(node, ast.expr):
    expr = ast.Expression(body=node)
    cexpr = compile(expr, filename=filename or '<ast_eval>', mode='eval')
    value = eval(cexpr, eval_globals, eval_locals)

    return value
  else:
    alog.xraise(ValueError, f'Invalid AST node: {dump(node)}')
