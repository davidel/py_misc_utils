import ast
import logging
import os


def _ends_with_return(slist):
  return slist and isinstance(slist[-1], ast.Return)


def _ifize_stmt_list(slist):
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
    if _ends_with_return(ifnode.body):
      if not _ends_with_return(orelse):
        remlist = _ifize_stmt_list(slist[ni + 1:])
        orelse.extend(remlist)
        ifnode.orelse = orelse
        slist = slist[: ni + 1]

    elif _ends_with_return(orelse):
      remlist = _ifize_stmt_list(slist[ni + 1:])
      ifnode.body.extend(remlist)
      slist = slist[: ni + 1]

  return slist


def ifize(node):
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
  if indent is None:
    indent = os.getenv('AST_INDENT', None)
    if indent is not None:
      indent = int(indent)

  return ast.dump(node, indent=indent)


def static_eval(node, eval_globals, eval_locals, filename=None):
  if isinstance(node, ast.stmt):
    mod = ast.Module(body=[node], type_ignores=[])
    cmod = compile(mod, filename=filename or 'static_eval', mode='exec')
    exec(cmod, eval_globals, eval_locals)
  elif isinstance(node, ast.expr):
    expr = ast.Expression(body=node)
    cexpr = compile(expr, filename=filename or 'static_eval', mode='eval')
    value = eval(cexpr, eval_globals, eval_locals)

    return value


class Tranformer(ast.NodeTransformer):

  def __init__(self, trans_fn):
    super().__init__()
    self.trans_fn = trans_fn

  def generic_visit(self, node):
    super().generic_visit(node)
    return self.trans_fn(node)

