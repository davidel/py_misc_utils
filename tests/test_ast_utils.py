"""Tests for py_misc_utils.ast_utils.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): ifize() transforms the AST in place (returns None) -
statements following an IF with an early-return branch move into the
other branch (orelse), linearizing control flow; dump() renders an AST
honoring an env-driven indentation (AST_INDENT); static_eval() executes
an ast.stmt in the given globals/locals and evaluates an ast.expr
returning its value.
"""

import ast

import pytest

from py_misc_utils import ast_utils as au


def _parse(src, mode='exec'):
  return ast.parse(src, mode=mode)


def test_ifize_moves_post_if_statements():
  """Statements after an early-return IF move into the open branch."""
  tree = _parse('''
def fn(x):
  if x > 0:
    return 1
  print(x)
  return 2
''')
  au.ifize(tree)

  ifnode = tree.body[0].body[0]
  assert [type(s).__name__ for s in ifnode.body] == ['Return']
  assert [type(s).__name__ for s in ifnode.orelse] == ['Expr', 'Return']
  assert ast.unparse(ifnode.orelse[0]) == 'print(x)'


def test_ifize_noop_without_return_branch():
  """An IF whose branches do not end with return keeps following
  statements in place."""
  tree = _parse('''
def fn(x):
  if x:
    print(1)
  print(2)
''')
  au.ifize(tree)

  fn = tree.body[0]
  assert len(fn.body) == 2
  assert type(fn.body[1]).__name__ == 'Expr'


def test_ifize_stabilizes():
  """A second ifize on the transformed tree changes nothing further."""
  tree = _parse('''
def fn(x):
  if x > 0:
    return 1
  print(x)
  return 2
''')
  au.ifize(tree)
  kinds_first = ast.dump(tree, indent=None)

  au.ifize(tree)
  assert ast.dump(tree, indent=None) == kinds_first


def test_dump_flat():
  """dump() renders a one-line dump without indentation."""
  node = _parse('x = 1')
  text = au.dump(node)

  assert 'Assign' in text
  assert '\n  ' not in text


def test_dump_indented(monkeypatch):
  """dump(indent=N) renders a multi-line indented dump."""
  node = _parse('x = 1')
  text = au.dump(node, indent=2)

  assert '\n' in text
  assert 'Assign' in text


def test_dump_env_indent(monkeypatch):
  """AST_INDENT env var drives the default indentation."""
  monkeypatch.setenv('AST_INDENT', '4')
  node = _parse('x = 1')

  assert '\n' in au.dump(node)


def test_static_eval_expr():
  """static_eval evaluates an expression node and returns its value."""
  node = _parse('1 + 2 * 3', mode='eval')

  assert au.static_eval(node.body, {}, {}) == 7


def test_static_eval_stmt():
  """static_eval executes a statement in the given namespaces."""
  node = _parse('res = a + b')
  env = dict(a=1, b=2)
  locs = {}

  au.static_eval(node.body[0], env, locs)

  assert locs['res'] == 3


def test_static_eval_invalid_node():
  """A non-statement/non-expression node raises ValueError."""
  with pytest.raises(ValueError):
    au.static_eval(ast.parse('x = 1'), {}, {})
