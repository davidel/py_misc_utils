"""Tests for the py_misc_utils.thread_context thread-local stacks.

Expectations follow the documented API contract: entering a Context pushes
its object onto the calling thread's named stack, exiting pops it back,
get_context() returns the innermost object (or None when the stack is
unknown or empty), and stacks are fully independent per thread.
"""

import threading

import py_misc_utils.thread_context as ptc


def test_enter_exit_push_and_pop():
  """Entering pushes the object and get_context() sees it; exit pops it."""
  obj = object()

  assert ptc.get_context('ctx_basic') is None

  with ptc.Context('ctx_basic', obj) as entered:
    assert entered is obj
    assert ptc.get_context('ctx_basic') is obj

  assert ptc.get_context('ctx_basic') is None


def test_nesting_innermost_wins_and_lifo_restore():
  """Nested contexts behave as a stack: innermost shadows, LIFO on exit."""
  outer, inner = object(), object()

  with ptc.Context('ctx_nest', outer):
    assert ptc.get_context('ctx_nest') is outer

    with ptc.Context('ctx_nest', inner):
      assert ptc.get_context('ctx_nest') is inner

    assert ptc.get_context('ctx_nest') is outer

  assert ptc.get_context('ctx_nest') is None


def test_independent_stacks_per_name():
  """Different names keep separate stacks in the same thread."""
  a, b = object(), object()

  with ptc.Context('ctx_a', a), ptc.Context('ctx_b', b):
    assert ptc.get_context('ctx_a') is a
    assert ptc.get_context('ctx_b') is b

  assert ptc.get_context('ctx_a') is None
  assert ptc.get_context('ctx_b') is None


def test_same_object_shared_across_threads():
  """The same Context object gives each thread its own stack entry."""
  ctx = ptc.Context('ctx_thr', 'shared-obj')
  seen = {}
  both_inside = threading.Barrier(3)
  both_leaving = threading.Barrier(3)

  def worker(tag):
    with ctx:
      seen[tag] = ptc.get_context('ctx_thr')
      # Wait until both workers hold their own entry at the same time.
      both_inside.wait(timeout=5)
      seen[tag + '_after'] = ptc.get_context('ctx_thr')
      both_leaving.wait(timeout=5)

    seen[tag + '_exit'] = ptc.get_context('ctx_thr')

  t1 = threading.Thread(target=worker, args=('t1',))
  t2 = threading.Thread(target=worker, args=('t2',))

  t1.start()
  t2.start()

  # The main thread is the third barrier participant: this guarantees both
  # workers are inside the context concurrently before anyone leaves.
  both_inside.wait(timeout=5)
  both_leaving.wait(timeout=5)

  t1.join(timeout=5)
  t2.join(timeout=5)

  assert seen['t1'] == 'shared-obj'
  assert seen['t2'] == 'shared-obj'
  # Overlapping entries do not leak across threads.
  assert seen['t1_after'] == 'shared-obj'
  assert seen['t2_after'] == 'shared-obj'
  assert seen['t1_exit'] is None
  assert seen['t2_exit'] is None


def test_exit_pops_own_thread_entry_not_others():
  """A thread's exit pops only its own stack frame."""
  ctx = ptc.Context('ctx_iso', 'obj')
  results = {}

  def worker(inside, main_exited):
    with ctx:
      inside.set()
      main_exited.wait(timeout=5)

    results['after'] = ptc.get_context('ctx_iso')

  inside = threading.Event()
  main_exited = threading.Event()
  t = threading.Thread(target=worker, args=(inside, main_exited))

  with ctx:
    t.start()
    inside.wait(timeout=5)
    assert ptc.get_context('ctx_iso') == 'obj'
    main_exited.set()

  t.join(timeout=5)

  # The main thread popped its own entry only.
  assert ptc.get_context('ctx_iso') is None
  # The worker recorded its view after popping its own entry.
  assert results['after'] is None
