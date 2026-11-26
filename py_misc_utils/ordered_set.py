"""Ordered set implementation maintaining insertion order.

This module provides an OrderedSet class that combines set semantics
(uniqueness) with list semantics (insertion order preservation).
"""

import copy


class OrderedSet:
  """A set that maintains insertion order of elements.

  Supports standard set operations (add, remove, discard, pop, clear)
  and set algebra (union, intersection, difference).
  Iteration yields elements in insertion order.
  """

  def __init__(self, init=None):
    """Initialize an ordered set.

    Args:
      init: Optional iterable of initial values.
    """
    self._data = dict()
    self._seqno = 0

    for value in init or ():
      self.add(value)

  def add(self, value):
    """Add a value to the set.

    Args:
      value: Value to add.

    Returns:
      The sequence number assigned to this value (0 for first, etc.).
      Returns existing sequence number if value already present.
    """
    n = self._data.get(value)
    if n is None:
      self._data[value] = n = self._seqno
      self._seqno += 1

    return n

  def remove(self, value):
    """Remove a value from the set.

    Args:
      value: Value to remove.

    Raises:
      KeyError: If value is not in the set.
    """
    self._data.pop(value)

  def discard(self, value):
    """Remove a value from the set if present.

    Args:
      value: Value to remove (no error if absent).
    """
    self._data.pop(value, None)

  def pop(self):
    """Remove and return the most recently added value.

    Returns:
      The most recently added value.

    Raises:
      KeyError: If the set is empty.
    """
    value, _ = self._data.popitem()

    return value

  def clear(self):
    """Remove all values from the set."""
    self._data = dict()
    self._seqno = 0

  def __len__(self):
    """Return the number of elements in the set."""
    return len(self._data)

  def values(self):
    """Iterate over values in insertion order.

    Returns:
      Generator yielding values in order of insertion.
    """
    return (y[0] for y in sorted(self._data.items(), key=lambda x: x[1]))

  def __iter__(self):
    """Iterate over values in insertion order."""
    return iter(self.values())

  def __contains__(self, value):
    """Check if a value is in the set."""
    return value in self._data

  def __copy__(self):
    """Copy protocol implementation: clone the internal state.

    Returns:
      New OrderedSet with the same elements and order, holding its own
      internal state.
    """
    nos = OrderedSet()
    nos._data = self._data.copy()
    nos._seqno = self._seqno

    return nos

  def copy(self):
    """Create a shallow copy of the ordered set.

    Returns:
      New OrderedSet with the same elements and order, holding its own
      internal state.
    """
    return copy.copy(self)

  def union(self, *others):
    """Return the union of this set with others.

    Args:
      *others: Other iterables to union with.

    Returns:
      New OrderedSet containing all unique elements, preserving order
      from this set followed by new elements from others.
    """
    nos = copy.copy(self)
    for other in others:
      for value in other:
        nos.add(value)

    return nos

  def intersection(self, *others):
    """Return the intersection of this set with others.

    Args:
      *others: Other iterables to intersect with.

    Returns:
      New OrderedSet containing only elements present in all sets,
      preserving order from this set.
    """
    nos = OrderedSet()
    for value in self.values():
      if all(value in other for other in others):
        nos.add(value)

    return nos

  def difference(self, *others):
    """Return the difference of this set with others.

    Args:
      *others: Other iterables to subtract.

    Returns:
      New OrderedSet containing elements in this set but not in any
      of the others, preserving order from this set.
    """
    nos = OrderedSet()
    for value in self.values():
      if not any(value in other for other in others):
        nos.add(value)

    return nos
