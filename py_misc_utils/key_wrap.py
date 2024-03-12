
class KeyWrap:

  def __init__(self, key, value):
    self.key = key
    self.value = value

  def __lt__(self, other):
    return self.key < other.key

  def __le__(self, other):
    return self.key <= other.key

  def __gt__(self, other):
    return self.key > other.key

  def __ge__(self, other):
    return self.key >= other.key

  def __eq__(self, other):
    return self.key == other.key

  def __ne__(self, other):
    return self.key != other.key

  def __hash__(self):
    return hash(self.key)

  def __str__(self):
    return f'key="{key}", value="{value}"'

