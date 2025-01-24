
def prime_factors(n):
  i = 2
  while i * i <= n:
    q, r = divmod(n, i)
    if r == 0:
      n = q
      yield i
    else:
      i += 1

  if n > 1:
    yield n


def nearest_divisor(value, n):
  nmin, nmax = n, n

  while nmin > 1:
    if value % nmin == 0:
      break
    nmin -= 1

  dmin = n - nmin
  top_n = min(n + dmin, value // 2)

  while nmax <= top_n:
    if value % nmax == 0:
      break
    nmax += 1

  if value % nmax != 0:
    return nmin

  return nmin if dmin < (nmax - n) else nmax


def sign_extend(value, nbits):
  sign = 1 << (nbits - 1)

  return (value & (sign - 1)) - (value & sign)


def round_up(v, step):
  return ((v + step - 1) // step) * step


def round_down(v, step):
  return (v // step) * step


def mix(a, b, gamma):
  return a * gamma + b * (1.0 - gamma)

