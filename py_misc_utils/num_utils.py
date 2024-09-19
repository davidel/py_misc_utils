
def prime_factors(n):
  i = 2
  while i * i <= n:
    if n % i == 0:
      n /= i
      yield i
    else:
      i += 1

  if n > 1:
    yield n


def nearest_divisor(value, n):
  nmin, nmax = n, n

  q, r = divmod(value, nmin)
  if r != 0:
    if n > q:
      cq, nmin = q, 1
      while cq * 2 <= value:
        qn, r = divmod(value, cq)
        if r == 0:
          nmin = qn
          break
        cq += 1

      cq, nmax = q, None
      while cq > 1:
        qn, r = divmod(value, cq)
        if r == 0:
          nmax = qn
          break
        cq -= 1
    else:
      while nmin > 1:
        if value % nmin == 0:
          break
        nmin -= 1

      while nmax * 2 <= value:
        if value % nmax == 0:
          break
        nmax += 1

      if value % nmax != 0:
        nmax = None

  if nmax is None:
    return nmin

  dmin, dmax = n - nmin, nmax - n

  return nmin if dmin < dmax else nmax

