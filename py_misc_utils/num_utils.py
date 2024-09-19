
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


def nearest_divisor(size, n):
  nmin = n
  while nmin > 1:
    if size % nmin == 0:
      break

    nmin -= 1

  nmax = n
  while nmax * 2 <= size:
    if size % nmax == 0:
      break

    nmax += 1

  if size % nmax != 0:
    return nmin

  dmin, dmax = n - nmin, nmax - n

  return nmin if dmin < dmax else nmax

