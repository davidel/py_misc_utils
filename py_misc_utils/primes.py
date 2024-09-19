
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

