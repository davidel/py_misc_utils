import math


def exp_bias(nx):
  return (1 << (nx - 1)) - 1


def real_to_bits(v, nx, nm):
  xm, xe = math.frexp(math.fabs(v))

  e = (xe + exp_bias(nx) - 1) if xm != 0 else 0
  m = int(xm * (1 << (nm + 1)))

  return 1 if v < 0 else 0, e, m


def pack_bits(s, e, m, nx, nm):
  return (s << (nx + nm)) | (e << nm) | m


def real_to_packedbits(v, nx, nm):
  s, e, m = real_to_bits(v, nx, nm)

  return pack_bits(s, e, m, nx, nm)


def _bits(v, pos, n):
  return (v >> pos) & ((1 << n) - 1)


def packedbits_to_real(v, nx, nm):
  s, e, m = _bits(v, nx + nm, 1), _bits(v, nm, nx), _bits(v, 0, nm)

  if e == 0 and m == 0:
    return 0.0
  if e == ((1 << nx) - 1):
    return math.inf if m == 0 else math.nan

  xm = float(m | (1 << nm))
  re = e - exp_bias(nx) - nm - 1
  if re >= 0:
    rm = xm * (1 << re)
  else:
    rm = xm / (1 << (-re))

  return rm if s == 0 else -rm

