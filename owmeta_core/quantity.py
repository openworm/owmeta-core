import functools


@functools.total_ordering
class Quantity:
    _ur = None  # Unit registry

    @classmethod
    def ur(cls):
        # importing pint and initialiing the UnitRegistry adds about a second
        # to the import time, so we delay it until it's needed
        import pint as Q
        if cls._ur is None:
            cls._ur = Q.UnitRegistry()

        return cls._ur

    @classmethod
    def parse(self, s):
        q = self.ur().Quantity(s)
        my_q = Quantity(0, "mL")
        my_q._quant = q
        return my_q

    def __init__(self, value, unit):
        self._quant = self.ur().Quantity(value, unit)

    @property
    def unit(self):
        return str(self._quant.units)

    @property
    def value(self):
        return self._quant.magnitude

    def __str__(self):
        return str(self._quant)

    def __repr__(self):
        return repr(self._quant)

    def __eq__(self, other):
        if isinstance(other, Quantity):
            return (id(self) == id(other)) or (self._quant == other._quant)
        else:
            return False

    def __lt__(self, other):
        if isinstance(other, Quantity):
            return self._quant < other._quant
        else:
            return NotImplemented

    def __hash__(self):
        return hash(self._quant)
