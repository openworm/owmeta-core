class InRange(object):
    """ A range between values """
    def __init__(self, minval=None, maxval=None, **kwargs):
        super(InRange, self).__init__(**kwargs)
        self.max_value = maxval
        self.min_value = minval

        # Ensure that the if max and min are both specified, that they have
        # some parent type in common
        if self.max_value is not None and self.min_value is not None:
            assert(isinstance(self.max_value, type(self.min_value))
                   or isinstance(self.min_value, type(self.max_value)))

    def __call__(self, val):
        if ((self.max_value is not None and not isinstance(val, type(self.max_value))) or
                (self.min_value is not None and not isinstance(val, type(self.min_value)))):
            raise Exception("Must have the same type for range and values")

        return ((self.max_value is None or self.max_value > val)
                and (self.min_value is None or self.min_value < val))

    @property
    def defined(self):
        return self.max_value is not None or self.min_value is not None


class LessThan(InRange):
    def __init__(self, maxval=float('+inf')):
        super(LessThan, self).__init__(maxval=maxval)


class GreaterThan(InRange):
    def __init__(self, minval=float('-inf')):
        super(GreaterThan, self).__init__(minval=minval)
