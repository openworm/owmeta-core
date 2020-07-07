from six import with_metaclass
from .data import DataUser
from .contextualize import Contextualizable, ContextualizableClass


class CustomPropertyMeta(ContextualizableClass):
    def __init__(self, name, bases, dct):
        super(CustomPropertyMeta, self).__init__(name, bases, dct)
        self.__context = None

    @property
    def context(self):
        return self.__context

    @context.setter
    def context(self, ctx):
        self.__context = ctx


class CustomProperty(with_metaclass(CustomPropertyMeta, Contextualizable, DataUser)):
    """ Store a value associated with a DataObject

    Properties can be be accessed like methods. A method call like::

        a.P()

    for a property ``P`` will return values appropriate to that property for
    ``a``, the `owner` of the property.

    Parameters
    ----------
    owner : owmeta_core.dataobject.DataObject
        The owner of this property
    name : string
        The name of this property. Can be accessed as an attribute like::

            owner.name
    """

    # Indicates whether the CustomProperty is multivalued
    multiple = False

    def __init__(self, name=False, owner=False, **kwargs):
        super(CustomProperty, self).__init__(**kwargs)
        self.owner = owner
        self.linkName = name
        if self.owner:
            if name:
                setattr(self.owner, name, self)

    @property
    def values(self):
        return []

    def get(self, *args):
        """ Get the things which are on the other side of this property

        The return value must be iterable. For a ``get`` that just returns
        a single value, an easy way to make an iterable is to wrap the
        value in a tuple like ``(value,)``.

        Derived classes must override.
        """
        # This should run a query or return a cached value
        raise NotImplementedError()

    def set(self, *args, **kwargs):
        """ Set the value of this property

        Derived classes must override.
        """
        # This should set some values and call DataObject.save()
        raise NotImplementedError()

    get_terms = get

    def one(self):
        """
        Returns a single value for the `CustomProperty` whether or not it is multivalued.
        """

        r = self.get()
        return next(iter(r), None)

    def has_value(self):
        """
        Returns true if the `CustomProperty` has any values set on it.

        This may be defined differently for each property
        """
        return True

    def __call__(self, *args, **kwargs):
        """
        If arguments are passed to the `CustomProperty`, its :meth:`set` method is called.
        Otherwise, the `get` method is called. If the ``multiple`` member for the
        `CustomProperty` is set to `True`, then a Python :py:func:`set` containing the
        associated values is returned. Otherwise, a single value is returned.
        """

        if len(args) > 0 or len(kwargs) > 0:
            self.set(*args, **kwargs)
            return self
        else:
            r = self.get(*args, **kwargs)
            if self.multiple:
                return set(r)
            else:
                return next(iter(r), None)
