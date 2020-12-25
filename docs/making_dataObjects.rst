.. _making_dataObjects:

Making data objects
====================
To make a new object type like you just need to make a subclass of
`~owmeta_core.dataobject.DataObject` with the appropriate.

Say, for example, that I want to record some information about drug reactions
in dogs. I make ``Drug``, ``Experiment``, and ``Dog`` classes to describe drug
reactions::

    >>> from owmeta_core.dataobject import (DataObject,
    ...                                     DatatypeProperty,
    ...                                     ObjectProperty,
    ...                                     Alias)
    >>> from owmeta_core.context import Context
    >>> from owmeta_core.mapper import Mapper

    >>> module_context = 'http://example.com/animals'

    >>> class Dog(DataObject):
    ...     breed = DatatypeProperty()

    >>> class Drug(DataObject):
    ...     name = DatatypeProperty()
    ...     drug_name = Alias(name)
    ...     key_property = 'name'
    ...     direct_key = True

    >>> class Experiment(DataObject):
    ...     drug = ObjectProperty(value_type=Drug)
    ...     subject = ObjectProperty(value_type=Dog)
    ...     route_of_entry = DatatypeProperty()
    ...     reaction = DatatypeProperty()

    # Do some accounting stuff to register the classes. Usually happens behind
    # the scenes.
    >>> m = Mapper()
    >>> m.process_classes(Drug, Experiment, Dog)

So, we have created I can then make a Drug object for moon rocks and describe an experiment by
Aperture Labs::

    >>> ctx = Context('http://example.org/experiments', mapper=m)
    >>> d = ctx(Drug)(name='moon rocks')
    >>> e = ctx(Experiment)(key='experiment001')
    >>> w = ctx(Dog)(breed='Affenpinscher')
    >>> e.subject(w)
    owmeta_core.statement.Statement(...Context(.../experiments"))

    >>> e.drug(d)
    owmeta_core.statement.Statement(...)

    >>> e.route_of_entry('ingestion')
    owmeta_core.statement.Statement(...)

    >>> e.reaction('no reaction')
    owmeta_core.statement.Statement(...)

and save those statements::

    >>> ctx.save()

For simple objects, this is all we have to do.

You can also add properties to an object after it has been created by calling
either ObjectProperty or DatatypeProperty on the class::

    >>> d = ctx(Drug)(name='moon rocks')
    >>> Drug.DatatypeProperty('granularity', owner=d)
    __main__.Drug_granularity(owner=Drug(ident=rdflib.term.URIRef('http://data.openworm.org/Drug#moon%20rocks')))

    >>> d.granularity('ground up')
    owmeta_core.statement.Statement(...Context(.../experiments"))

    >>> do = Drug()

Properties added in this fashion will not propagate to any other objects::

    >>> do.granularity
    Traceback (most recent call last):
        ...
    AttributeError: 'Drug' object has no attribute 'granularity'


They will, however, be saved along with the object they are attached to.
