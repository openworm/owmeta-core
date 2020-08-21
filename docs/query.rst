.. _query:

Querying for data objects
=========================

DataObject query form
---------------------
Sub-classes of `~owmeta_core.dataobject.DataObject` have a ``query`` attribute
that provides a modified form of the class which is fit for creating instances
used in queries. The query form may do other things later, but, principally, it
overrides identifier generation based on attributes (see
`~owmeta_core.identifier_mixin.IdMixin`).

For example, to query for a :py:class:`~owmeta.neuron.Neuron` object with the
name "AVAL" you would instantiate the `Neuron` like this::

   >>> Neuron.query(name='AVAL')

Although it is possible to include instances without the query form, it is
generally preferred to the basic form since later versions of a class may
change how they generate identifiers while keeping property URIs and RDF types
the same (or declaring new ones as sub-properties or sub-classes). Use of the
query form is also recommended when a class generates identifiers based on some
number of properties, but a subclass doesn't use the superclass identifier
scheme (:py:class:`~owmeta.cell.Cell` and `Neuron` are an example). The query
form allows to query for instances of the superclass for subclass instances.
