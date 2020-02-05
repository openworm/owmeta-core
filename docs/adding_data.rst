.. _adding_data:

Adding Data to *YOUR* owmeta-core Database
==========================================

Contexts
--------
In natural languages, our statements are made in a context that influences how
they should be interpreted. In |owm|, that kind of context-sensitivity is
modeled by using :class:`owmeta_core.context.Context` objects. To see what this
looks like, let's start with an example.

Basics
^^^^^^
Say I have data about widgets from BigDataWarehouse (BDW) that I want to
translate into RDF using |owm|, but I don't want put them with my other widget
data since BDW data may conflict with mine. Also, if get more BDW data, I want
to be able to relate these data to that. A good way to keep data which are made
at distinct times or which come from different, possibly conflicting, sources
is using contexts. The code below shows how to do that::

   >>> from rdflib import ConjunctiveGraph
   >>> from owmeta_core.context import Context
   >>> # from mymod import Widget  # my own OWM widget model
   >>> # from bdw import Load # BigDataWarehouse API

   >>> # Create a Context with an identifier appropriate to this BDW data import
   >>> ctx = Context('http://example.org/data/imports/BDW_Widgets_2017-2018')

   >>> # Create a context manager using the default behavior of reading the
   >>> # dictionary of current local variables
   >>> with ctx(W=Widget) as c:
   ...     for record in Load(data_set='Widgets2017-2018'):
   ...         # declares Widgets in this context
   ...         c.W(part_number=record.pnum,
   ...             fullness=record.flns,
   ...             hardiness=record.hrds)
   Widget(ident=rdflib.term.URIRef(...))

   
   >>> # Create an RDFLib graph as the target for the data
   >>> g = ConjunctiveGraph()

   >>> # Save the data
   >>> ctx.save(g)

   >>> # Serialize the data in the nquads format so we can see that all of our
   >>> # statements are in the proper context
   >>> print(g.serialize(format='nquads').decode('UTF-8'))
   <http://openworm.org/entities/Widget/12> <http...> <http://example.org/data/imports/BDW_Widgets_2017-2018> .
   <http://openworm.org/entities/Widget/12> <...

If you've worked with lots of data before, this kind of pattern should be
familiar. You can see how, with later imports, you would follow the naming
scheme to create new contexts (e.g.,
``http://example.org/data/imports/BDW_Widgets_2018-2019``). These additional
contexts could then have separate metadata attached to them or they could be
compared::

   >>> len(list(ctx(Widget)().load()))
   1
   >>> len(list(ctx18(Widget)().load()))  # 2018-2019 context
   3

Context Metadata
^^^^^^^^^^^^^^^^
Contexts, because they have identifiers just like any other objects, so we can
make statements about them as well. An essential statement is imports: Contexts
import other contexts, which means, if you follow owmeta_core semantics, that
when you query objects from the importing context, that the imported contexts
will also be available to query. In other words, the statements in the imported
context are entailed by the imports statement.

.. Importing contexts
.. Evidence, DataSources, DataTranslators, Provenance and contexts

