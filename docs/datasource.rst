.. _datasource:

Data Sources and Transformers
=============================
Data Sources and Data Transformers, hereafter just called "Sources" and
"Transformers", are a simple abstraction for describing how resources are
derived from others. A Source is described with a
`owmeta_core.datasource.DataSource` instance. Information for accessing the
underlying data, whether it be a CSV file or a database record, are written
into the properties of the `~owmeta_core.datasource.DataSource`. For instance,
the `~owmeta_core.data_trans.local_file_ds.LocalFileDataSource` has a
`~owmeta_core.data_trans.local_file_ds.LocalFileDataSource.file_name` property
for the name of the file underlying the Source. Here's how it my be declared::

    >>> from rdflib.namespace import Namespace
    >>> from owmeta_core.command import OWM
    >>> from owmeta_core.context import Context
    >>> from owmeta_core.data_trans.local_file_ds import LocalFileDataSource as LFDS

    >>> # Set up basic project info
    >>> ex = Namespace('http://example.org/')
    >>> owm = OWM(non_interactive=True)
    >>> owm.init(default_context_id=ex.music_db)
    Initialized owmeta-core project at .../.owm

    >>> owm.save('owmeta_core.data_trans.local_file_ds')
    <...>

    >>> with owm.connect().transaction() as conn, owm.default_context as defctx:
    ...     defctx(LFDS)(
    ...         ident=ex.lsd_wiki, file_name='LakeStreetDive.wiki')
    LocalFileDataSource(ident=rdflib.term.URIRef('http://example.org/lsd_wiki'))


Transforming Sources
--------------------
A Source can be transformed into another Source by using a Transformer.
Transforming doesn't generally "destroy" the original Source, so the original
and the new Source will both be available. The chief advantage of describing
how a Source is derived using a Transformer is that a future reader can review,
and potentially replay, the process by which some piece of information was
created. Translating a Source can be done with the `OWM.translate` method::


    >>> from examples.datasource.translator import WikipediaText, ExtractWikipediaTables
    >>> trans_id = OWM().translator.create(ExtractWikipediaTables.rdf_type)
    >>> OWM().declare(
    ...     'examples.datasource.translator:WikipediaText',
    ...     [('page', 'Lake Street Dive'), ('section', 8)],
    ...     id=ex.lsd_wiki_web)
    >>> results = OWM().translate(trans_id, data_sources=(ex.lsd_wiki_web,))
    >>> results_list = list(results)
    >>> OWM().source.show(results_list[0].identifier)

Displaying attributes
---------------------
Sources have additional capabilities for text display, including retrieving
attributes from the RDF store. You can access these like this::

    >>> owm = OWM(non_interactive=True)
    >>> with owm.connect().transaction() as conn:
    ...     qctx = owm.default_context.stored
    ...     ds = qctx(LFDS).query(ident=ex.lsd_wiki).load_one()
    ...     print(ds.format_str(stored=True))
    LocalFileDataSource(<http://example.org/lsd_wiki>)
        File name: 'LakeStreetDive.wiki' 
    <BLANKLINE>   
