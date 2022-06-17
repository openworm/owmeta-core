.. _transactions:

Transactions
============
Transactions in |owm| are managed through the `transaction`_ library. The
default RDF store is transactional. You can execute code within a transaction
using a transaction manager. |owm| connections come with a transaction manager
which you can access via the `transaction_manager` attribute. It's recommended
to use a context manager to start and commit transactions like this::

    >>> from rdflib.term import URIRef
    >>> from owmeta_core import connect
    >>> with connect() as conn, conn.transaction_manager:
    ...     conn.rdf.add((
    ...         URIRef('http://example.org/bob'),
    ...         URIRef('http://example.org/likes'),
    ...         URIRef('http://example.org/alice')))

Because this is a common pattern, there's a
:meth:`~owmeta_core.Connection.transaction` method that does something
equivalent which is provided for convenience::

    >>> with connect().transaction() as conn:
    ...     conn.rdf.add((
    ...         URIRef('http://example.org/bob'),
    ...         URIRef('http://example.org/likes'),
    ...         URIRef('http://example.org/alice')))

Similar usage is possible with project connections through the high-level
`~owmeta_core.command.OWM` interface::

    >>> from owmeta_core.command import OWM
    >>> owm = OWM(non_interactive=True)
    >>> owm.init(default_context_id=URIRef("http://example.org/context"))
    Initialized owmeta-core project at .../.owm

    >>> with owm.connect().transaction() as conn:
    ...     conn.rdf.add((
    ...         URIRef('http://example.org/bob'),
    ...         URIRef('http://example.org/likes'),
    ...         URIRef('http://example.org/alice')))

However, the methods of `~owmeta_core.command.OWM` and its "sub-commands" will
typically manage the transactions themselves, so it wouldn't be necessary to
start a transaction explicitly before calling these methods--in fact, doing so
would typically cause an exception. For example, in this code::

    >>> owm.say('http://example.org/bob',
    ...         'http://example.org/likes',
    ...         'http://example.org/eve')

we don't have to declare a transaction since the `~owmeta_core.command.OWM.say`
method handles that for us.

For read-only operations, it is not strictly necessary to read from the RDF
store within the context of a transaction, but it is recommended if you're in a
multithreaded context to avoid getting an inconsistent picture of the data if
there's an update part way through your operation.

.. _transaction: https://transaction.readthedocs.io/en/latest/
