[![Build develop](https://github.com/openworm/owmeta-core/actions/workflows/scheduled-dev-build.yml/badge.svg)](https://github.com/openworm/owmeta-core/actions/workflows/scheduled-dev-build.yml)[![Docs](https://readthedocs.org/projects/owmeta-core/badge/?version=latest)](https://owmeta-core.readthedocs.io/en/latest)
[![Coverage Status](https://coveralls.io/repos/github/openworm/owmeta-core/badge.svg?branch=develop)](https://coveralls.io/github/openworm/owmeta-core?branch=develop)

owmeta-core
===========
owmeta-core helps with sharing relational data over the Internet and keeping
track of where that data comes from. Exactly *how* that is achieved is best
understood through demonstration, which you can find in the "Usage" section
below.

Install
-------
To install owmeta-core, you can use pip:

    pip install owmeta-core

Usage
-----
First of all, owmeta-core wraps [RDFLib][rdflib]:

    >>> from owmeta_core import connect
    >>> with connect() as conn: # creates an in-memory graph
    ...    conn.rdf
    <Graph identifier=... (<class 'rdflib.graph.Dataset'>)>

That means you can do several of the same things as you would do with RDFLib
alone.[^1] You can configure different backing stores as well:

    >>> with connect({"rdf.store": "FileStorageZODB",
    ...              "rdf.store_conf": "./example.db"}) as conn:
    ...    pass

(This will make a few files, named like `example.db*`, in the current working
directory.)

Assuming you've added some data to your graph, you may want to share it. In
owmeta-core, the primary way to share things is via "Bundles". Bundles are,
essentially, serialized collections of named graphs with additional attributes
for identifying them. To create a bundle, you "install" it, collecting the
named graphs you want to include in the bundle, serializing them, and putting
them in a particular file structure. Here's how you can do that:

    >>> from owmeta_core.bundle import Installer, Descriptor
    >>> from rdflib.term import URIRef
    >>> import transaction

    >>> with connect() as conn, transaction.manager:
    ...     # add some stuff to http://example.org/ctx ...
    ...     g = conn.rdf.graph(URIRef('http://example.org/ctx'))
    ...     _ = g.add((URIRef('http://example.org/s'),
    ...            URIRef('http://example.org/p'),
    ...            URIRef('http://example.org/o')))
    ...     inst = Installer('.', './bundles', conn.rdf)
    ...     desc = Descriptor("a-bundle",
    ...             version=1,
    ...             includes=['http://example.org/ctx'])
    ...     bundle_directory = inst.install(desc)

So, let's unpack that a little. First we add some things to named graphs. How
you do this is up to you, but above is a trivial example of adding a statement
to the graph that we'll ultimately include in the bundle. Then, we create an
`Installer` that will install bundles to the directory "bundles" in the current
working directory. Installers get instructed on what to install through
`Descriptor` objects. We create a bundle descriptor that says what the bundle
identifier is ("a-bundle") what version of the bundle we're installing (1) and
which contexts we're including in the bundle (just `http://example.org/ctx`).
We pass the descriptor to the installer's `install` method and it does creates
the bundle file structure under `bundles`.

We haven't *shared* the bundle with anyone yet with the above. You may choose
to package the bundle into an archive and share that somehow (E-mail, shared
file storage, etc.), and `owmeta_core.bundle.archive.Archiver` can help with
that:

    >>> from owmeta_core.bundle.archive import Archiver

    >>> # Save a bundle a-bundle.tar.xz in the current working directory
    >>> Archiver('.').pack(
    ...        bundle_directory=bundle_directory,
    ...        target_file_name='a-bundle.tar.xz')
    './a-bundle.tar.xz'

There are, however, facilities for uploading and downloading bundles through
owmeta-core. owmeta-core has the concept of "Remotes" which are places bundles
can be sent to or retrieved from. A remote has a set of "accessor configs"
(ACs) which provide information for getting bundles. An AC may include a URL
or other pieces of information, like authentication credentials. A remote may
be set up as the upload target or download source for a single bundle or
multiple bundles, and it may have multiple ACs for different protocols or for
the same protocol (e.g., for download mirrors). Here's how we can define a
remote:

    >>> from owmeta_core.bundle import Remote, URLConfig

    >>> a_remote = Remote('a-bundle-server',
    ...    [URLConfig('https://example.org/bundle-server')])

Here's how we can upload to a remote:

    >>> from owmeta_core.bundle import Deployer

    >>> deployer = Deployer([a_remote])
    >>> deployer.deploy(bundle_directory)  # doctest: +SKIP

Assuming that `a_remote` reflects an actual HTTP server that can accept POST
requests with bundle archives in the request body, this would upload the
bundle. (As written, this sends the bundle to Downloading bundles is similar:

    >>> from owmeta_core.bundle import Fetcher

    # Fetch version 3 of "another-bundle" and install it to "./bundles"
    >>> fetcher = Fetcher('./bundles', [a_remote])
    >>> fetcher.fetch('another-bundle', bundle_version=3)  # doctest: +SKIP

Of course, once we can fetch bundles from remotes, we might want to actually
use them. While it is possible to parse the serialized graphs from the
"./bundles" directory, that would discard some useful information about the
bundle, including dependencies between bundles (not discussed above).[^2]
Instead, it's recommended to use `Bundle` objects to access bundle data. Here's
an example:

    >>> from owmeta_core.bundle import Bundle

    >>> with Bundle('a-bundle', version=1, bundles_directory='./bundles') as bnd:
    ...    g = bnd.rdf.get_context('http://example.org/ctx')
    ...    assert (URIRef('http://example.org/s'),
    ...            URIRef('http://example.org/p'),
    ...            URIRef('http://example.org/o')) in g # .get_context('http://example.org/ctx')


<!--TODO: Describe DataSource / DataTransformer-->

[rdflib]: https://rdflib.readthedocs.io/en/stable/

### RDF <-> Python object mapping

When sharing data we have the problem of data independence: dealing with schema
changes of the underlying RDF. owmeta-core tries to deal with this problem by
providing tools for constructing adaptable two-way mappings between RDF and
Python objects. These mappings relate [RDF classes][rdf_class] to Python
classes, which classes are constrained to be sub-classes of
`owmeta_core.dataobject.BaseDataObject`.

[rdf_class]: https://www.w3.org/TR/rdf-schema/#ch_classes



Notes
-----
[^1]: You can also create an `rdflib.graph.Graph` rather than a `Dataset` by
   defining a new `owmeta_core.data.RDFSource` and assigning it to
   `conn.conf["rdf.graph"]`. This turns out to not be especially useful in
   owmeta-core, but it is possible.
[^2]: Bundle dependency information isn't stored as RDF. It likely will be
   eventually, to allow other software to query bundle relationships without
   needing to understand the particular format of bundle manifests and the
   bundle cache file tree.
