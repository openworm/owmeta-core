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

    from owmeta_core import connect
    with connect() as conn: # creates an in-memory graph
        conn.rdf

That means you can do several of the same things as you would do with RDFLib
alone.<sup><a href="#note-1">1</a></sup> You can configure different backing stores as well:

    with connect({"rdf.store": "FileStorageZODB",
                  "rdf.store_conf": "./example.db"}) as conn:
        pass

(This will make a few files, named like `example.db*`, in the current working
directory.)

Assuming you've added some data to your graph, you may want to share it. In
owmeta-core, the primary way to share things is via "Bundles". Bundles are,
essentially, serialized collections of named graphs with additional attributes
for identifying them. To create a bundle, you "install" it, collecting the
named graphs you want to include in the bundle, serializing them, and putting
them in a particular file structure. Here's how you can do that:
```
    from owmeta_core.bundle import Installer, Descriptor
    with connect() as conn:
        # add some stuff to http://example.org/ctx ...
        inst = Installer('.', './bundles', conn.rdf)
        desc = Descriptor("aBundle",
                version=1,
                includes=['http://example.org/ctx'])
        inst.install(desc)
```
So, let's unpack that a little. First we add some things to named graphs. How
you do this is up to you, but this is a trivial example of adding a statement
to the graph that we'll ultimately include in the bundle:

    g = conn.rdf.graph(URIRef('http://example.org/ctx'))
    g.add((URIRef('http://example.org/s'),
           URIRef('http://example.org/p'),
           URIRef('http://example.org/o')))

Then, we create an `Installer` that will install bundles to the directory
"bundles" in the current working directory. Installers get instructed on what
to install through `Descriptor` objects. We create a bundle descriptor that
says what the bundle identifier is ("aBundle") what version of the bundle we're
installing (1) and which contexts we're including in the bundle (just
`http://example.org/ctx`). We pass the descriptor to the installer's `install`
method and it does creates the bundle file structure under `bundles`.

We haven't *shared* the bundle with any one yet with the above. You may choose
to package the bundle into an archive and share that somehow (E-mail, shared
file storage, etc.), and `owmeta_core.bundle.archive.Archiver` can help with
that. 
<!--TODO: Describe deploying bundles-->
<!--TODO: Describe the RDF <-> Python object mapping-->
<!--TODO: Describe DataSource / DataTransformer-->

[rdflib]: https://rdflib.readthedocs.io/en/stable/

Notes
-----
<a id="note-1"></a>
1. You can also create an `rdflib.graph.Graph` rather than a `Dataset` by
   defining a new `owmeta_core.data.RDFSource` and assigning it to
   `conn.conf["rdf.graph"]`. This turns out to not be especially useful in
   owmeta-core, but it is possible.
