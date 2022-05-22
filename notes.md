Attaching Evidence to the DataSources Which Lack It
===================================================
Some data sources come with Evidence and/or Documents attached to them. This
means that they have fields for URLs, DOIs, etc embedded in the document,
either in some kind of metadata segment or as an auxiliary file -- whatever.
This makes the difference for us because with our DWEDS, for instance, we can
translate using just that source into a DWEDS. However, some DataSources don't
have such a metadata segment, but must have that attached to them in one way or
another. In addition, we have planned a model where documents can be retrieved
from a database (openworm-scholar) -- an external location to the data source
itself. This all means that we need to attach evidence to data sources
after-the-fact.

So, how do we attach the Evidence after-the-fact? Well, initially, I thought
that there would be a distinct tool for attaching evidence like:

    owm evidence attach <ds> <evidence-id>

That could work...but it also furthers a practice of writing basically the same
code in distinct sub-commands to do basically the same thing. So, I thought
that I would generalize that pattern to:

    owm declare <subject> <property1>=<value1> <property2>=<value2>

which allows me to do

    owm declare --id=<ds> owmeta_core.datasource:DataSource \
        <attached-evidence-property>=<evidence-id>

So, then a user just needs to know the values for those placeholders:

- The first one is pretty easy: we'll show them that ID when they create the
  data source and they can always do `owm source list` to list available data
  sources.
- The second one is somewhat more difficult -- there's a possibility that we
  can have some kind of tab-complete for a couple of shells that does a `dir()`
  function call on the object corresponding to <ds>, but that's a bit "extra".
  Perhaps a `list-props` command that lists available properties to use in a
  `declare` which could, if desired, be put into a tab-completion, or just used
  directly by the user. Still, it's a bit of a pain because you have to do that
  search to find what it is you want. I don't really have a fix for this other
  than "you just have to know it", but this may be an argument for having a
  specialized sub-command that's findable through the usage info.
- The third is, again, relatively easy, *but* the user needs to know how to
  make the Evidence in the first place. That could just be more `declare`
  calls, since `declare` also creates objects when needed.

So, then we can use that attached Evidence to make the DWEDS. One thing we deal
with is the relationship of the DWEDS evidence context with the context where
the Evidence is defined. This is important here because we want to keep the
size of the included contexts to a minimum: this makes it cheaper to pull in
only needed data. What we can do, maybe, is to have a specialized command, `owm
source attach-evidence ...`, that has syntax like `owm declare` but declares
the evidence in a context specially made for the evidence we're attaching to in
order to keep the context small.

Scenario?

    $ owm declare owmeta.document:Document doi=10.1117/1.JBO.19.1.010901 \
        title="Medical hyperspectral imaging: a review" \
        author='["Guolan Lu", "Baowei Fei"]' \
        --context='http://example.org/doc_context'
    http://data.openworm.org/Document#a3fe2f847680ff6a6e80f6d9c0d79d994
    $ owm declare owmeta.document:DocumentReference \
        document=Document:a3fe2f847680ff6a6e80f6d9c0d79d994 \
        table=1 --id='https://example.org/10.1117/1.JBO.19.1.010901/hsi-summary-table' \
        --context='http://example.org/doc_context'
    https://example.org/10.1117/1.JBO.19.1.010901/hsi-summary-table
    $ owm declare owmeta_core.data_trans.csv_ds:CSVDataSource \
        --id=https://example.org/dsid \
        owmeta.document:DocumentReference=https://example.org/10.1117/1.JBO.19.1.010901/hsi-summary-table
    https://example.org/dsid


This makes a document, makes a reference to the document (optional, but adds
specificity), and attaches it to a data source. This last one is a bit of a
funny one. We need some way to attach a document reference, and to do it with
declare, needs a key that doesn't (and can't) have an equals sign in it, so we
can't use a URL unless we come up with some kind of escaping or quoting (which
is annoying). To get around that, I reuse the same trick as with the class
references. This can also work with properties from other classes rather than
those defined on their own since classes do have properties as attributes when
they're defined that way:

    $ owm declare owmeta_core.datasource:CSVDataSource \
        --id=https://example.org/dsid \
        example.module:AClass.class_property=47

This is rather verbose: it would be nicer to have something like this when the meaning is unambiguous:

    $ owm declare CSVDataSource --id=https://example.org/dsid AClass.class_property=47

I think eventually, we may do something like this, but first off I want to keep
to the most explicit, least "magic" way of making this work. Allowing a partial
reference like this means that we could have a conflict introduced into an
already written command by adding an otherwise unrelated module: This is worse
than an error due to a "property not found" type of error because with that
kind you can obviously prevent that breakage by just holding constant the
module where the property is defined whereas if the conflict is between
modules your command can cease to be unambiguous by the introduction of a
third-party module. Of course, if we allow the abbreviated form, we'll retain
the canonical form as a way to prevent that kind of problem and discourage use
of the abbreviated form for scripting and the like.

[1]: and possibly evidence as well, meaning, possibly, more precise citation
and, typically, elaboration of the relationship between the document and the
object the reference is attached to


Repository Index Caching
========================
We would like to have the ability to reuse the repository index across
downloads. The question is how to do that. One way is to re-use a loader across
instances for a given URL or accessor config. To some extent, this violates
expectations about a given class's `__call__` producing new instances, but that
isn't an expectation that has any particular value for us. Still, if we just
reuse the same object, then we have to concern ourselves with multi-threaded
programs which may try to download the same index and assign to the same field
at the same time. This may not be a problem if we decide to just not to handle
that...


Declaring packages / module accessors
=====================================

The key thing with distribution data is how to access code that we reference in
the class registry. So, the commands for manipulating package data in the
registry should be oriented around describing module access. To that end, we
have a `module-access` sub-command.

`module-access`, further, has a `declare` sub-command for declaring
module-access and package information. Because of the diversity of methods of
retrieving packages across languages and package management schemes, the
`declare` command has yet a further family of sub-commands which can be defined
via entry points.

All of these `declare` sub-commands would execute as part of bundle release
preparation. Although it may seem like adding module access at the point where
we declare registry entries makes sense, the CONOPS for bundle development I
have in mind puts code development in tandem with generation of the data in a
project and thus the data that will ultimately be in a bundle. We would need to
define class registry entries for correct functioning of many of the data
authoring tools, so registry entry declaration would happen before a software
release, meaning we couldn't just find out the module access info automatically
and add it. (Besides, as I've found in the Python world, and know from the Java
world, most software doesn't have information about where you can get it within
the program, so it's not workable as a general technique.)

Rather than having sub-commands to declare each of the module accessors, we
might consider having this all described in the Bundle descriptor. We don't
actually need that info during development: it's only when we're pulling in a
bundle from elsewhere that we wouldn't necessarily have the software required
and would want to use the class registry to find the code needed to make better
use of the data. So, we could open the Bundle descriptor to a similar plugin
architecture that the OWM/command interface uses to allow for the specification
of software dependencies that way. Some examples are below for what this could
look like.

Regardless of how we declare the module accessors, we'll need to be able to get
the information back out in a way that's readily usable by the intended
recipients. The goal is to, at least, make it possible to get a human-readable
explanation of what must be installed. A step up from that displays the data in
a way that's machine readable to allow for automation where circumstances allow.

Examples
--------

Declare everything needed to make PyPI module access and check that it is
actually available on PyPI:

    owm registry module-access declare python-pip \
        --module-name=owmeta_core.collections \
        --package-name=owmeta-core \
        --package-version=12.4.0

Same, but with a module URI to be more specific about which module we're
talking about:

    owm registry module-access declare python-pip \
        --module-id='http://data.openworm.org/PythonModule#owmeta_core.collections' \
        --package-name=owmeta-core \
        --package-version=12.4.0

Same, but rely on package metadata (i.e., from importlib/pkg_resources) to
figure out package name and version:

    owm registry module-access declare python-pip \
        --module=owmeta_core.document

Declare module access for a package on CRAN (Comprehensive R Archive Network)
package index that has the same module name as the package:
    owm registry module-access declare r-cran \
        --module-name=PlaneGeometry

Declare module access for an R package on GitHub using devtools:

    owm registry module-access declare r-devtools-github \
        --module-name=stringr --package-name=tidyverse/stringr

Similar commands with customized options would be used for different
languages/runtimes/package servers. Alternative to having commands to add such
module access info, we could put this info into the bundle descriptor, like this:

    ---
    id: openworm/owmeta-core
    name: owmeta-core bundle
    version: 3
    description: Core bundle for owmeta-core bundles
    includes:
        - http://schema.openworm.org/2020/07
        - http://www.w3.org/1999/02/22-rdf-syntax-ns
        - http://www.w3.org/2000/01/rdf-schema
    software:
        python-pip:
            - module-name: owmeta_core.collections
              package-name: owmeta-core
              package-version: 12.4.0
            - module-name: owmeta_core.dataobject
              package-name: owmeta-core
              package-version: 12.4.0

This can get pretty verbose -- we would want to avoid duplicating the package
info when several modules are provided by the same package. So, we might do
something like this:

    software:
        python-pip:
          - package-name: owmeta-core
            package-version: 12.4.0
            modules:
              - owmeta_core.collections
              - owmeta_core.dataobject
              - id: http://data.openworm.org/PythonModule#owmeta_core.data_trans.csv_ds
          - package-name: made-up-package
            package-version: 1.1.1
            module-patterns:
              - my_good_module.*
        r-cran:
          - package-name: PlaneGeometry
            modules:
              - PlaneGeometry

There's also a possibility we could have multiple means of getting a package.
Like, maybe we allow for separate package definition?

    software:
        packages:
          - id: owmeta-core-12.4
            package-name: owmeta-core
            package-version: 12.4.0
          - id: owmeta-core-13.0.1
            package-name: owmeta-core
            package-version: 13.0.1
        package-access:
          python-pip:
            - package: # multiple compatible packages
                - owmeta-core-12.4
                - owmeta-core-13.0.1
              modules:
                - owmeta_core.collections
                - owmeta_core.dataobject
                - id: http://data.openworm.org/PythonModule#owmeta_core.data_trans.csv_ds
            - package-name: made-up-package
              package-version: 1.1.1
              module-patterns:
                - my_good_module.*
          r-cran:
            - package-name: PlaneGeometry
              modules:
                - PlaneGeometry

eh...that's kinda crap. Maybe just accept that anchors and aliases are a thing
to allow for repeated info:

    software:
        python-pip:
          - package-name: owmeta-core
            package-version: 12.4.0
            modules:
              - owmeta_core.collections
              - owmeta_core.dataobject
              - id: http://data.openworm.org/PythonModule#owmeta_core.data_trans.csv_ds
          - package-name: made-up-package
            package-version: 1.1.1
            module-patterns:
              - my_good_module.*
        r-cran:
          - package-name: PlaneGeometry
            modules:
              - PlaneGeometry

Hrm...given that we ultimately want to describe this stuff in RDF, it seems
like having a completely separate YAML format that we'll continually have to
adapt from and update is a really bad idea. The sub-commands, OTOH, would
define just what they need to define and have the advantage that we'll have the
full Python language and run-time environment available to interpret what the
commands mean.

For getting a human-readable description of module access, we could do
something like this:

    owm registry module-access show python-pip-requirements --module=stringr

(I guess this would just return an empty string)

Each `ModuleAccessor` can provide an implementation of "show" that would
provide either human readable instructions for how to install the required
packages or machine readable package description or partial package description
(e.g., a `<dependencies>` element in Maven). It's best to have a default way to
"show" and maybe to "install" as well:

    import subprocess
    import sys
    from textwrap import dedent

    class PythonPip(ModuleAccessor):

        def show(self):
            pkgs = self.package.get()
            pkg_list = '\n'.join(f'"{pkg.name()}=={pkg.version()}"' for pkg in pkgs)
            return ('To install add the following to a file, "requirements.txt":\n' +
                    pkg_list + '\nand execute `pip install requirements.txt`')

        def install(self, pip=None):
            '''
            Parameters
            ----------
            pip : list, optional
                The pip command to use
            '''
            if pip is None:
                pip = [sys.executable, '-m', 'pip']
            if not isinstance(pip, list):
                raise TypeError('"pip" argument must be a list')
            cmd = pip + ['install', '--isolated']
            extra = ''
            for index_url in set(self.index_url.get()):
                cmd += [f'--{extra}index-url', index_url]
                extra = 'extra-'
            cmd += [f'"{pkg.name()}=={pkg.version()}"']
            pkg = self.package.one()
            subprocess.check_call(cmd)




Property Value Alternatives
===========================

a.subclass_of.alt(['blah'])
a.subclass_of.either('blah')
a.subclass_of.either(['blah', 'bluh', 'fluh', 'eeuuuh'])
a.subclass_of.one_of(['blah', 'bluh', 'fluh', 'eeuuuh'])
a.subclass_of.any_of(['blah', 'bluh', 'fluh', 'eeuuuh'])
a.subclass_of.one_or_more_of(['blah', 'bluh', 'fluh', 'eeuuuh'])
a.has_friend_named.one_of(['Agnes', 'Agatha', 'Germaine', 'Jack'])


Namespace Management
====================
For splitting off the namespace management from the individual graphs and graph
stores so they can be managed properly for display and stuff.

- Define a new NamespaceStore interface which is just a Store, but only
  supporting the namespace management methods
- Define a new config key `rdf_namespace_manager.conf` for the namespace store
- Update Data to read the store from `rdf_namespace_manager.conf` file location
- Define `rdf_namespace_manager.conf` for OWM
- Replace the argument to the Data NamespaceManager with a dummy graph that
  just points to a NamespaceStore
- Change the OWMNamespace methods to work on the OWM conf 'rdf.namespace_manager'
  (see Data) rather than own_rdf.namespace_manager (the one maintained on the
  RDFLib Graph)
- Add a serialization of the namespace bindings from NamespaceStore for the
  project repository

Related, but distinct: getting suggested namespace mappings from bundles:

- Add suggested prefix-namespace mappings to bundles via the bundle descriptor
- Add a sub-command for reading mappings from bundles in the dependency
  list (includes transitive dependencies)

There is not, yet, a sub-command or anything that specifically adds a
dependency, so there's not really a natural place to pull prefixes from a given
bundle and its dependencies. So, there would need to be a user action to
initiate reading in those mappings.

Importantly, there are namespace mappings for all of our mapped classes. I've
changed that to add the bindings when saving the classes using the OWM.save
command, but it should probably be possible to do that some other way without
the baggage of a project. In any case, we'll want to make sure that it's easy
to add these mappings to a bundle...
