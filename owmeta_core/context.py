from __future__ import print_function
from types import ModuleType
import logging

import rdflib
from rdflib.term import Variable, URIRef
from rdflib.graph import ConjunctiveGraph
import wrapt

from .data import DataUser

from .context_common import CONTEXT_IMPORTS
from .context_store import ContextStore, RDFContextStore
from .contextualize import (BaseContextualizable,
                            Contextualizable,
                            ContextualizableClass,
                            ContextualizingProxy,
                            contextualize_metaclass)
from .utils import FCN
from six.moves.urllib.parse import quote
from six import text_type
import six


L = logging.getLogger(__name__)

DEFAULT_CONTEXT_KEY = 'default_context_id'
'''
Configuration file key for the URI of a default RDF graph context.

This is the URI of the default graph in a project or bundle.
'''

IMPORTS_CONTEXT_KEY = 'imports_context_id'
'''
Configuration file key for the URI of an imports RDF graph context.

The imports context holds the relationships between contexts, especially the imports
relationship
'''

# TODO: Move this into mapper or a new mapper_common module
CLASS_REGISTRY_CONTEXT_KEY = 'class_registry_context_id'
'''
Configuration file key for the URI of the class registry RDF graph context.

The class registry context holds the mappings between RDF types and Python classes for a
project or bundle.
'''


class ModuleProxy(wrapt.ObjectProxy):
    def __init__(self, ctx, *args, **kwargs):
        super(ModuleProxy, self).__init__(*args, **kwargs)
        self._self_overrides = dict()
        self._self_ctx = ctx

    def add_attr_override(self, name, override):
        self._self_overrides[name] = override

    def __getattr__(self, name):
        o = self._self_overrides.get(name, None)
        if o is not None:
            return o
        else:
            o = super(ModuleProxy, self).__getattr__(name)
            if isinstance(o, (BaseContextualizable, ContextualizableClass)):
                o = o.contextualize(self._self_ctx)
                self._self_overrides[name] = o
            return o


class ContextMeta(ContextualizableClass):
    @property
    def context(self):
        return None

    @context.setter
    def context(self, v):
        pass

    def contextualize_class_augment(self, context):
        if context is None:
            return self
        ctxd_meta = contextualize_metaclass(context, self)
        res = ctxd_meta(self.__name__, (self,), dict(class_context=context.identifier))
        res.__module__ = self.__module__
        return res


class ContextualizableDataUserMixin(Contextualizable, DataUser):

    @property
    def conf(self):
        if self.context is None:
            return super(ContextualizableDataUserMixin, self).conf
        else:
            return self.context.conf

    @conf.setter
    def conf(self, conf):
        super(ContextualizableDataUserMixin, self).conf = conf


class Context(six.with_metaclass(ContextMeta,
                                 ContextualizableDataUserMixin)):
    """
    A context. Analogous to an RDF context, with some special sauce

    .. automethod:: __call__
    .. automethod:: __bool__
    """

    def __init__(self, ident=None,
                 imported=(),
                 mapper=None,
                 key=None,
                 base_namespace=None,
                 **kwargs):
        super(Context, self).__init__(**kwargs)

        if key is not None and ident is not None:
            raise Exception("Only one of 'key' or 'ident' can be given to Context")
        if key is not None and base_namespace is None:
            raise Exception("If 'key' is given, then 'base_namespace' must also be given to Context")

        if not isinstance(ident, URIRef) \
           and isinstance(ident, (str, text_type)):
            ident = URIRef(ident)

        if not isinstance(base_namespace, rdflib.namespace.Namespace) \
           and isinstance(base_namespace, (str, text_type)):
            base_namespace = rdflib.namespace.Namespace(base_namespace)

        if ident is None and key is not None:
            ident = URIRef(base_namespace[quote(key)])

        if not hasattr(self, 'identifier'):
            self.identifier = ident
        else:
            raise Exception(self)

        self._statements = []
        self._imported_contexts = list(imported)
        self._rdf_object = None
        self._graph = None

        if mapper is None:
            my_context = getattr(self, 'context')
            if my_context:
                mapper = my_context.mapper

        self.__mapper = mapper
        self.base_namespace = base_namespace

        self._change_counter = 0
        self._triples_saved = 0

        self._stored_context = None
        self._own_stored_context = None

    @property
    def mapper(self):
        if self.__mapper is None:
            from .mapper import Mapper
            self.__mapper = Mapper(conf=self.conf)

        return self.__mapper

    def contents(self):
        '''
        Returns statements added to this context

        Returns
        -------
        generator
        '''
        return (x for x in self._statements)

    def clear(self):
        '''
        Clear declared statements
        '''
        del self._statements[:]

    def add_import(self, context):
        '''
        Add an imported context
        '''
        self._imported_contexts.append(context)

    def add_statement(self, stmt):
        '''
        Add a statement to the context. Typically, statements will be added by
        `contextualizing <Contextualizable.contextualize>` a
        `~owmeta_core.dataobject.DataObject` and making a statement thereon. For instance,
        if a class ``A`` has a property ``p``, then for the context ``ctx``::

            ctx(A)(ident='http://example.org').p('val')

        would add a statement to ``ctx`` like::

            (A(ident='http://example.org'), A.p.link, rdflib.term.Literal('val'))

        Parameters
        ----------
        stmt : tuple
            Statement to add
        '''
        if self.identifier != stmt.context.identifier:
            raise ValueError("Cannot add statements from a different context")
        self._graph = None
        self._statements.append(stmt)
        self._change_counter += 1

    def remove_statement(self, stmt):
        '''
        Remove a statement from the context

        Parameters
        ----------
        stmt : tuple
            Statement to remove
        '''
        self._graph = None
        self._statements.remove(stmt)
        self._change_counter += 1

    @property
    def imports(self):
        '''
        Return imports on this context

        Yields
        ------
        Context
        '''
        for x in self._imported_contexts:
            yield x

    def transitive_imports(self):
        '''
        Return imports on this context and on imported contexts

        Yields
        ------
        Context
        '''
        for x in self._imported_contexts:
            yield x
            for y in x.transitive_imports():
                yield y

    def save_imports(self, context=None, *args, transitive=True, **kwargs):
        '''
        Add the `imports` on this context to a graph

        Parameters
        ----------
        context : .Context, optional
            The context to add statements to. This context's configured graph will
            ultimately receive the triples. By default, a context will be created with
            ``self.conf[IMPORTS_CONTEXT_KEY]`` as the identifier
        transitive : bool, optional
            If `True`, call imported imported contexts to save their imports as well
        '''
        if not context:
            ctx_key = self.conf[IMPORTS_CONTEXT_KEY]
            context = Context(ident=ctx_key, conf=self.conf)
        self.declare_imports(context, transitive)
        context.save_context(*args, **kwargs)

    def declare_imports(self, context=None, transitive=False):
        '''
        Declare `imports <~context_dataobject.ContextDataObject.imports>` statements in
        the given context

        Parameters
        ----------
        context : .Context, optional
            The context in which to declare statements. If not provided, one will be
            created with ``self.conf[IMPORTS_CONTEXT_KEY]`` as the identifier

        Returns
        -------
        Context
            The context in which the statements were declared
        '''
        if not context:
            ctx_key = self.conf[IMPORTS_CONTEXT_KEY]
            context = Context(ident=ctx_key, conf=self.conf)
        self._declare_imports(context, transitive)
        return context

    def _declare_imports(self, context, transitive):
        for ctx in self._imported_contexts:
            if self.identifier is not None \
                    and ctx.identifier is not None \
                    and not isinstance(ctx.identifier, rdflib.term.BNode):
                context(self.rdf_object).imports(ctx.rdf_object)
                if transitive:
                    ctx._declare_imports(context, transitive)

    def save_context(self, graph=None, inline_imports=False, autocommit=True, saved_contexts=None):
        '''
        Adds the staged statements in the context to a graph

        Parameters
        ----------
        graph : rdflib.graph.Graph or set, optional
            the destination graph. Defaults to ``self.rdf``
        inline_imports : bool, optional
            if `True`, imported contexts will also be written added to the graph
        autocommit : boolean, optional
            if `True`, `graph.commit <rdflib.graph.Graph.commit>` is invoked after adding statements to the
            graph (including any imported contexts if `inline_imports` is `True`)
        saved_contexts : set, optional
            a collection of identifiers for previously saved contexts. Note that `id` is
            used to get an identifier: the return value of `id` can be repeated after an
            object is deleted.
        '''
        if saved_contexts is None:
            saved_contexts = set()

        if (self._change_counter, id(self)) in saved_contexts:
            return

        saved_contexts.add((self._change_counter, id(self)))

        if graph is None:
            graph = self._retrieve_configured_graph()
        if autocommit and hasattr(graph, 'commit'):
            graph.commit()

        if inline_imports:
            for ctx in self._imported_contexts:
                ctx.save_context(graph, inline_imports, autocommit=False,
                        saved_contexts=saved_contexts)

        # XXX: Why is this here....?
        if hasattr(graph, 'bind') and self.mapper is not None:
            for c in self.mapper.mapped_classes():
                if hasattr(c, 'rdf_namespace'):
                    try:
                        graph.bind(c.__name__, c.rdf_namespace)
                    except Exception:
                        L.warning('Failed to bind RDF namespace for %s to %s', c.__name__,
                               c.rdf_namespace, exc_info=True)
        if isinstance(graph, set):
            graph.update(self._save_context_triples())
        else:
            ctx_graph = self.get_target_graph(graph)
            ctx_graph.addN((s, p, o, ctx_graph) for s, p, o in self._save_context_triples())

        if autocommit and hasattr(graph, 'commit'):
            graph.commit()

    save = save_context
    ''' Alias to save_context '''

    @property
    def triples_saved(self):
        '''
        The number of triples saved in the most recent call to `save_context`
        '''
        return self._triples_saved_helper()

    def _triples_saved_helper(self, seen=None):
        if seen is None:
            seen = set()
        if id(self) in seen:
            return 0
        seen.add(id(self))
        res = self._triples_saved
        for ctx in self._imported_contexts:
            res += ctx._triples_saved_helper(seen)
        return res

    def _save_context_triples(self):
        self._triples_saved = 0
        for x in self._statements:
            t = x.to_triple()
            if not (isinstance(t[0], Variable) or
                    isinstance(t[2], Variable) or
                    isinstance(t[1], Variable)):
                self._triples_saved += 1
                yield t

    def get_target_graph(self, graph):
        res = graph
        if self.identifier is not None:
            if hasattr(graph, 'graph_aware') and graph.graph_aware:
                res = graph.graph(self.identifier)
            elif hasattr(graph, 'context_aware') and graph.context_aware:
                res = graph.get_context(self.identifier)
        return res

    def contents_triples(self):
        '''
        Returns, as triples, the statements staged in this context

        Yields
        ------
        tuple
            A triple of `RDFLib Identifiers <rdflib.term.Identifier>`

        '''
        for x in self._statements:
            yield x.to_triple()

    def contextualize_augment(self, context):
        '''
        Returns a contextualized proxy of this context

        Parameters
        ----------
        context : .Context
            The context to contextualize this context with
        '''
        res = ContextualizingProxy(context, self)
        res.add_attr_override('_stored_context', None)
        res.add_attr_override('_own_stored_context', None)
        return res

    @property
    def rdf_object(self):
        '''
        Returns a dataobject for this context

        Returns
        -------
        owmeta_core.dataobject.DataObject
        '''
        if self._rdf_object is None:
            from owmeta_core.context_dataobject import ContextDataObject
            self._rdf_object = ContextDataObject.contextualize(self.context)(ident=self.identifier)

        return self._rdf_object.contextualize(self.context)

    def __bool__(self):
        '''
        Always returns `True`. Prevents a context with zero statements from testing false
        since that's not typically a useful branching condition.
        '''
        return True

    __nonzero__ = __bool__

    def __len__(self):
        return len(self._statements)

    def __call__(self, o=None, *args, **kwargs):
        """
        Contextualize an object

        Parameters
        ----------
        o : object
            The object to contexualize
        """
        if o is None:
            if kwargs:
                o = kwargs
        elif args:
            o = {x.__name__: x for x in [o] + list(args)}

        if isinstance(o, ModuleType):
            return ModuleProxy(self, o)
        elif isinstance(o, dict):
            return ContextContextManager(self, o)
        elif isinstance(o, BaseContextualizable):
            return o.contextualize(self)
        elif isinstance(o, ContextualizableClass):
            # Yes, you can call contextualize on a class and it'll do the right
            # thing, but let's keep it simple here, okay?
            return o.contextualize_class(self)
        else:
            return o

    def __str__(self):
        return repr(self)

    def __repr__(self):
        ident = getattr(self, 'identifier', '???')
        if ident is None:
            identpart = ''
        else:
            identpart = 'ident="{}"'.format(ident)
        return '{}({})'.format(FCN(type(self)), identpart)

    def load_own_graph_from_configured_store(self):
        '''
        Create a RDFLib graph for accessing statements in this context, *excluding* imported
        contexts. The "configured" graph is the one at ``self.conf['rdf.graph']``.

        Returns
        -------
        rdflib.graph.ConjunctiveGraph
        '''
        return ConjunctiveGraph(identifier=self.identifier,
                                store=RDFContextStore(self, include_imports=False))

    def load_graph_from_configured_store(self):
        '''
        Create an RDFLib graph for accessing statements in this context, *including* imported
        contexts. The "configured" graph is the one at ``self.rdf``.

        Returns
        -------
        rdflib.graph.ConjunctiveGraph
        '''
        return ConjunctiveGraph(identifier=self.identifier,
                store=RDFContextStore(self, imports_graph=self._imports_graph()))

    def _imports_graph(self):
        ctxid = self.conf.get(IMPORTS_CONTEXT_KEY, None)
        return ctxid and self.rdf.get_context(URIRef(ctxid))

    def rdf_graph(self):
        '''
        Return the principal graph for this context. For a regular `Context` this will be
        the "staged" graph.

        Returns
        -------
        rdflib.graph.ConjunctiveGraph

        See Also
        --------
        staged : Has the "staged" principal graph.
        mixed : Has the "mixed" principal graph.
        stored : Has the "stored" graph, including imports.
        own_stored : Has the "stored" graph, excluding imports.
        '''
        if self._graph is None:
            self._graph = self.load_staged_graph()
        return self._graph

    def load_mixed_graph(self):
        '''
        Create a graph for accessing statements both staged (see `load_staged_graph`) and
        stored (see `load_graph_from_configured_store`). No effort is made to either
        deduplicate, smush blank nodes, or logically reconcile statements between staged
        and stored graphs.

        Returns
        -------
        rdflib.graph.ConjunctiveGraph
        '''
        return ConjunctiveGraph(identifier=self.identifier,
                                store=ContextStore(context=self, include_stored=True,
                                    imports_graph=self._imports_graph()))

    def load_staged_graph(self):
        '''
        Create a graph for accessing statements declared in this specific instance of
        this context. This statements may not have been written to disk; therefore, they
        are "staged".

        Returns
        -------
        rdflib.graph.ConjunctiveGraph
        '''
        return ConjunctiveGraph(identifier=self.identifier, store=ContextStore(context=self))

    @property
    def mixed(self):
        '''
        A read-only context whose principal graph is the "mixed" graph.

        Returns
        -------
        QueryContext

        See also
        --------
        rdf_graph
        load_mixed_graph : Defines the principal graph for this context
        '''
        return QueryContext(
                mapper=self.mapper,
                graph=self.load_mixed_graph(),
                ident=self.identifier,
                conf=self.conf)

    @property
    def staged(self):
        '''
        A read-only context whose principal graph is the "staged" graph.

        Returns
        -------
        QueryContext

        See also
        --------
        rdf_graph
        load_staged_graph : Defines the principal graph for this context
        '''
        return QueryContext(
                mapper=self.mapper,
                graph=self.load_staged_graph(),
                ident=self.identifier,
                conf=self.conf)

    @property
    def stored(self):
        '''
        A read-only context whose principal graph is the "stored" graph, including
        imported contexts.

        Returns
        -------
        QueryContext

        See also
        --------
        rdf_graph
        load_graph_from_configured_store : Defines the principal graph for this context
        '''
        if self._stored_context is None:
            self._stored_context = QueryContext(
                    mapper=self.mapper,
                    graph=self.load_graph_from_configured_store(),
                    ident=self.identifier,
                    conf=self.conf)
        return self._stored_context

    @property
    def own_stored(self):
        '''
        A read-only context whose principal graph is the "stored" graph, excluding
        imported contexts.

        Returns
        -------
        QueryContext

        See also
        --------
        rdf_graph
        load_own_graph_from_configured_store : Defines the principal graph for this context
        '''
        if self._own_stored_context is None:
            self._own_stored_context = QueryContext(
                    mapper=self.mapper,
                    graph=self.load_own_graph_from_configured_store(),
                    ident=self.identifier,
                    conf=self.conf)
        return self._own_stored_context

    def _retrieve_configured_graph(self):
        return self.rdf

    def resolve_class(self, uri):
        if self.mapper is None:
            return None
        return self.mapper.resolve_class(uri, self)


class QueryContext(Context):
    '''
    A read-only context.
    '''
    def __init__(self, graph, *args, **kwargs):
        super(QueryContext, self).__init__(*args, **kwargs)
        self.__graph = graph

    def rdf_graph(self, *args, **kwargs):
        return self.__graph

    @property
    def imports(self):
        ctxid = self.conf.get(IMPORTS_CONTEXT_KEY, None)

        imports_graph = ctxid and self.rdf.get_context(URIRef(ctxid))
        if imports_graph is None:
            return
        for t in imports_graph.triples((self.identifier, CONTEXT_IMPORTS, None)):
            yield QueryContext(mapper=self.mapper,
                    graph=self.__graph,
                    ident=t[2],
                    conf=self.conf)

    def add_import(self, *args, **kwargs): raise ContextIsReadOnly
    def save_imports(self, *args, **kwargs): raise ContextIsReadOnly
    def save_context(self, *args, **kwargs): raise ContextIsReadOnly


class ContextIsReadOnly(Exception):
    def __init__(self):
        super(ContextIsReadOnly, self).__init__('This context is read-only')


ClassContexts = dict()


class ClassContextMeta(ContextMeta):

    def __call__(self, ident, base_namespace=None, imported=()):
        res = ClassContexts.get(URIRef(ident))
        if not res:
            res = super(ClassContextMeta, self).__call__(ident=ident,
                    base_namespace=base_namespace, imported=imported)
            ClassContexts[URIRef(ident)] = res
        else:
            if base_namespace or imported:
                raise Exception('Arguments can only be provided to a ClassContext on'
                                ' first creation')
        return res


class ClassContext(six.with_metaclass(ClassContextMeta, Context)):
    pass


class ContextContextManager(object):
    """ The context manager created when Context::__call__ is passed a dict """

    def __init__(self, ctx, to_import):
        self._overrides = dict()
        self._ctx = ctx
        self._backing_dict = to_import
        self.save = self._ctx.save_context

    @property
    def context(self):
        return self._ctx

    def __call__(self, o):
        return self._ctx(o)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def __getattr__(self, name):
        return self.lookup(name)

    def __getitem__(self, key):
        return self.lookup(key)

    def lookup(self, key):
        o = self._overrides.get(key, None)
        if o is not None:
            return o
        o = self._backing_dict[key]
        if isinstance(o, (BaseContextualizable, ContextualizableClass)):
            o = o.contextualize(self._ctx)
            self._overrides[key] = o
        return o
