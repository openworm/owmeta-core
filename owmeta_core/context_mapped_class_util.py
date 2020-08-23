from importlib import import_module
import logging

from rdflib.namespace import Namespace
from rdflib.term import URIRef
import six

from .context import ClassContext


L = logging.getLogger(__name__)


def find_class_context(cls, dct, bases):
    ctx = None
    ctx_or_ctx_uri = dct.get('class_context', None)

    if ctx_or_ctx_uri is None:
        try:
            modname = getattr(cls, '__module__', None)
            if modname:
                mod = import_module(modname)
                ctx_or_ctx_uri = getattr(mod, 'module_context', None)
        except Exception:
            L.warning('Error getting module for class', exc_info=True)

    if not isinstance(ctx_or_ctx_uri, URIRef) \
       and isinstance(ctx_or_ctx_uri, (str, six.text_type)):
        ctx_or_ctx_uri = URIRef(ctx_or_ctx_uri)

    if isinstance(ctx_or_ctx_uri, (str, six.text_type)):
        ctx = ClassContext(ctx_or_ctx_uri)
    else:
        ctx = ctx_or_ctx_uri

    return ctx


def find_base_namespace(dct, bases):
    base_ns = dct.get('base_namespace', None)
    if base_ns is None:
        for b in bases:
            if hasattr(b, 'base_namespace') and b.base_namespace is not None:
                base_ns = b.base_namespace
                break
    if base_ns and not isinstance(base_ns, Namespace):
        base_ns = Namespace(base_ns)
    return base_ns
