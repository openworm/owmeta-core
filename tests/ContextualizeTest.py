from owmeta_core.context import Context
from owmeta_core.contextualize import (AbstractBaseContextualizable,
                                          BaseContextualizable,
                                          Contextualizable,
                                          ContextualizableClass)
from owmeta_core.dataobject import DataObject


def test_contextualizable_class_subclass(): assert issubclass(ContextualizableClass, AbstractBaseContextualizable)


def test_contextualizable_subclass(): assert issubclass(Contextualizable, AbstractBaseContextualizable)


def test_base_contextualizable_subclass(): assert issubclass(BaseContextualizable, AbstractBaseContextualizable)


def test_base_contextualizable_isinstance():
    class BC(BaseContextualizable):
        pass
    bc = BC()
    assert isinstance(bc, AbstractBaseContextualizable)


def test_contextualizable_isinstance():
    class C(Contextualizable):
        pass
    c = C()
    assert isinstance(c, AbstractBaseContextualizable)


def test_contextualizable_class_isinstance():
    class CC(metaclass=ContextualizableClass):
        pass
    assert isinstance(CC, AbstractBaseContextualizable)


def test_contextualized_contextualizable_class_isinstance():
    class CC(metaclass=ContextualizableClass):
        definition_context = Context('http://example.org/defctx')
    ctx = Context('http://example.org/ctx')
    assert isinstance(CC.contextualize(ctx), AbstractBaseContextualizable)


def test_multi_based_meta_contextualizable_class_isinstance():
    class B(type):
        pass

    class MC(B, ContextualizableClass):
        pass

    class CC(metaclass=MC):
        definition_context = Context('http://example.org/defctx')
    ctx = Context('http://example.org/ctx')
    assert isinstance(CC.contextualize(ctx), AbstractBaseContextualizable)


def test_dataobject_isinstance():
    assert isinstance(DataObject(), AbstractBaseContextualizable)
