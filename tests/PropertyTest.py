from __future__ import absolute_import
from owmeta_core.custom_dataobject_property import CustomProperty
from .DataTestTemplate import _DataTest


class PropertyTest(_DataTest):
    def test_one(self):
        """ `one` should return None if there isn't a value or just the value if there is one """
        t = T()
        self.assertIsNone(t.one())
        t.b = 42
        self.assertEqual(42, t.one())

    def test_context_getter(self):
        p = CustomProperty()
        self.assertIsNone(p.context)

    def test_values_property(self):
        p = CustomProperty()
        self.assertEquals(p.values, [])

    def test_call_without_arg(self):
        t = T()
        self.assertIsNone(t())
        t.b = 42
        self.assertEqual(42, t())

    def test_call_with_arg(self):
        t = T()
        self.assertIsNone(t())
        n = t(42)
        self.assertEquals(n.b, 42)


class T(CustomProperty):
    def __init__(self):
        CustomProperty.__init__(self)
        self.b = None

    def get(self):
        if self.b is not None:
            yield self.b
        yield self.b

    def set(self, b):
        self.b = b
