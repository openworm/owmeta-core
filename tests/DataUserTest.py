from __future__ import absolute_import
from six.moves import range
from owmeta_core.data import DataUser
from owmeta_core.configure import Configurable
import rdflib

from .DataTestTemplate import _DataTest


class DataUserTest(_DataTest):

    def test_init_no_config(self):
        """ Should fail to initialize since it's lacking basic configuration """
        c = Configurable.default_config
        Configurable.default_config = False
        DataUser()
        Configurable.default_config = c

    def test_init_no_config_with_default(self):
        """ Should suceed if the default configuration is a Data object """
        DataUser()

    def test_init_False_with_default(self):
        """ Should suceed if the default configuration is a Data object """
        DataUser(conf=False)

    def test_add_statements_completes(self):
        """ Test that we can upload lots of triples.

        This is to address the problem from issue #31 on https://github.com/openworm/owmeta/issues
        """
        g = rdflib.Graph()
        for i in range(9000):
            s = rdflib.URIRef("http://somehost.com/s%d" % i)
            p = rdflib.URIRef("http://somehost.com/p%d" % i)
            o = rdflib.URIRef("http://somehost.com/o%d" % i)
            g.add((s, p, o))
        du = DataUser(conf=self.config)
        du.add_statements(g)
