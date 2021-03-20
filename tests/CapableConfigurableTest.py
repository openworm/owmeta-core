import pytest

from owmeta_core.capability import Capability, Provider
from owmeta_core.capable_configurable import CapableConfigurable, CAPABILITY_PROVIDERS_KEY


class TestCap(Capability):
    pass


@pytest.fixture
def cc():
    class CUT(CapableConfigurable):
        needed_capabilities = [TestCap()]

        def accept_capability_provider(self, cap, prov):
            self.prov = prov
    return CUT


def test_no_providers(cc):
    cc()


def test_empty_providers(cc):
    cc(conf={CAPABILITY_PROVIDERS_KEY: []})


def test_provide_provider(cc):
    class CP(Provider):
        provided_capabilities = [TestCap()]

        def provides_to(self, ob):
            return self
    cut = cc(conf={CAPABILITY_PROVIDERS_KEY: [CP()]})
    assert isinstance(cut.prov, CP)


def test_provide_provider_str(cc):
    cut = cc(conf={CAPABILITY_PROVIDERS_KEY: [f'{__name__}:NamedCP']})
    assert isinstance(cut.prov, NamedCP)


class NamedCP(Provider):
    provided_capabilities = [TestCap()]

    def provides_to(self, ob):
        return self
