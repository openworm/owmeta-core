from owmeta_core.capability import Provider


class P1(Provider):
    provided_capabilities = ['cap1', 'cap2']


class P2(Provider):
    provided_capabilities = ['cap4', 'cap2']


def test_provider_capability_merging_1():
    class P3(P1, P2):
        pass

    assert P3.provided_capabilities == ['cap1', 'cap2', 'cap4']


def test_provider_capability_merging_2():
    class P3(P2, P1):
        pass

    assert P3.provided_capabilities == ['cap1', 'cap2', 'cap4']


def test_provider_capability_merging_3():
    class P3(P1, P2):
        provided_capabilities = ['cap5']

    assert P3.provided_capabilities == ['cap1', 'cap2', 'cap4', 'cap5']


def test_provider_capability_merging_4():
    assert P2.provided_capabilities == ['cap2', 'cap4']
