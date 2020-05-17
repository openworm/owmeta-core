from owmeta_core.bundle import Remote, URLConfig


def test_remote_add_config_no_dupe():
    uc = URLConfig('http://example.org/bluh')
    cut = Remote('test', (uc,))
    cut.add_config(uc)
    assert len(cut.accessor_configs) == 1


def test_remote_equality():
    uc = URLConfig('http://example.org/bluh')
    cut1 = Remote('test', (uc,))
    cut2 = Remote('test', (uc,))
    assert cut1 == cut2


def test_remote_inequality_by_accessors():
    uc = URLConfig('http://example.org/bluh')
    cut1 = Remote('test', (uc,))
    cut2 = Remote('test', ())
    assert cut1 != cut2


def test_remote_inequality_by_name():
    uc = URLConfig('http://example.org/bluh')
    cut1 = Remote('test1', (uc,))
    cut2 = Remote('test2', (uc,))
    assert cut1 != cut2
