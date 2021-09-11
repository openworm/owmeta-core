from os.path import join as p
from unittest.mock import patch, Mock, MagicMock

from pytest import raises
from rdflib.store import Store
from rdflib.term import URIRef
from rdflib.plugin import PluginException, get as plugin_get
try:
    from rdflib.plugins.stores.memory import Memory
except ImportError:
    # rdflib<6.0.0
    from rdflib.plugins.memory import IOMemory as Memory

from owmeta_core.bundle_dependency_store import (BundleDependencyStore, _is_cacheable,
                                                 _cache_key, RDFLIB_PLUGIN_KEY, StoreCache)


def test_excludes_no_triples():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert 0 == sum(1 for _ in bds.triples((None, None, None)))


def test_excludes_some_triples():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx1')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert 1 == sum(1 for _ in bds.triples((None, None, None)))


def test_excludes_all_for_excluded_context():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx1')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert 0 == sum(1 for _ in bds.triples((None, None, None),
                                           context='http://example.org/ctx'))


def test_includes_triples():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    bds = BundleDependencyStore(iom)
    assert 1 == sum(1 for _ in bds.triples((None, None, None)))


def test_includes_contexts():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    bds = BundleDependencyStore(iom)
    assert set(['http://example.org/ctx']) == set(bds.contexts())


def test_excludes_contexts():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert set([]) == set(bds.contexts())


def test_excludes_some_contexts1():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx2')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert set(['http://example.org/ctx2']) == set(bds.contexts())


def test_excludes_some_contexts2():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx1')
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx2')
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx3')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx1']))
    assert set(['http://example.org/ctx2', 'http://example.org/ctx3']) == set(bds.contexts())


def test_empty_contexts_with_excludes():
    iom = Memory()
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert set([]) == set(bds.contexts())


def test_empty_contexts_without_excludes():
    iom = Memory()
    bds = BundleDependencyStore(iom)
    assert set([]) == set(bds.contexts())


def test_len_some_excludes():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx1')
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx2')
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx3')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx3']))
    assert 1 == len(bds)


def test_len_with_ctx_excluded1():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert 0 == bds.__len__('http://example.org/ctx')


def test_len_with_ctx_excluded2():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/d')),
            context='http://example.org/ctx')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert 0 == bds.__len__('http://example.org/ctx')


def test_triples_choices_excluded():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    iom.add((URIRef('http://example.org/e'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/d')),
            context='http://example.org/ctx')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert set() == set(bds.triples_choices(
        (None, None, [URIRef('http://example.org/c'),
                      URIRef('http://example.org/d')])))


def test_triples_choices_with_context_excluded():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    iom.add((URIRef('http://example.org/e'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/d')),
            context='http://example.org/ctx')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert set() == set(bds.triples_choices(
        (None, None, [URIRef('http://example.org/c'),
                      URIRef('http://example.org/d')]),
        context='http://example.org/ctx'))


def test_triples_choices_with_some_excluded():
    iom = Memory()
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context='http://example.org/ctx')
    iom.add((URIRef('http://example.org/e'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/d')),
            context='http://example.org/ctx1')
    bds = BundleDependencyStore(iom, excludes=set(['http://example.org/ctx']))
    assert set([(URIRef('http://example.org/e'),
                 URIRef('http://example.org/b'),
                 URIRef('http://example.org/d'))]) == set(t for t, _ in
                         bds.triples_choices(
                             (None, None, [URIRef('http://example.org/c'),
                                           URIRef('http://example.org/d')])))


def test_triples_contexts():
    iom = Memory()
    ctx = 'http://example.org/ctx'
    ctx1 = 'http://example.org/ctx1'
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context=ctx)
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context=ctx1)
    bds = BundleDependencyStore(iom)
    for t, ctxs in bds.triples((None, None, None)):
        assert set([ctx, ctx1]) == set(ctxs)


def test_triples_choices_contexts():
    iom = Memory()
    ctx = 'http://example.org/ctx'
    ctx1 = 'http://example.org/ctx1'
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context=ctx)
    iom.add((URIRef('http://example.org/a'),
             URIRef('http://example.org/b'),
             URIRef('http://example.org/c')),
            context=ctx1)
    bds = BundleDependencyStore(iom)
    for t, ctxs in bds.triples_choices(([URIRef('http://example.org/a')], None, None)):
        assert set([ctx, ctx1]) == set(ctxs)


def test_readonly_FileStorageZODB_is_cacheable():
    '''
    A read-only FileStorageZODBStore is cacheable since you should be able to use it any
    multiple threads without any problem. OTOH, with a writeable store of that type, there
    is an assumption that only one thread is using the store at a time which cannot easily
    be assured if we "cache" the store.
    '''
    assert _is_cacheable('FileStorageZODB', {'read_only': True})


def test_readonly_false_FileStorageZODB_is_not_cacheable():
    assert not _is_cacheable('FileStorageZODB', {'read_only': False})


def test_str_conf_FileStorageZODB_is_not_cacheable():
    '''
    By default, FileStorageZODB is a read/write store
    '''
    assert not _is_cacheable('FileStorageZODB', '/tmp/blah_blah')


def test_agg_with_readonly_FileStorageZODB_is_cacheable():
    assert _is_cacheable('agg', [['FileStorageZODB', {'read_only': True}]])


def test_agg_with_writeable_FileStorageZODB_is_not_cacheable():
    assert not _is_cacheable('agg', [
        ['FileStorageZODB', '/tmp/blah_blah'],
        ['FileStorageZODB', {'read_only': True}]])


def test_cache_key_is_not_none():
    cc1 = {'read_only': True,
           'some': 'other',
           'config': {'values': 'including',
                      'a': dict()}}
    ck1 = _cache_key('FileStorageZODB', cc1)
    assert ck1 is not None


def test_cache_key_same():
    cc1 = {'read_only': True,
           'some': 'other',
           'config': {'values': 'including',
                      'a': dict()}}
    cc2 = {'some': 'other',
           'read_only': True,
           'config': {'a': dict(),
                      'values': 'including'}}
    ck1 = _cache_key('FileStorageZODB', cc1)
    ck2 = _cache_key('FileStorageZODB', cc2)
    assert ck1 == ck2


def test_open_overlong_tuple():
    cut = BundleDependencyStore()
    with raises(ValueError):
        cut.open((1, 2, 3))


def test_open_overlong_list():
    cut = BundleDependencyStore()
    with raises(ValueError):
        cut.open([1, 2, 3])


def test_open_dict_missing_type():
    cut = BundleDependencyStore()
    with raises(ValueError):
        cut.open({'conf': 'doesntmatter'})


def test_open_dict_missing_conf():
    cut = BundleDependencyStore()
    with raises(ValueError):
        cut.open({'type': 'doesntmatter'})


def test_open_plugin_missing():
    with patch('owmeta_core.bundle_dependency_store._is_cacheable'), \
          patch('owmeta_core.bundle_dependency_store.plugin') as plugin:
        plugin.get.side_effect = PluginException
        cut = BundleDependencyStore()
        with raises(PluginException):
            cut.open({'conf': 'doesntmatter',
                'type': 'doesntmatter'})


def test_open_invalid_config():
    cut = BundleDependencyStore()
    with raises(ValueError):
        cut.open(object())


def test_not_cacheable():
    with patch('owmeta_core.bundle_dependency_store._is_cacheable') as is_cacheable, \
          patch('owmeta_core.bundle_dependency_store.plugin') as plugin:
        is_cacheable.return_value = False
        cut = BundleDependencyStore()
        cut.open(('StoreKey', 'StoreConf'))

        plugin.get.assert_called()
        assert cut.wrapped == plugin.get()()


def test_cached_used_when_cacheable():
    with patch('owmeta_core.bundle_dependency_store._is_cacheable') as is_cacheable, \
          patch('owmeta_core.bundle_dependency_store._cache_key') as cache_key, \
          patch('owmeta_core.bundle_dependency_store.plugin') as plugin:
        is_cacheable.return_value = True
        cache_key.return_value = 'cache_key'
        cut = BundleDependencyStore()
        store_cache = MagicMock()
        store_cache.get.return_value = Mock(name='CachedStore')
        cut.open(dict(type='StoreKey', conf='StoreConf', cache=store_cache))

        # test_use_cached_store_no_plugin
        plugin.get.assert_not_called()

        # test_cached_sought
        store_cache.get.assert_called()

        # test_cached_used
        assert cut.wrapped == store_cache.get()


def test_cached_store_created_when_cacheable():
    with patch('owmeta_core.bundle_dependency_store._is_cacheable') as is_cacheable, \
          patch('owmeta_core.bundle_dependency_store._cache_key') as cache_key, \
          patch('owmeta_core.bundle_dependency_store.plugin') as plugin:
        is_cacheable.return_value = True
        cache_key.return_value = 'cache_key'
        cut = BundleDependencyStore()
        store_cache = MagicMock()
        store_cache.get.return_value = None
        cut.open(dict(type='StoreKey', conf='StoreConf', cache=store_cache))

        # test_use_cached_store_no_plugin
        plugin.get.assert_called()

        # test_cached_sought
        store_cache.get.assert_called()

        # test_cached_not_used
        assert cut.wrapped == plugin.get()()


def test_bds_is_not_cacheable_FileStorageZODB_overlong_list():
    assert not _is_cacheable(RDFLIB_PLUGIN_KEY, [1, 2, 3])


def test_bds_is_not_cacheable_FileStorageZODB_overlong_tuple():
    assert not _is_cacheable(RDFLIB_PLUGIN_KEY, (1, 2, 3))


def test_bds_is_not_cacheable_FileStorageZODB_missing_type_key():
    assert not _is_cacheable(RDFLIB_PLUGIN_KEY, dict(conf='blahblah'))


def test_bds_is_not_cacheable_FileStorageZODB_missing_conf_key():
    assert not _is_cacheable(RDFLIB_PLUGIN_KEY, dict(type='blahblah'))


def test_bds_is_cacheable_for_readonly_FileStorageZODB_tuple_config():
    assert _is_cacheable(RDFLIB_PLUGIN_KEY, ('FileStorageZODB', {'read_only': True}))


def test_bds_is_cacheable_for_readonly_FileStorageZODB_list_config():
    assert _is_cacheable(RDFLIB_PLUGIN_KEY, ['FileStorageZODB', {'read_only': True}])


def test_bds_is_cacheable_for_readonly_FileStorageZODB_dict_config():
    assert _is_cacheable(RDFLIB_PLUGIN_KEY, dict(
        type='FileStorageZODB',
        conf={'read_only': True}))


def test_bds_is_not_cacheable_for_writeable_FileStorageZODB_dict_config():
    assert not _is_cacheable(RDFLIB_PLUGIN_KEY, dict(
        type='FileStorageZODB',
        conf={'read_only': False}))


def test_bds_is_cacheable_for_readonly_FileStorageZODB_dict_config_with_cache():
    assert _is_cacheable(RDFLIB_PLUGIN_KEY, dict(
        type='FileStorageZODB',
        conf={'read_only': True},
        cache=MagicMock()))


def test_bds_is_not_cacheable_for_unknown_config_type():
    assert not _is_cacheable(RDFLIB_PLUGIN_KEY, object())


def test_bds_reuse():
    with patch('owmeta_core.bundle_dependency_store._is_cacheable') as is_cacheable, \
          patch('owmeta_core.bundle_dependency_store._cache_key'), \
          patch('owmeta_core.bundle_dependency_store.plugin'):
        cut = BundleDependencyStore()
        store_cache = MagicMock()
        conf = dict(type='StoreKey', conf='StoreConf', cache=store_cache)
        cut.open(conf)
        is_cacheable.assert_called_with(RDFLIB_PLUGIN_KEY, conf)
        assert cut.wrapped == store_cache.get()


def test_open_open_close_close_1(tempdir):
    '''
    A cached store, once opened, cannot be closed unilaterally by a BDS holdidng a
    reference to that store. Consequently, we must prevent closing of the store. However,
    calling 'close' on the store must remain an allowed operation (i.e., it must not raise
    an exception) so that the sharing of the store remains transparent to the BDS user.
    '''
    bds1 = BundleDependencyStore()
    bds2 = BundleDependencyStore()
    store_cache = StoreCache()
    store = plugin_get('FileStorageZODB', Store)()
    store.open(p(tempdir, 'db.fs'))
    store.close()

    conf = dict(
        type='FileStorageZODB',
        conf={'read_only': True, 'url': p(tempdir, 'db.fs')},
        cache=store_cache)

    print("OPEN BDS 1")
    bds1.open(conf)
    print("OPEN BDS 2")
    bds2.open(conf)

    print("CLOSE BDS 1")
    bds1.close()
    print("CLOSE BDS 2")
    bds2.close()


def test_open_open_close_close_2(tempdir):
    '''
    A cached store, once opened, cannot be closed unilaterally by a BDS holdidng a
    reference to that store. Consequently, we must prevent closing of the store. However,
    calling 'close' on the store must remain an allowed operation (i.e., it must not raise
    an exception) so that the sharing of the store remains transparent to the BDS user.
    '''
    bds1 = BundleDependencyStore()
    bds2 = BundleDependencyStore()
    store_cache = StoreCache()
    store = plugin_get('FileStorageZODB', Store)()
    store.open(p(tempdir, 'db.fs'))
    store.close()

    conf = dict(
        type='FileStorageZODB',
        conf={'read_only': True, 'url': p(tempdir, 'db.fs')},
        cache=store_cache)

    print("OPEN BDS 1")
    bds1.open(conf)
    print("OPEN BDS 2")
    bds2.open(conf)

    print("CLOSE BDS 2")
    bds2.close()
    print("CLOSE BDS 1")
    bds1.close()


def test_open_open_close_query(tempdir):
    bds1 = BundleDependencyStore()
    bds2 = BundleDependencyStore()
    store_cache = StoreCache()
    store = plugin_get('FileStorageZODB', Store)()
    store.open(p(tempdir, 'db.fs'))
    trip = (URIRef('http://example.org/a'),
            URIRef('http://example.org/b'),
            URIRef('http://example.org/c'))
    store.add(trip, context=None)
    store.close()

    conf = dict(
        type='FileStorageZODB',
        conf={'read_only': True, 'url': p(tempdir, 'db.fs')},
        cache=store_cache)

    print("OPEN BDS 1")
    bds1.open(conf)
    print("OPEN BDS 2")
    bds2.open(conf)

    print("CLOSE BDS 1")
    bds1.close()

    assert list(bds2.triples((None, None, None)))[0][0] == trip


def test_unimplemented():
    unimplemented_methods = ['add',
                             'addN',
                             'remove',
                             'add_graph',
                             'remove_graph',
                             'create',
                             'destroy',
                             'commit',
                             'rollback',
                             ]
    cut = BundleDependencyStore()
    for method_name in unimplemented_methods:
        with raises(NotImplementedError):
            getattr(cut, method_name)()


def test_pure_pass_throughs():
    wrapped = Mock(name='wrapped')
    cut = BundleDependencyStore(wrapped)

    cut.prefix('blah')
    wrapped.prefix.assert_called_with('blah')

    cut.namespace('blah')
    wrapped.namespace.assert_called_with('blah')

    cut.gc()
    wrapped.gc.assert_called()


def test_repr_contains_wrapped():
    cut = BundleDependencyStore('wrapped')
    assert repr('wrapped') in repr(cut)
