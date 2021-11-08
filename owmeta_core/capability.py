'''
Defines 'capabilities', pieces of functionality that an object needs which must
be injected. The receiver of the capability is called a :dfn:`capable`.

A given capability can be provided by more than one capability provider, but,
for a given set of providers, only one will be bound at a time. Logically, each
provider that provides the capability is asked, in a user-provided preference
order, whether it can provide the capability for the *specific* capable and the
first one which can provide the capability is bound to the object.

The core idea is dependency injection: a capability does not modify the capable:
the capable receives the provider and a reference to the capability provided,
but how the capable uses the provider is up to the capable. This is important
because the user of the capable should not condition its behavior on the
particular capability provider used, although it may change its behavior based
on which capabilities the capable has.

Note, that there may be some providers that lose their ability to provide a
capability after they have been bound to a capable. This loss should be
communicated with a `CannotProvideCapability` exception when the relevant
methods are called on the provider. This *may* allow certain operations to be
retried with a provider lower on the capability order, *but* a provider that
throws `CannotProvideCapability` may validly be asked if it can provide the
capability again -- if it *still* cannot provide the capability, it should
communicate that by returning `None` from its `provides_to` method.

Providers may keep state between calls to provide a capability but their
correctness must not depend on any ordering of method calls except that, of
course, their ``__init__`` is called first. For instance, a provider can retain
an index that it downloads to answer `provides_to`, but if that index can
expire, the provider should check for that and retrieve an updated index if
necessary.
'''
import six
from .utils import FCN
from itertools import chain, repeat


class _Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            _Singleton._instances[cls] = super(_Singleton, cls).__call__(*args, **kwargs)
        return _Singleton._instances[cls]


class Capability(six.with_metaclass(_Singleton)):
    '''
    A capability.
    '''
    def __init__(self):
        self._class_name = FCN(type(self))

    def __str__(self):
        return self._class_name

    def __lt__(self, other):
        if isinstance(other, Capability):
            return self._class_name < other._class_name
        return NotImplemented


class ProviderMeta(type):
    def __init__(self, name, bases, dct):
        super(ProviderMeta, self).__init__(name, bases, dct)
        my_caps = set(self.provided_capabilities)
        for base in bases:
            for base_cap in base.provided_capabilities:
                if base_cap not in my_caps:
                    my_caps.add(base_cap)
        self.provided_capabilities = sorted(my_caps)


class Provider(metaclass=ProviderMeta):
    '''
    A capability provider.

    In general, providers should do any general setup in their initializer, and setup for
    any source passed into `provides_to` method if, in fact, the provider does provide the
    needed capabilities
    '''
    provided_capabilities = []

    def provides(self, cap, obj):
        '''
        Returns a provider of the given capability if it's one this provider provides;
        otherwise, returns None.

        Parameters
        ----------
        cap : Capability
            The capability to provide
        obj : Capable
            The object to provide the capability to

        Returns
        -------
        Provider or None
        '''
        if cap in getattr(self, 'provided_capabilities', ()):
            return self.provides_to(obj, cap)

        return None

    def provides_to(self, obj, cap):
        '''
        Returns a `Provider` if the provider provides a capability to the given object;
        otherwise, returns `None`.

        The default implementation always returns `None`. Implementers of `Provider`
        should check they can actually provide the capability for the given object rather
        than just that they *might* be able to.

        It's best to do setup for providing the capability before exiting this method
        rather than, for instance, in the methods of the returned provider when the
        `Capable` is trying to use it.

        Parameters
        ----------
        obj : Capable
            The object needing/wanting the capability
        cap : Capability
            The capability needed/wanted

        Returns
        -------
        Provider or None
        '''
        return None


class Capable(object):
    '''
    An object which can have capabilities
    '''

    @property
    def needed_capabilities(self):
        '''
        The list of needed capabilities. These should be treated as though they are
        required for any of the object's methods.
        '''
        return []

    @property
    def wanted_capabilities(self):
        '''
        The list of wanted capabilities. These should be treated as though they are
        optional. The `Capable` subclass must determine how to deal with the provider not
        being available.
        '''
        return []

    def accept_capability_provider(self, cap, provider):
        '''
        The `Capable` should replace any previously accepted provider with the one
        given.

        The capability *should* be checked to determine which capability is being
        provided, even if only one is declared on the class, so that if a sub-class
        defines a capability without defining how to accept it, then the wrong actions
        won't be taken. In case the capability isn't recognized, it is generally better to
        pass it to the super() implementation rather than failing to allow for
        `cooperative multiple inheritance`__.

        __ https://rhettinger.wordpress.com/2011/05/26/super-considered-super/

        Parameters
        ----------
        cap : Capability
            the capabiilty
        provider : Provider
            the provider which provides `cap`
        '''
        raise NotImplementedError()


class CannotProvideCapability(Exception):
    '''
    Thrown by a *provider* when it cannot provide the capability during the
    object's execution
    '''
    def __init__(self, cap, provider):
        '''
        Parameters
        ----------
        cap : Capability
            the capabiilty
        provider : Provider
            the provider which failed to provide `cap`
        '''
        super(CannotProvideCapability, self).__init__('Provider, {}, cannot, now, provide the capability, {}'
                                                      .format(provider, cap))
        self._cap = cap
        self._provider = provider


class NoProviderAvailable(Exception):
    '''
    Thrown when there is no provider available for a capabiilty

    Attributes
    ----------
    cap : Capability
        The capability that was sought
    receiver : Capable
        The object for which the capability was sought
    '''
    def __init__(self, cap, receiver, providers):
        '''
        Parameters
        ----------
        cap : Capability
            The capability that was sought
        receiver : Capable
            The object for which the capability was sought
        providers : list of Provider
            Providers that were tried for the capability
        '''
        super(NoProviderAvailable, self).__init__(
                f'No providers currently provide {cap} for {receiver} among {providers}')
        self._cap = cap
        self._receiver = receiver

    @property
    def capability(self):
        return self._cap

    @property
    def receiver(self):
        return self._receiver


class NoProviderGiven(Exception):
    '''
    Thrown by a `Capable` when a `Capability` is needed, but none has been
    provided by a call to `~Capable.accept_capability_provider`
    '''
    def __init__(self, cap, receiver=None):
        '''
        Parameters
        ----------
        cap : Capability
            The capability that was sought
        receiver : Capable
            The object for which a capability was needed
        '''
        super(NoProviderGiven, self).__init__('No {} providers were given{}'
                .format(cap, ' to ' + repr(receiver) if receiver else ''))
        self._cap = cap


class UnwantedCapability(Exception):
    '''
    Thrown by a `Capable` when `~Capable.accept_capability_provider` is offered a provider
    for a capability that it does not "want", meaning it doesn't have the code to use it.
    This can happen when a sub-class of a Capable declares a needed capability without
    overriding `~Capable.accept_capability_provider` to accept that capability.
    '''


def provide(ob, provs):
    '''
    Provide capabilities to `ob` out of `provs`

    Parameters
    ----------
    ob : object
        An object which may need capabilities
    provs : list of Provider
        The providers available

    Raises
    ------
    NoProviderAvailable
        when there is no provider available
    '''
    if is_capable(ob):
        for required, cap in chain(
                zip(repeat(True), ob.needed_capabilities),
                zip(repeat(False), ob.wanted_capabilities)):
            provider = get_provider(ob, cap, provs)
            if not provider and required:
                raise NoProviderAvailable(cap, ob, provs)
            if provider:
                ob.accept_capability_provider(cap, provider)


def get_provider(ob, cap, provs):
    '''
    Get provider for a capabilty that can provide to the given object

    Parameters
    ----------
    ob : Capable
        Object needing the capability
    cap : Capability
        Capability needed
    provs : list of Provider
        All providers available

    Returns
    -------
    Provider
        A provider of the given capability or `None`
    '''
    for provider in get_providers(cap, provs, ob):
        return provider
    return None


def get_providers(cap, provs, ob):
    '''
    Get providers for a capabilty

    Parameters
    ----------
    cap : Capability
        Capability needed
    provs : list of Provider
        All providers available

    Yields
    ------
    Provider
        A Provider that provides the given capability
    '''
    for p in provs:
        provfn = p.provides(cap, ob)
        if provfn:
            yield provfn


def is_capable(ob):
    '''
    Returns true if the given object can accept capability providers

    Parameters
    ----------
    ob : object
        An object which may be a `Capable`

    Returns
    -------
    bool
        True if the given object accepts capability providers of some kind. Otherwise,
        false.
    '''
    return isinstance(ob, Capable)
