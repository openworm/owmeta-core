import logging

from .configure import Configurable
from .capability import Capable, provide
from .utils import retrieve_provider


CAPABILITY_PROVIDERS_KEY = 'capability.providers'


class CapableConfigurable(Capable, Configurable):
    '''
    Helper class for `.Capable` objects that are also `.Configurable`

    Takes the providers from the :confval:`capability.providers` configuration value and
    calls `provide` with the resulting providers. If the value is unset or empty, then
    `provide` will not be called.

    .. confval:: capability.providers

         a list of `"provider path" <.utils.retrieve_provider>` strings or `Providers
         <.capability.Provider>`
    '''
    def __init__(self, *args, **kwargs):
        '''
        Raises
        ------
        NoProviderAvailable
            if any of the needed capabilities cannot be provided
        '''
        super().__init__(*args, **kwargs)
        conf_providers = self.conf.get(CAPABILITY_PROVIDERS_KEY, [])
        if conf_providers:
            providers = []
            for p in conf_providers:
                if isinstance(p, str):
                    provider = retrieve_provider(p)()
                else:
                    provider = p
                if not provider:
                    raise Exception('No provider found for ' + p)
                providers.append(provider)
            provide(self, providers)
        else:
            logging.debug("No providers for %s in %r", self, self.conf)
