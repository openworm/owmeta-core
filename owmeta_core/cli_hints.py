'''
Hints for the CLI wrapper that help mapping from the Python methods to command line
arguments.

:CLI_HINTS: hints accepted by `~.cli_command_wrapper.CLICommandWrapper`
'''

from .cli_common import METHOD_NAMED_ARG

CLI_HINTS = {
    'owmeta_core.command.OWM': {
        'commit': {
            (METHOD_NAMED_ARG, 'message'): {
                'names': ['--message', '-m'],
            },
        },
        'context': {
            (METHOD_NAMED_ARG, 'context'): {
                'nargs': '?',
                'names': ['context'],
            },
        },
        'imports_context': {
            (METHOD_NAMED_ARG, 'context'): {
                'nargs': '?',
                'names': ['context'],
            },
        },
        'clone': {
            (METHOD_NAMED_ARG, 'url'): {
                'names': ['url'],
            },
        },
        'translate': {
            (METHOD_NAMED_ARG, 'translator'): {
                'names': ['translator']
            },
            (METHOD_NAMED_ARG, 'data_sources'): {
                'nargs': '*',
                'names': ['data_sources'],
            },
        },
        'say': {
            (METHOD_NAMED_ARG, 'subject'): {
                'names': ['subject']
            },
            (METHOD_NAMED_ARG, 'property'): {
                'names': ['property']
            },
            (METHOD_NAMED_ARG, 'object'): {
                'names': ['object']
            }
        },
        'save': {
            (METHOD_NAMED_ARG, 'module'): {
                'names': ['module']
            }
        },
        'serialize': {
            (METHOD_NAMED_ARG, 'destination'): {
                'names': ['--destination', '-w']
            },
            (METHOD_NAMED_ARG, 'format'): {
                'names': ['--format', '-f']
            }
        },
        'non_interactive': {
            'names': ['--non-interactive', '-b']
        },
        'IGNORE': ['message', 'progress_reporter', 'prompt', 'connect', 'disconnect', 'rdf', 'default_context']
    },
    'owmeta_core.command.OWMContexts': {
        'list_imports': {
            (METHOD_NAMED_ARG, 'context'): {
                'names': ['context'],
            },
        },
        'list_importers': {
            (METHOD_NAMED_ARG, 'context'): {
                'names': ['context'],
            },
        },
        'bundle': {
            (METHOD_NAMED_ARG, 'context'): {
                'names': ['context'],
            },
        },
        'edit': {
            (METHOD_NAMED_ARG, 'context'): {
                'names': ['context'],
                'nargs': '?'
            },
            (METHOD_NAMED_ARG, 'list_formats'): {
                'nargs': '?'
            }
        },
        'rm': {
            (METHOD_NAMED_ARG, 'context'): {
                'names': ['context'],
                'nargs': '?'
            },
        },
        'rm_import': {
            (METHOD_NAMED_ARG, 'importer'): {
                'names': ['importer'],
            },
            (METHOD_NAMED_ARG, 'imported'): {
                'names': ['imported'],
                'nargs': '*'
            },
        }
    },
    'owmeta_core.commands.bundle.OWMBundleRemote': {
        'show': {
            (METHOD_NAMED_ARG, 'name'): {
                'names': ['name'],
            },
        },
        'remove': {
            (METHOD_NAMED_ARG, 'name'): {
                'names': ['name'],
            },
        }
    },
    'owmeta_core.commands.bundle.OWMBundleRemoteAdd': {
        '__call__': {
            (METHOD_NAMED_ARG, 'name'): {
                'names': ['name'],
            },
            (METHOD_NAMED_ARG, 'url'): {
                'names': ['url'],
            },
        },
    },
    'owmeta_core.commands.bundle.OWMBundleRemoteUpdate': {
        '__call__': {
            (METHOD_NAMED_ARG, 'name'): {
                'names': ['name'],
            },
            (METHOD_NAMED_ARG, 'url'): {
                'names': ['url'],
            },
        },
    },
    'owmeta_core.commands.bundle.OWMBundle': {
        'fetch': {
            (METHOD_NAMED_ARG, 'bundle_id'): {
                'names': ['bundle_id'],
            },
        },
        'load': {
            (METHOD_NAMED_ARG, 'input_file_name'): {
                'names': ['input'],
            },
        },
        'save': {
            (METHOD_NAMED_ARG, 'bundle_id'): {
                'names': ['bundle_id'],
            },
            (METHOD_NAMED_ARG, 'output'): {
                'names': ['output'],
            },
        },
        'install': {
            (METHOD_NAMED_ARG, 'bundle'): {
                'names': ['bundle'],
            },
        },
        'deregister': {
            (METHOD_NAMED_ARG, 'bundle_id'): {
                'names': ['bundle_id'],
            },
        },
        'register': {
            (METHOD_NAMED_ARG, 'descriptor'): {
                'names': ['descriptor'],
            },
        },
        'deploy': {
            (METHOD_NAMED_ARG, 'bundle_id'): {
                'names': ['bundle_id'],
            },
        },
        'checkout': {
            (METHOD_NAMED_ARG, 'bundle_id'): {
                'names': ['bundle_id'],
            },
        },
    },
    'owmeta_core.command.OWMSource': {
        'show': {
            (METHOD_NAMED_ARG, 'data_source'): {
                'names': ['data_source'],
            },
        },
        'derivs': {
            (METHOD_NAMED_ARG, 'data_source'): {
                'names': ['data_source'],
            },
        },
    },
    'owmeta_core.command.OWMSourceData': {
        'retrieve': {
            (METHOD_NAMED_ARG, 'source'): {
                'names': ['source'],
            },
            (METHOD_NAMED_ARG, 'archive'): {
                'names': ['archive'],
            },
        },
    },
    'owmeta_core.command.OWMTranslator': {
        'show': {
            (METHOD_NAMED_ARG, 'translator'): {
                'names': ['translator'],
            },
        },
        'create': {
            (METHOD_NAMED_ARG, 'translator_type'): {
                'names': ['translator_type'],
            },
        },
    },
    'owmeta_core.command.OWMConfig': {
        'set': {
            (METHOD_NAMED_ARG, 'key'): {
                'names': ['key'],
            },
            (METHOD_NAMED_ARG, 'value'): {
                'names': ['value'],
            },
        },
        'get': {
            (METHOD_NAMED_ARG, 'key'): {
                'names': ['key'],
            },
        },
        'delete': {
            (METHOD_NAMED_ARG, 'key'): {
                'names': ['key'],
            },
        },
    },
}
