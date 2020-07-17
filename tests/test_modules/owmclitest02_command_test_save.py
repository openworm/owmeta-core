from test_module.monkey import Monkey


def owm_data(ns):
    ns.context.add_import(Monkey.definition_context)
    ns.context(Monkey)(bananas=55)
