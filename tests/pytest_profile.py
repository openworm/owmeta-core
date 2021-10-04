# This file specified pytest plugins

from __future__ import absolute_import
from __future__ import print_function
import pstats
import cProfile
import json
import os
from os.path import join as p
import pytest


# Module level, to pass state across tests.  This is not multiprocessing-safe.
function_profile_list = []
enabled = False


def pytest_addoption(parser):
    parser.addoption('--prof', dest='prof', action='store_true', help='Do cProfile stats collection')


def pytest_configure(config):
    """
    Called before tests are collected.
    """
    global enabled

    enabled = config.getoption('prof')


@pytest.mark.hookwrapper
def pytest_runtest_call(item):
    """
    Calls once per test.
    """
    global function_profile_list, enabled

    if not enabled:
        yield
        return

    item.profiler = cProfile.Profile()

    item.profiler.enable()
    outcome = yield
    item.profiler.disable()

    # Item's excinfo will indicate any exceptions thrown
    if enabled and outcome.excinfo is None:
        # item.originalname must be used because item.name can include a label from
        # parametrize
        fp = FunctionProfile(cprofile=item.profiler, function_name=item.originalname)
        function_profile_list.append(fp)


def pytest_unconfigure(config):
    """
    Called after all tests are completed.
    """
    global enabled

    if not enabled:
        return

    os.makedirs('.prof', exist_ok=True)
    summary = {'cumulative_time': {},
               'total_time': {}}
    for fp in function_profile_list:
        fp.profile.dump_stats(p('.prof', fp.function_name))
        for k in summary:
            summary[k][fp.function_name] = getattr(fp, k)
    with open(p('.prof', 'summary'), 'w') as f:
        json.dump(summary, f, indent=4)


class FunctionProfile(object):

    def __init__(self, cprofile, function_name):
        """
        :param cprofile: Cprofile object created by cProfile.Profile().  Must be paired with function_name parameter.
        :param function_name: Name of function profiled.  Must be paired with cprofile parameter.
        :param json: Create a function profile from a JSON string.  Overridden by cprofile/functionname parameters.

        >>> pr = cProfile.Profile()
        >>> pr.enable()
        >>> x = map(lambda x: x**2, xrange(1000))
        >>> pr.disable()
        >>> function_profile = FunctionProfile(pr, "map")
        >>> print function_profile
        """

        if not cprofile or not function_name:
            raise ValueError("Invalid initialization arguments to FunctionProfile.")

        stats = pstats.Stats(cprofile, stream=open(os.devnull, "w"))

        _, lst = stats.get_print_list("")

        function_tuple = None
        for func_tuple in lst:
            if function_name in func_tuple[2]:
                function_tuple = func_tuple
                break
        else: # no break
            possible_methods = ", ".join(x[2] for x in lst)
            raise ValueError("Function Profile received invalid function name " +
                         "<{}>.  Options are: {}".format(function_name, str(possible_methods)))

        # stats.stats[func_tuple] returns tuple of the form:
        #  (# primitive (non-recursive) calls , # calls, total_time, cumulative_time, dictionary of callers)
        stats_tuple = stats.stats[function_tuple]
        self.profile = cprofile
        self.function_name = function_name
        self.primitive_calls = stats_tuple[0]
        self.calls = stats_tuple[1]
        self.total_time = stats_tuple[2]
        self.cumulative_time = stats_tuple[3]

    def __iter__(self):
        return iter((self.function_name,
                self.primitive_calls,
                self.calls,
                self.total_time,
                self.cumulative_time))

    def __str__(self):
        l = []
        l.append("Function Name: " + self.function_name)
        l.append("Primitive Calls: " + str(self.primitive_calls))
        l.append("Calls: " + str(self.calls))
        l.append("Total Time: " + str(self.total_time))
        l.append("Cumulative Time: " + str(self.cumulative_time))
        # l.append("Callers: " + str(self.callers))
        return "\n".join(l)
