"""Pipeline Engine Hooks
"""
from contextlib import contextmanager

from interface import Interface, implements


class PipelineHooks(Interface):

    def on_create_execution_plan(self, plan):
        pass

    @contextmanager
    def on_run_chunked_pipeline(self, start_date, end_date):
        pass

    @contextmanager
    def on_run_pipeline(self, start_date, end_date):
        return
        yield

    @contextmanager
    def compute_term(self, term):
        return
        yield


class NoOpHooks(implements(PipelineHooks)):
    """
    A PipelineHooks that defines no-op methods for all available hooks.

    Use this as a base class if you only want to implement a subset of all
    possible hooks.
    """


class LogProgressHook(implements(PipelineHooks)):

    def __init__(self, notify):
        self.notify = notify
        self._in_chunked_pipeline = False

    def on_create_execution_plan(self, plan):
        self.notify("Created execution plan.")

    @contextmanager
    def on_run_chunked_pipeline(self, start_date, end_date):
        notify = self.notify
        args = ("pipeline", start_date, end_date)
        try:
            self._in_chunked_pipeline = True
            notify("Running %s from %s to %s" % args)
            yield
        finally:
            notify("Finished running %s from %s to %s" % args)

    @contextmanager
    def on_run_pipeline(self, start_date, end_date):
        notify = self.notify
        noun = "pipeline chunk" if self._in_chunked_pipeline else "pipeline"
        args = (noun, start_date, end_date)
        try:
            notify("Running %s from %s to %s" % args)
            yield
        finally:
            notify("Finished running %s from %s to %s" % args)


def delegating_hooks_method(method_name):
    def method(self, *args, **kwargs):
        for hook in self._hooks:
            getattr(hook, method_name)(*args, **kwargs)
    return method


class DelegatingHooks(implements(PipelineHooks)):
    """A PipelineHooks that delegates to one or more other hooks.
    """
    def __new__(cls, hooks):
        if len(hooks) == 0:
            return NoOpHooks()
        elif len(hooks) == 1:
            return hooks[0]
        else:
            return super(DelegatingHooks, cls).__new__(cls, hooks)

    def __init__(self, hooks):
        self._hooks = hooks

    # Implement all interface methods by delegating to corresponding methods on
    # input hooks.
    locals().update({
        name: delegating_hooks_method(name)
        for name in vars(PipelineHooks)
    })


del delegating_hooks_method
