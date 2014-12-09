import itertools
import logging


def split_seq(iterable, size):
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))


def load_as_set(filename):
    return set([l.rstrip() for l in open(filename) if l.strip() != ''])


def squelch_errors(method):
    '''
    Decorator to squelch most errors.
    Just output them to stderr
    '''
    def raising(fn):
        def run(*args):
            try:
                return fn(*args)
            except Exception as e:
                tpl = (fn.__name__, e.__class__.__name__, str(e))
                logging.error("Exception squelched inside:'%s': %s (%s)" % tpl)
                pass
        return run
    return raising(method)
