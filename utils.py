import itertools


def split_seq(iterable, size):
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))


def load_as_set(filename):
    return set([l.rstrip() for l in open(filename) if l.strip() != ''])
