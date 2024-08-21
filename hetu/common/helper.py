import itertools


def batched(iterable, n):
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def resolve_import(s):
    """
    Resolve strings to objects using standard import and attribute
    syntax.
    """
    name = s.split('.')
    used = name.pop(0)
    try:
        found = __import__(used)
        for frag in name:
            used += '.' + frag
            try:
                found = getattr(found, frag)
            except AttributeError:
                __import__(used)
                found = getattr(found, frag)
        return found
    except ImportError as e:
        v = ValueError('Cannot resolve %r: %s' % (s, e))
        raise v from e