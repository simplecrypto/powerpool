import collections
import importlib


def time_format(seconds):
    # microseconds
    if seconds <= 1.0e-3:
        return "{:,.4f} us".format(seconds * 1000000.0)
    if seconds <= 1.0:
        return "{:,.4f} ms".format(seconds * 1000.0)
    return "{:,.4f} sec".format(seconds)


def recursive_update(d, u):
    """ Simple recursive dictionary update """
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = recursive_update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


def import_helper(dotted_path):
    module, cls = dotted_path.rsplit(".", 1)
    return getattr(importlib.import_module(module), cls)
