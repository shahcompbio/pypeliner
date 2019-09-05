""" Reworked version of standard libary copy module for transforming
arbitrary python objects.
"""

import sys
import types
import weakref

import six

if sys.version_info[0] < 3:
    from copy_reg import dispatch_table
else:
    from copyreg import dispatch_table

    long = int
    xrange = range


class Error(Exception):
    pass


error = Error  # backward compatibility

__all__ = ["Error", "copy", "deeptransform"]


def deeptransform(x, f, memo=None, _nil=[]):
    """Deep transform operation on arbitrary Python objects.

    See the module's __doc__ string for more info.
    """

    if memo is None:
        memo = {}

    d = id(x)
    y = memo.get(d, _nil)
    if y is not _nil:
        return y

    cls = type(x)

    y, transformed = f(x)
    if not transformed:
        copier = _deeptransform_dispatch.get(cls)
        if copier:
            y = copier(x, f, memo)
        else:
            try:
                issc = issubclass(cls, type)
            except TypeError:  # cls is not a class (old Boost; see SF #502085)
                issc = 0
            if issc:
                y = _deeptransform_atomic(x, f, memo)
            else:
                copier = getattr(x, "__deeptransform__", None)
                if copier:
                    y = copier(memo)
                else:
                    reductor = dispatch_table.get(cls)
                    if reductor:
                        rv = reductor(x)
                    else:
                        reductor = getattr(x, "__reduce_ex__", None)
                        if reductor:
                            rv = reductor(2)
                        else:
                            reductor = getattr(x, "__reduce__", None)
                            if reductor:
                                rv = reductor()
                            else:
                                raise Error(
                                    "un(deep)copyable object of type %s" % cls)
                    y = _reconstruct(x, f, rv, 1, memo)

    memo[d] = y
    _keep_alive(x, f, memo)  # Make sure x lives at least as long as d
    return y


_deeptransform_dispatch = d = {}


def _deeptransform_atomic(x, f, memo):
    return x


d[type(None)] = _deeptransform_atomic
d[type(Ellipsis)] = _deeptransform_atomic
d[int] = _deeptransform_atomic
d[float] = _deeptransform_atomic
d[bool] = _deeptransform_atomic
try:
    d[complex] = _deeptransform_atomic
except NameError:
    pass
d[str] = _deeptransform_atomic
try:
    d[unicode] = _deeptransform_atomic
except NameError:
    pass
try:
    d[types.CodeType] = _deeptransform_atomic
except AttributeError:
    pass
d[type] = _deeptransform_atomic
d[xrange] = _deeptransform_atomic
# d[types.ClassType] = _deeptransform_atomic
d[types.BuiltinFunctionType] = _deeptransform_atomic
d[types.FunctionType] = _deeptransform_atomic
d[weakref.ref] = _deeptransform_atomic


def _deeptransform_list(x, f, memo):
    y = []
    memo[id(x)] = y
    for a in x:
        y.append(deeptransform(a, f, memo))
    return y


d[list] = _deeptransform_list


def _deeptransform_tuple(x, f, memo):
    y = []
    for a in x:
        y.append(deeptransform(a, f, memo))
    d = id(x)
    try:
        return memo[d]
    except KeyError:
        pass
    for i in range(len(x)):
        if x[i] is not y[i]:
            y = tuple(y)
            break
    else:
        y = x
    memo[d] = y
    return y


d[tuple] = _deeptransform_tuple


def _deeptransform_dict(x, f, memo):
    y = {}
    memo[id(x)] = y
    for key, value in x.items():
        y[deeptransform(key, f, memo)] = deeptransform(value, f, memo)
    return y


d[dict] = _deeptransform_dict


def _deeptransform_method(x, f, memo):  # Copy instance methods
    try:
        return type(x)(x.__func__, deeptransform(x.__self__, f, memo), x.__class__)
    except Exception as e:
        return type(x)(x.__func__, deeptransform(x.__self__, f, memo))


_deeptransform_dispatch[types.MethodType] = _deeptransform_method


def _keep_alive(x, f, memo):
    """Keeps a reference to the object x in the memo.

    Because we remember objects by their id, we have
    to assure that possibly temporary objects are kept
    alive by referencing them.
    We store a reference at the id of the memo, which should
    normally not be used unless someone tries to deeptransform
    the memo itself...
    """
    try:
        memo[id(memo)].append(x)
    except KeyError:
        # aha, this is the first one :-)
        memo[id(memo)] = [x]


def _deeptransform_inst(x, f, memo):
    if hasattr(x, '__deeptransform__'):
        return x.__deeptransform__(memo)
    if hasattr(x, '__getinitargs__'):
        args = x.__getinitargs__()
        args = deeptransform(args, f, memo)
        y = x.__class__(*args)
    else:
        y = _EmptyClass()
        y.__class__ = x.__class__
    memo[id(x)] = y
    if hasattr(x, '__getstate__'):
        state = x.__getstate__()
    else:
        state = x.__dict__
    state = deeptransform(state, f, memo)
    if hasattr(y, '__setstate__'):
        y.__setstate__(state)
    else:
        y.__dict__.update(state)
    return y


if sys.version_info[0] < 3:
    d[types.InstanceType] = _deeptransform_inst


def _reconstruct(x, f, info, deep, memo=None):
    if isinstance(info, six.string_types):
        return x
    assert isinstance(info, tuple)
    if memo is None:
        memo = {}
    n = len(info)
    assert n in (2, 3, 4, 5)
    callable, args = info[:2]
    if n > 2:
        state = info[2]
    else:
        state = None
    if n > 3:
        listiter = info[3]
    else:
        listiter = None
    if n > 4:
        dictiter = info[4]
    else:
        dictiter = None
    if deep:
        args = deeptransform(args, f, memo)
    y = callable(*args)
    memo[id(x)] = y

    if state is not None:
        if deep:
            state = deeptransform(state, f, memo)
        if hasattr(y, '__setstate__'):
            y.__setstate__(state)
        else:
            if isinstance(state, tuple) and len(state) == 2:
                state, slotstate = state
            else:
                slotstate = None
            if state is not None:
                y.__dict__.update(state)
            if slotstate is not None:
                for key, value in slotstate.items():
                    setattr(y, key, value)

    if listiter is not None:
        for item in listiter:
            if deep:
                item = deeptransform(item, f, memo)
            y.append(item)
    if dictiter is not None:
        for key, value in dictiter:
            if deep:
                key = deeptransform(key, f, memo)
                value = deeptransform(value, f, memo)
            y[key] = value
    return y


del d

del types


# Helper for instance creation without calling __init__
class _EmptyClass:
    pass
