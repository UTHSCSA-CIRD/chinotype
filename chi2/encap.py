'''encap -- lexical scoping for encapsulation
Written by DanC of KUMC apparently but not available via standard
distribution channels, so, welcome to CIRD's repo.

'''


class ESuite(object):
    '''ESuite -- Encapsulated (or: E-like) method suite

    Define methods in the scope of the `__new__` method. Note access
    to constructor args does not require `self.` prefix::

      >>> class Ex(ESuite):
      ...     def __new__(cls, x):
      ...         def double(self):
      ...             "make it bigger!"
      ...             return x + x
      ...         return cls.make(double)

      >>> it = Ex(4)
      >>> it.double()
      8

    The state of the object is encapsulated*::

      >>> it.__dict__.keys()
      []

    Inheritance is done by delegation:

      >>> class Ex2(ESuite):
      ...     def __new__(cls, x):
      ...         x2 = Ex(x)
      ...         def quadruple(self):
      ...             "make it even bigger-er!"
      ...             return x2.double() * 2
      ...         return cls.make(quadruple, delegate=x2)

      >>> it = Ex2(4)
      >>> it.double()
      8
      >>> it.quadruple()
      16


    TODO: take another look at making docstrings visible.

    * modulo various stack introspection mechanisms.
    '''
    def __repr__(self):
        return '%s(...)' % self.__class__.__name__

    @classmethod
    def make(cls, *args, **kwargs):
        arg_methods = [(f.__name__, f) for f in args]

        delegate = kwargs.get('delegate', None)

        def nextattr(self, n):
            return getattr(delegate, n)

        delegate_methods = [('__getattr__', nextattr)
                            for once in [1]
                            if delegate is not None]
        suite = dict(arg_methods + delegate_methods,
                     **kwargs)

        return type(cls.__name__, (ESuite, object), suite)()


def slot(obj):
    '''Make a mutable slot, since python (2.x) closures are-read only.

    >>> x = slot(1)
    >>> update(x, 5)
    >>> val(x)
    5
    '''
    return [obj]


def val(slot):
    return slot[0]


def update(slot, val):
    slot[0] = val
