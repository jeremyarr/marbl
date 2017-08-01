class Trigger(object):
    def __init__(self):
        self._trigger = False

    def __get__(self, obj, objtype):
        print("retrieving")
        return self._trigger

    def __set__(self, obj, val):
        print("setting")
        assert type(val) == bool
        self._trigger = val

class Test(object):
    x = Trigger()