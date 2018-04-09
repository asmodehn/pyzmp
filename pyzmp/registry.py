from __future__ import absolute_import, division, print_function

import contextlib
from io import open

import os
import tempfile
import abc

import collections
import yaml
import errno


# TODO : namedtuples ? CRDT ?
class FileBasedRegistry(collections.MutableMapping):
    """
    Implements a Registry as a set of files, each one containing only one attribute.
    """

    def __init__(self, value_desc, representer=None, constructor=None):
        """
        Initialize the registry
        :param value_desc: The description of the value stored in this registry
        :param representer: The YAML representer 
        :param constructor: The YAML constructor
        """
        self.desc = value_desc
        self.representer = representer
        self.constructor = constructor

    @staticmethod
    def _get_registry_path():
        """
        A deterministic way to find the path to a registry, so it can be used in any context.
        :return: 
        """
        _zmp_froot = os.path.join(tempfile.gettempdir(), 'zmp')
        return _zmp_froot

    def _name2filepath(self, name):
        # trying to follow the de-facto standard way to register daemon process info (as "name.pid" file for example)
        fname = os.path.join(FileBasedRegistry._get_registry_path(), name + os.extsep + self.desc)
        return fname

    def _filepath2name(self):
        for f in os.listdir(FileBasedRegistry._get_registry_path()):
            if f.endswith(os.extsep + self.desc):
                yield os.path.basename(f)[:-len(os.extsep + self.desc)]

    def __setitem__(self, key, value):
        attrfname = self._name2filepath(key)
        try:
            with open(attrfname, "w") as fh:
                # Note : we use yaml as a codec
                yaml.dump(value, fh, default_flow_style=False)
        except IOError as ioe:
            if ioe.errno == errno.ENOENT:  # No such file or directory
                # TODO : handle all possible cases
                os.makedirs(os.path.dirname(attrfname))
                # now we can try again...
                with open(attrfname, "w") as fh:
                    yaml.dump(value, fh, default_flow_style=False)

    def __delitem__(self, key):
        pidfname = self._name2filepath(key)
        os.remove(pidfname)

    def __getitem__(self, item):
        fname = self._name2filepath(item)
        try:
            with open(fname, "r") as fh:
                attr = yaml.load(fh)
            return attr
        except IOError as ioe:
            if ioe.errno == errno.ENOENT:
                raise KeyError

    def __iter__(self):
        for name in self._filepath2name():
            yield name

    def __len__(self):
        return len([a for a in self._filepath2name()])

    def __str__(self):
        return str({n: getattr(self, n) for n in self})

    def __repr__(self):
        return str({n: getattr(self, n) for n in self})

    @contextlib.contextmanager
    def registered(self, name, value):
        # advertise itself
        self[name] = value

        # Do not yield until we are register (otherwise noone can find us, there is no point.)
        yield

        # concealing itself
        # Note this will not be done if the process is killed or crash...
        self.pop(name)

