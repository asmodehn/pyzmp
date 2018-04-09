from __future__ import absolute_import, division, print_function

import pytest

from pyzmp.registry import FileBasedRegistry

# Testing operation combinations that make sense


class TestFileBasedRegistry(object):

    def setup_method(self, method):
        self.attr_reg = FileBasedRegistry("myattr")

    def teardown_method(self, method):
        # cleaning up what might exists
        for a in self.attr_reg:
            self.attr_reg.pop(a)

    def test_store_erase(self):

        with pytest.raises(KeyError) as e_info:
            self.attr_reg["myid"]

        self.attr_reg["myid"] = 42

        assert self.attr_reg["myid"] == 42

        self.attr_reg.pop("myid")

        with pytest.raises(KeyError) as e_info:
            self.attr_reg["myid"]



if __name__ == '__main__':
    pytest.main(['-s', '-x', __file__])
