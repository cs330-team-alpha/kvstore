# -*- coding: utf-8 -*-

from .context import loadbalancer

import unittest
import time

class LocalKVTestSuite(unittest.TestCase):
    def setUp(self):
        self.memcache = loadbalancer.kv_clients.MemcachedClient('localhost', '11211')

    def tearDown(self):
        self.memcache.delete('1')
        del self.memcache

    def test_set_get(self):
        print "Testing Set"
        self.memcache.delete('1')
        self.memcache.set('1', 'abcd')
        print "Testing Get"
        returnval = self.memcache.get('1')
        self.assertEqual(returnval, 'abcd', msg='Get Return: {0}'.format(returnval))


if __name__ == '__main__':
    unittest.main()