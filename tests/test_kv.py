# -*- coding: utf-8 -*-

from .context import loadbalancer

import unittest


class LocalKVTestSuite(unittest.TestCase):
    def setUp(self):
        self.memcache = loadbalancer.kv_clients.MemcachedClient(
            'localhost', '8000')

    # def tearDown(self):
    #    self.memcache.delete('1')
    #    del self.memcache

    def test_set_get(self):
        print "Testing Set"
        # self.memcache.delete('1')
        self.memcache.set('1', 'abcd')
        print "Testing Get"
        returnval = self.memcache.get('1')
        self.assertEqual(returnval, 'abcd',
                         msg='Get Return: {0}'.format(returnval))

    def test_query_parsing(self):
        with open('tests/sample_query.txt', 'r') as myfile:
            querystring = myfile.read()
        query = loadbalancer.query.MemcachedQuery(querystring)
        self.assertEqual(query.command, 'add')
        self.assertEqual(query.key, 'usertable-user2071219101098386137')


if __name__ == '__main__':
    unittest.main()
