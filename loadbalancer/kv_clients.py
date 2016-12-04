#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module consists of the key-value store abstract class
and an implementation for memcached using python memcached

"""

import memcache


class KVStoreClient(object):
    """Abstract KV client class for interacting with a single-node KV store

    Initialiation will take address, port number and any optional arguments

    Dervied classes must implement init, get, set, and delete methods.
    """
    _type = "Generic KV Store"

    # some examples of how to wrap code to conform to 79-columns limit:
    def __init__(self, address, port, **kwargs):
        self.address = address
        self.port = port
        self.KVconnection = None  # Derived classes must perform connection

    def insert(self, key, value):
        return self.set(key, value)

    def get(self, key):
        return self.KVconnection.get(key)

    def set(self, key, value):
        return self.KVconnection.set(key, value)

    def delete(self, key):
        return self.KVconnection.delete(key)

    def __str__(self):
        return type(self)._type + ": " \
            + str(self.address) + ":" + str(self.port)

    def __del__(self):
        return NotImplementedError  # Clean-up


class MemcachedClient(KVStoreClient):
    """Memcached KV client class for interacting with a single-node Memcached
    """
    _type = "Memcached"
    _STORE_COMMANDS = ['add', 'store', 'set', 'replace', 'cas']
    _GET_COMMANDS = ['get', 'gets']
    _META_COMMANDS = ['version']

    def __init__(self, address, port, **kwargs):
        KVStoreClient.__init__(self, address, port)
        self.KVconnection = memcache.Client(
            [address + ':' + str(port)], debug=0)

    def process_memcached_query(self, query):
        ''' expects query.MemacachedQuery type with all fields parsed
        '''
        ### DEBUG
        print 'FORWARDING query to ' + self.address + " : " + str(self.port)
        returnstring = ''
        if not query.exptime:
            query.exptime = 0
        if query.command in MemcachedClient._STORE_COMMANDS:
            result = self.KVconnection.set(
                query.key, query.data, time=0)  # query.exptime)
            if not result:
                returnstring += 'NOT_STORED\r\n'
            else:
                returnstring += 'STORED\r\n'
        elif query.command in MemcachedClient._GET_COMMANDS:
            result = self.KVconnection.get(query.key)  # query.exptime)
            if result:
                returnstring += 'VALUE ' + query.key + \
                    ' 0 ' + str(len(result)) + '\r\n'
                returnstring += str(result) + '\r\n'
            returnstring += 'END\r\n'
        elif query.command in MemcachedClient._META_COMMANDS:
            returnstring += 'VERSION 1.4.25 Ubuntu\r\n'
        # DEBUG print returnstring
        return returnstring

    def __del__(self):
        return self.KVconnection.disconnect_all()  # Clean-up
