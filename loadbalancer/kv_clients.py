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

    # some examples of how to wrap code to conform to 79-columns limit:
    def __init__(self, address, port, **kwargs):
        KVStoreClient.__init__(self, address, port)
        self.KVconnection = memcache.Client([address + ':' + str(port)], debug=0)

    def __del__(self):
        return self.KVconnection.disconnect_all()  # Clean-up