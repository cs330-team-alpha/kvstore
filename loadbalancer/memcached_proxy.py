#!/usr/bin/env python

from twisted.internet import protocol, reactor
from kv_clients import MemcachedClient
from query import MemcachedQuery

LISTEN_PORT = 8000
SERVER_PORT = 11211  # Memcached default port
SERVER_ADDR = "localhost"

# Easiest to keep this as a global variable.
kv_pool = []


class ServerProtocol(protocol.Protocol):
    def __init__(self):
        self.buffer = None
        self.client = None
        self.kv_pool = []

    def connectionMade(self):
        print "Incoming Connection..."

    # Incoming Query
    def dataReceived(self, data):
        query = MemcachedQuery(data)
        if query.key is None:
            # META/Non-key request direct to first node by default
            nodeid = 0
        else:
            nodeid = get_node(query.key)

        returnstring = kv_pool[nodeid].process_memcached_query(query)
        self.transport.write(returnstring)


def add_node(address, port):
    kv_pool.append(MemcachedClient(address, port))


def remove_node(nodeid):
    del kv_pool[nodeid]


def get_node(key):
    # TODO: Implement RR hash function to select memcache node
    return 0


def main():
    factory = protocol.ServerFactory()
    factory.protocol = ServerProtocol

    # TESTING single node only.
    add_node(SERVER_ADDR, SERVER_PORT)

    print "Starting Memcached Proxy..."
    reactor.listenTCP(LISTEN_PORT, factory)
    reactor.run()


if __name__ == '__main__':
    main()
