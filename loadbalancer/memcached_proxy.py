#!/usr/bin/env python

from twisted.internet import protocol, reactor
from kv_clients import MemcachedClient
from query import MemcachedQuery

LISTEN_PORT = 8000
SERVER_PORT = 11211  # Memcached default port
SERVER_ADDR = "localhost"


class ServerProtocol(protocol.Protocol):
    def __init__(self):
        self.buffer = None
        self.client = None

    def connectionMade(self):
        print "Incoming Connection..."
        self.memcache = MemcachedClient(SERVER_ADDR, SERVER_PORT)

    # Incoming Query
    def dataReceived(self, data):
        returnstring = self.memcache.process_memcached_query(
            MemcachedQuery(data))
        self.transport.write(returnstring)


def main():
    factory = protocol.ServerFactory()
    factory.protocol = ServerProtocol

    print "Starting Memcached Proxy..."
    reactor.listenTCP(LISTEN_PORT, factory)
    reactor.run()


if __name__ == '__main__':
    main()
