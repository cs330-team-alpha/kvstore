#!/usr/bin/env python

from twisted.internet import protocol, reactor
# from kv_clients import MemcachedClient
from query import MemcachedQuery
from kv_clients import MemcachedClient

import LB  # load balancer
from timed_threading import RepeatedTimer

LISTEN_PORT = 8000
SERVER_PORT = 11211  # Memcached default port
SERVER_ADDR = "localhost"

NUM_CORE = 10
DURATION = 3
BUDGET = 10.0  # Dollars per hour

INCOMING = '######INCOMING########'
OUTGOING = "#####OUTGOING######"

# Easiest to keep this as a global variable.
# kv_pool = []
# Using Load Balancer to keep track of cluster now

lb = LB.LoadBalancer(NUM_CORE, DURATION, budget=BUDGET)


def printHotKeysThread(lb):
    for node in lb.pool.values():
        print "Node: " + str(node.index) + " Hot Keys: " + str(node.getHotKeys(2))


class ServerProtocol(protocol.Protocol):
    def __init__(self):
        self.buffer = None
        self.client = None

        print "Initializing Proxy Server Protocol"
        self.load_balancer = lb

    def connectionMade(self):
        print "Incoming Connection..."

    # Incoming Query
    def dataReceived(self, data):
        # print INCOMING
        # print data
        query = MemcachedQuery(data)
        if query.key is None:
            # META/Non-key request direct to first node by default
            node_id = 0
        else:
            node_id = self.load_balancer.get_node_id(query.key)

        # returnstring = kv_pool[nodeid].process_memcached_query(query)

        # assuming node_id did not change, i.e. no terminate_opp called
        m_client = self.load_balancer.get_memcached_client(node_id)

        returnstring = m_client.process_memcached_query(query)
        # print OUTGOING
        # print returnstring

        self.transport.write(returnstring)

        # Update node frequencies:
        read, write = [], []
        if query.command in MemcachedClient._STORE_COMMANDS:
            write = [query.key]
        elif query.command in MemcachedClient._GET_COMMANDS:
            read = [query.key]

        node = self.load_balancer.get_node(node_id)
        node.updateFreq(read, write)


# def add_node(address, port):
#    kv_pool.append(MemcachedClient(address, port))


# def remove_node(nodeid):
#    del kv_pool[nodeid]


# def get_node(key):
#    # TODO: Implement RR hash function to select memcache node
#    return 0


def main():
    factory = protocol.ServerFactory()
    factory.protocol = ServerProtocol

    # TESTING single node only.
    # add_node(SERVER_ADDR, SERVER_PORT)

    print "Starting Memcached Proxy..."

    thread_list = []

    thread_list.append(RepeatedTimer(10, printHotKeysThread, lb))

    try:
        reactor.listenTCP(LISTEN_PORT, factory)
        reactor.run()
    finally:
        for thread in thread_list:
            thread.stop()


if __name__ == '__main__':
    main()
