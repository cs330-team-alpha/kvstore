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

NUM_CORE = 1
DURATION = 3
BUDGET = 100.0  # Dollars per hour

INCOMING = '######INCOMING########'
OUTGOING = "#####OUTGOING######"


MIN_RESCALE_THRESHOLD = 100  # Ops/min

# Easiest to keep this as a global variable.
# kv_pool = []
# Using Load Balancer to keep track of cluster now

lb = LB.LoadBalancer(NUM_CORE, DURATION, budget=BUDGET)


def do_rescale():
    print "Polling Nodes for Rescale"
    max_freq = 0
    hot_node = None
    for i in range(0, lb.numcore):
        if lb.pool[i].freq > max_freq:
            hot_node = i
            max_freq = lb.pool[i].freq

    if max_freq > MIN_RESCALE_THRESHOLD:
        print "Triggering Rescale, we have node " + str(hot_node) + " with " + str(max_freq) + "operations"
        lb.rescale(hot_node)
        print "Resetting Frequencies"
        # Reset Node Frequencies:
        for i in range(0, lb.numcore):
            lb.pool[i].freq = 0
            lb.pool[i].counter.clear()


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
    thread_list.append(RepeatedTimer(30, printHotKeysThread, lb))
    thread_list.append(RepeatedTimer(60, do_rescale))

    try:
        reactor.listenTCP(LISTEN_PORT, factory)
        reactor.run()
    finally:
        for thread in thread_list:
            thread.stop()

if __name__ == '__main__':
    main()
