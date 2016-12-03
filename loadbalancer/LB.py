import os
import sys
sys.path.insert(0, os.path.abspath('../predictor'))

import collections
import predict
import spot
from kv_clients import MemcachedClient

CAPACITY = 100 #max number of entries each node can store

class Node(object):
    nodeCount = 0
    def __init__(self, capacity, addr, port):
        self.index = Node.nodeCount
        Node.nodeCount += 1
        self.capacity = capacity
        self.memcache = MemcachedClient(addr, port)
        self.freq = 0 # total workload on the node
        self.counter = collections.Counter() # counter of read/write request for each key
        self.hours = 0 # how long has it been active

    def updateFreq(self, reads, writes):
        # reads/writes = list of keys being read/written (can have duplicates)
        self.freq += len(reads) + len(writes)
        r = collections.Counter(reads)
        w = collections.Counter(writes)
        self.counter += r + w

# Core node information stored in LB - inherits from Node
# Note: A = CoreNode(...); isinstance(A) == CoreNode
class CoreNode(Node):
    def __init__(self, capacity, addr, port):
        super().__init__(capacity, addr, port)
        # dont need to store bid, b/c all core has same bidcore
        # TODO: for each k-v entry in core nodes,
        #  indicate its locations if duplicated, so that
        #  we dont need to search for it in all opp nodes

    def getTopKeys(self, num):
        return self.counter.most_common(num)

# Opportunistic node information stored in LB - inherits from Node
# Note: B = OppNode(...); isinstance(B) == OppNode
class OppNode(Node):
    def __init__(self, capacity, addr, port, bid):
        super().__init__(capacity, addr, port)
        self.bid = bid
        self.numEntries = 0

    def addEntries(self, numAdd):
        self.numEntries += numAdd
        if (self.numEntries > self.capacity):
            print "Error: Exceeding capacity."
            #TODO: Copy over some
            self.numEntries = self.capacity
        else:
            #TODO: Copy over all
            self.numEntries = self.numEntries
        return (self.capacity - self.numEntries)

# Naive bidding strategy: mid
def next_bid_mid(bids, market):
    return (bids[-1] - market) / 2.0

# Distribution of biddings of core/opportunistic nodes
class Bidding(object):
    #counter = 0
    def __init__(self, bidCore):
        self.bidCore = bidCore
        self.bids = [bidCore]

    def get_lowest_bid(self):
        return self.bids[-1]

    def new(self):
        # get next bidding
        #Bidding.counter += 1
        # Assume spot.get_..._price returns price in float
        new_bid = next_bid_mid(self.bids, spot.get_current_spot_price())
        self.bids.append(new_bid)
        return new_bid
    
    def del_low(self):
        old_bid = self.bids.pop()
        return old_bid

    def del_any(self, index):
        #TODO: Check valid
        old_bid = self.bids.pop(index)
        return old_bid


class LoadBalancer(object):
    def launch_all_cores(self):
        # Still dummy operation for now
        for i in xrange(self.numcore):
            # Contact spot with self.bidcore and get address and port
            addr = 'localhost'
            port = 11211
            # Assume at this point this spot instance has been fulfilled
            # Then store spot info into LB node pool
            self.pool.append(CoreNode(CAPACITY, addr, port))
        return self.numcore
        #return number of cores launched?
    
    def launch_opp(self, bid):
        # Still dummy operation for now
        # Contact spot with bid and get address and port
        addr = 'localhost'
        port = 11211
        # Assume at this point this spot instance has been fulfilled
        # Then store spot info into LB node pool
        self.pool.append(OppNode(CAPACITY, addr, port, bid))
        return 1 # return 1 when success, otherwise 0
    
    def terminate_opp(self, index):
        del self.pool[index]
        return index
    
    def cmemcache_hash(self, key):
        # Use memcache-style hash to locate key in core nodes
        return ((((binascii.crc32(key) & 0xffffffff) >> 16) & 0x7fff) or 1)

    def get_node_id(self, key):
        #TODO:If duplicated in opp, use Round-Robin hash to select memcache node
        return self.cmemcache_hash(key)

    def get_memcached_client(self, index):
        return self.pool[index].memcache
        
    # Use trigger ot allow Proxy initiate re-distribution of duplicates
    def rebalance(self):
        return 42

    # Use trigger or allow Proxy initiate launch/terminate opp nodes
    def rescale(self):
        return 42


    def __init__(self, numcore, duration):
        #data.corenodes = [ ] # change to dict? map index to node
        #data.oppnodes = [ ]
        self.numcore = numcore
        self.duration = duration

        # Bidding for core node
        self.bidcore = predict.pull_prediction(self.duration)
        
        self.pool = [ ] # list of nodes
        # Start with a series of core nodes
        num_launched = self.launch_all_cores()
        
       
