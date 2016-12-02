import os
import sys
sys.path.insert(0, os.path.abspath('../predictor'))

import collections
import predict
import spot

class Node(object):
    nodeCount = 0
    def __init__(self, capacity, bid):
        self.index = Node.nodeCount
        Node.nodeCount += 1
        self.capacity = capacity
        self.bid = bid 
        self.freq = 0 # total workload on the node
        self.counter = collections.Counter() # counter of read/write request for each key
        self.hours = 0 # how long has it been active

    def updateFreq(self, reads, writes):
        # reads/writes = list of keys being read/written (can have duplicates)
        self.freq += len(reads) + len(writes)
        r = collections.Counter(reads)
        w = collections.Counter(writes)
        self.counter += r + w

# Core node information stored in LB - inherit from Node
# A = CoreNode(c,b); isinstance(A) == CoreNode
class CoreNode(Node):
    def __init__(self, capacity, bid):
        super().__init__(capacity, bid)
    
    def getTopKeys(self, num):
        return self.counter.most_common(num)

# Opportunistic node information stored in LB - inherit from Node
class OppNode(Node):
    def __init__(self, capacity, bid, numEntries):
        super().__init__(capacity, bid)
        self.numEntries = numEntries
    
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
        new_bid = next_bid(self.bids, spot.get_current_spot_price())
        self.bids.append(new_bid)
        return new_bid
    
    def del_low(self):
        old_bid = self.bids.pop()
        return old_bid

    def del_any(self, index):
        #TODO: Check valid
        old_bid = self.bids.pop(index)
        return old_bid


def run(numcore=1, duration=3):
    def launch_core(data, bid, num):
        # Dummy operation for now
        capacity = 100
        for i in xrange(num):
           data.corenodes.append(CoreNode(capacity, bid))
        return num # return only when all instances have responded
    def launch_new(data, bid):
        # Dummy operation for now
        capacity = 100
        numEntries = 0
        data.oppnodes.append(OppNode(capacity, bid, numEntries))
        return 1 # return 1 when success, otherwise 0
    
    def init(data):
        data.corenodes = [ ] # change to dict? map index to node
        data.oppnodes = [ ]
        market = spot.get_current_spot_price() #assume it returns price
        bid = predict.pull_prediction(data.duration)
        success = launch_core(data, bid, data.numcore)
        if (success < data.numcore):
            print ("Error: Only %d instances intialized. %d requested." % (success, data.numcore))

    class Struct(object): pass
    data = Struct()
    data.numcore = numcore
    data.duration = duration
    init(data)
    # Main function begins
    print "Init success!"
    launch_core(data, 2.0, 3)
    print "Core success!"
    launch_new(data, 1.9)
    print "Opp success!"
    # Main function ends

#run(3, 3)
