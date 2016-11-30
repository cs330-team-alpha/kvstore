import os
import sys
sys.path.insert(0, os.path.abspath('../predictor'))

import predict
import spot

# Core node information stored in LB
class CoreNode(object):
    nodeCount = 0
    def __init__(self, capacity, bid):
        CoreNode.nodeCount += 1
        self.capacity = capacity # Assume always full
        #self.numEntries = 0
        self.bid = bid
        self.freq = 0

    def updateFreq(self, freq):
        self.freq = freq

# Opportunistic node information stored in LB
class OtherNode(object):
    nodeCount = 0
    def __init__(self, capacity, numEntries, bid)
        OtherNode.nodeCount += 1
        self.capacity = capacity
        self.numEntries = numEntries
        self.bid = bid
        self.freq = 0

    def updateFreq(self, freq):
        self.freq = freq

    def addEntries(self, numAdd):
        self.numEntries += numAdd
        if (self.numEntries > self.capacity):
            print "Error: Exceeding capacity."
            #TODO: Copy over some
            self.numEntries = self.capacity
        else:
            #TODO: Copy over all
        return (self.capacity - self.numEntries)

# Naive bidding strategy: mid
def next_bid_mid(bids, market):
    return (bids[-1] - market) / 2.0

# Distribution of biddings of opportunistic nodes
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
        for i in xrange(numcore):
           data.corenodes.append(CoreNore(capacity, bid))
        return num # return only when all instances have responded
    def launch_new(data, bid):
        # Dummy operation for now
        capacity = 100
        numEntries = 0
        data.othernodes.append(OtherNode(capacity, numEntries, bid))
        return 1 # return 1 when success, otherwise 0
    
    def init(data):
        data.corenodes = [ ]
        data.othernodes = [ ]
        market = spot.get_current_spot_price() #assume it returns price
        bid = predict.pull_prediction(data.duration)
        success = lauch_core(bid, data.numcore)
        if (success < data.numcore):
            print ("Error: Only %d instances intialized. %d requested." % (success, data.numcore))




    class Struct(object): pass
    data = Struct()
    data.numcore = numcore
    data.duration = duration
    init(data)
    # Main function begins

    # Main function ends

#run(3, 3)
