import os
import sys
sys.path.insert(0, os.path.abspath('../predictor'))

import collections
import random
import predict
import spot
from kv_clients import MemcachedClient
import binascii
import spot_instance

CAPACITY = 1000 #max number of entries each opp node can store

LOW_THRESHOLD = 20
HIGH_THRESHOLD = 80


class Node(object):
    nodeCount = 0
    def __init__(self, addr, port):
        self.index = Node.nodeCount
        Node.nodeCount += 1
        #self.capacity = capacity
        self.memcache = MemcachedClient(addr, port)
        self.freq = 0  # total workload on the node
        # counter of read/write request for each key
        self.counter = collections.Counter()
        self.hours = 0  # how long has it been active

    def updateFreq(self, reads, writes):
        # reads/writes = list of keys being read/written (can have duplicates)
        self.freq += len(reads) + len(writes)
        r = collections.Counter(reads)
        w = collections.Counter(writes)
        self.counter += r + w

    def getHotKeys(self, num):
        return self.counter.most_common(num)

    def getHotKV(self, num):
        return [(k[0], self.memcache.get(k[0])) for k in self.getHotKeys(num)]

    def getColdKeys(self, num):
        return self.counter.most_common()[:-num - 1:-1]

    def getColdKV(self, num):
        return [(k, self.memcache.get(k)) for k in self.getColdKeys(num)]


# Core node information stored in LB - inherits from Node
# Note: A = CoreNode(...); isinstance(A) == CoreNode
class CoreNode(Node):

    def __init__(self, addr, port):
        super(CoreNode, self).__init__(addr, port)
        # dont need to store bid, b/c all core has same bidcore
        # For each k-v entry in core nodes,
        #  indicate its locations if duplicated, so that
        #  we dont need to search for it in all opp nodes
        self.dupLocations = dict()  # key -> (rr_counter, [dups_in_opp])
        # rr_counter = 0 for core, so dups[rr_counter - 1] for opp nodes

    def dupRemoved(self, oppidx, keys):
        for k in keys:
            if k in self.dupLocations:
                (c, dups) = self.dupLocations[k]
                if oppidx in dups:
                    dups.remove(oppidx)
                    if len(dups) > 0:
                        self.dupLocations[k] = (c, dups)
                    else:
                        del self.dupLocations[k]
            else:
                print "Error: key was not duplicated."

    def dupAdded(self, oppidx, keys):
        for k in keys:
            if k in self.dupLocations:
                (c, dups) = self.dupLocations[k]
                if oppidx not in dups:
                    dups.append(oppidx)
                    self.dupLocations[k] = (c, dups)
            else:
                self.dupLocations[k] = (0, [oppidx])

# Opportunistic node information stored in LB - inherits from Node
# Note: B = OppNode(...); isinstance(B) == OppNode


class OppNode(Node):
    def __init__(self, capacity, addr, port, bid):
        super(OppNode, self).__init__(addr, port)
        self.bid = bid
        self.capacity = capacity
        self.numEntries = 0 # for computing spare space we have in the opp node

    # Assuming new entries will fit this opp node
    # new = [(k1, v1), ...]
    def addEntries(self, new, lb):
        print 'Adding ' + str(len(new)) + ' entries to opportunistic node ' + str(self.index)
        numAdd = len(new)
        self.numEntries += numAdd
        for (k, v) in new:
            self.memcache.insert(k, v)

        # Update the dupLocations in cores
        partitions = dict()
        for (k, _) in new:
            dest_id = lb.cmemcache_hash(k)
            if dest_id in partitions:
                # DEBUG print "Paritiion: " + str(k) + ", " + str(dest_id) + ", " + str(partitions[dest_id])
                partitions[dest_id].append(k)
            else:
                partitions[dest_id] = [k]
        for core_id in partitions:
            lb.pool[core_id].dupAdded(self.index, partitions[core_id])

        return numAdd

    # for write-invalidation
    
    # def invalidateEntries(self, old, lb):
    #     numInv = len(old)
    #     self.numEntries -= numInv
    #     # Update the dupLocations in cores
    #     partitions = dict()
    #     for (k, _) in old:
    #         dest_id = lb.cmemcache_hash(k)
    #         if dest_id in partitions:
    #             partitions[dest_id].append(k)
    #         else:
    #             partitions[dest_id] = [k]
    #    for core_id in partitions:
    #         lb.pool[core_id].dupRemoved(self.index, partitions[core_id])
    #     
    #     # Now really invalidate
    #     for k in old:
    #         self.counter[k] = 0  # set invlaid entry as (dead) cold

    #     return numInv

    # old = [k1, k2, ...]
    def removeEntries(self, old, lb):
        numDel = len(old)
        self.numEntries -= numDel
        # Update the dupLocations in cores
        partitions = dict()
        for (k, _) in old:
            dest_id = lb.cmemcache_hash(k)
            if dest_id in partitions:
                partitions[dest_id].append(k)
            else:
                partitions[dest_id] = [k]
        for core_id in partitions:
            lb.pool[core_id].dupRemoved(self.index, partitions[core_id])

        # Now really delete it
        for k in old:
            self.memcache.delete(k)

        return numDel

    def replaceEntries(self, old, new):
        self.removeEntries(old)
        self.addEntries(new)

# Naive bidding strategy: mid


def next_bid_mid(bids, market):
    return ((bids[-1] - market) / 2.0) + market

def max_nodes_bid(bidcore, market, budget):
    # budget is for opp only
    i = 0
    while True:
        i += 1
        sum_bids = 0
        step = (bidcore - market) / i
        j = 1
        while j <= i:
            sum_bids += bidcore - j * step
            j += 1
        if sum_bids > budget:
            bids = [ ]
            if i == 2:
                bids = [budget]
            elif i > 2:
                step = (bidcore - market) / (i-1)
                # make a list
                j = 1
                while j < i - 1:
                    bids.append(bidcore - j * step)
                    j += 1
            return bids

# Distribution of biddings of core/opportunistic nodes


class Bidding(object):
    # counter = 0
    def __init__(self, bidCore):
        self.bidCore = bidCore
        self.bids = [bidCore]

    def get_lowest_bid(self):
        return self.bids[-1]

    def new(self):
        # get next bidding
        # Bidding.counter += 1
        # Assume spot.get_..._price returns price in float
        new_bid = next_bid_mid(self.bids, spot.get_current_spot_price())
        self.bids.append(new_bid)
        return new_bid


class LoadBalancer(object):
    def launch_all_cores(self):
        # Still dummy operation for now
        print "Launching Core Nodes"
        for i in xrange(self.numcore):
            # SUHAIL LOCAL LAUNCH TESTING

            # Contact spot with self.bidcore and get address and port
            # addr = 'localhost'
            # port = 11211
            # Assume at this point this spot instance has been fulfilled
            # Then store spot info into LB node pool

            # addr, port = spot_instance.launch_local_node(i)

            # END SUHAIL LOCAL LAUNCH TESTING

            # SUHAIL SINGLE NODE LATENCY TEST
            # addr, port = 'ec2-54-84-220-157.compute-1.amazonaws.com', 11211
            # END SUHAIL SINGLE NODE LATENCY TEST

            addr, port = spot_instance.launch_spot_node(self.bidCore)

            new_node = CoreNode(addr, port)
            idx = new_node.index
            self.pool.update({idx: new_node})
        return self.numcore
        # return number of cores launched?

    def launch_opp(self, bid):
        # SUHAIL LOCAL LAUNCH TESTING

        # Still dummy operation for now
        # Contact spot with bid and get address and port
        # addr = 'localhost'
        # port = 11211
        # Assume at this point this spot instance has been fulfilled
        # Then store spot info into LB node pool

        addr, port = spot_instance.launch_local_node(max(self.pool.keys()) + 1)

        # END SUHAIL LOCAL LAUNCH TESTING
        addr, port = spot_instance.launch_spot_node(bid)

        new_node = OppNode(CAPACITY, addr, port, bid)
        idx = new_node.index
        self.pool.update({idx: new_node})
        return idx  # return the index of new opp node

    def terminate_opp(self, index):
        del self.pool[index]
        return index

    def write_invalidate(self, core_id, key):
        node = self.pool[core_id]
        for opp_idx in node.dupLocations:
            # delete the copy in opp
            try:
                self.pool[opp_idx].memcache.delete(key)
            except:
                pass
        # Now clear the duplication list
        if key in node.dupLocations:
            del node.dupLocations[key]

    def cmemcache_hash(self, key):
        # Use memcache-style hash to locate key in core nodes
        return ((((binascii.crc32(key) & 0xffffffff) >> 16) & 0x7fff) or 1) % self.numcore

    def get_node_id(self, key, write=False):
        core_id = self.cmemcache_hash(key)
        if write:
            self.write_invalidate(core_id, key)
        if self.rebalance_lock:
            return core_id
        else:
            # Round-Robin
            if key not in self.pool[core_id].dupLocations:
                return core_id
            else:
                # print "Serving replica..."
                (rr_counter, dups) = self.pool[core_id].dupLocations[key]
                total_copies = 1 + len(dups)  # 1 for core
                rr_counter = (rr_counter + 1) % total_copies
                if rr_counter == 0:
                    return core_id
                else:
                    #print "Serving Replica"
                    return dups[rr_counter - 1]

    def get_memcached_client(self, index):
        return self.pool[index].memcache

    def get_node(self, index):
        return self.pool[index]

    def lb_lock(self):
        while True:
            while (self.rebalance_lock == True):
                continue # while other has the lock
            if (self.rebalance_lock == False):
                self.rebalance_lock = True
                return

    def lb_unlock(self):
        self.rebalance_lock = False

    # Use trigger or allow Proxy initiate re-distribution of duplicates
    def rebalance(self, hot_core_node_id):
        '''Returns the opp node id we are rebalancing, or -1 if no opp node'''
        print "Trying rebalance:"
        # self.rebalance_lock = True
        self.lb_lock() # Acquire the lock
        cold_opp_id = -1
        cold_opps = [ ]
        for idx in xrange(len(self.pool)):
            node = self.pool[idx]
            if (isinstance(node) == OppNode and node.freq < self.low_thr):
                # this node is under-utilized
                cold_opp_id = idx
                cold_opps.append((idx, node))

        if cold_opp_id >= 0:
            # There exists at least one under-utilized node
            (cold_opp_id, cold_opp) = random.choice(cold_opps) # randomly choose one
            hot_core = self.pool[hot_core_node_id]

            num_elem = len(list(hot_core.counter.elements()))
            num_move = num/elem # Note 10% of non-zero elem in hot node
            hotKV = hot_core.getHotKV(num_move)
            hot = hot_core.getHotKeys(num_move)

            # Occupy spare capacity first
            spare = cold_opp.capacity - cold_opp.numEntries
            if (num_move < spare):
                cold_opp.addEntries(hotKV, self)
            else:
                num_replace = num_move - spare
                cold = cold_opp.getColdKeys(num_replace)
                cold_opp.replaceEntries(cold, hotKV[:num_replace], self)
                partitions = dict()
                for k in cold:
                    dest_id = self.cmemcache_hash(k)
                    if dest_id in partitions:
                        partitions[dest_id] = partitions[dest_id].append(k)
                    else:
                        partitions[dest_id] = [k]
                for core_id in partitions:
                    self.pool[core_id].dupRemoved(cold_opp_id, partitions[core_id])
                # Now fill the spare
                cold_opp.addEntries(hotKV[num_replace:], self)

            # Finish rebalancing. Notify core node.
            hot_core.dupAdded(cold_opp_id, hot)
            print "Rebalance successful. Moving %d KV entries from Node %d to Node %d." % (len(cold), hot_core_node_id, cold_opp_id)
          
        # self.rebalance_lock = False
        self.lb_unlock() # release the lock
        return cold_opp_id

    # Use trigger or allow Proxy initiate launch/terminate opp nodes
    def rescale(self, hot_core_node_id):
            ''' Return True on a successful rescale, False if not'''
        # See rebalancing can fix the problem already
        #rebalanced_opp = self.rebalance(self, hot_core_node_id)
        #if rebalanced_opp == -1:
            self.lb_lock() # Acquire the lock
            print "Rebalance failed. Trying rescale:"
            # rebalance was not successful
            hot_core = self.pool[hot_core_node_id]
            num_elem = len(list(hot_core.counter.elements()))
            # Note 10% of non-zero elem
            hotKV = hot_core.getHotKV(num_elem / 10)
            print "Hot Items: " + str([k for k, v in hotKV])
            hot = hot_core.getHotKeys(num_elem / 10)
            last_id = max(self.pool.keys())
            # SUHAIL FIX: Wrong assumption that opp_nodes exist
            if len(self.pool.keys()) > self.numcore:  # Check if we have opprotunistic nodes
                print "We have opportunistic nodes in our cluster"
                last_opp = self.pool[last_id]
                capacity = last_opp.capacity
                numEntries = last_opp.numEntries
                if ((capacity - numEntries) >= len(hot)):
                    last_opp.addEntries(hotKV, self)
                    print "Enough space in the last node: Moving %d KV entries from Node %d to Node %d" % (len(hot), hot_core_node_id, last_id)
                    print "Move part1: Moving %d KV entries from Node %d to Node %d" % ((capacity - numEntries), hot_core_node_id, last_id)
                    last_opp.addEntries(hotKV[:(capacity - numEntries)], self)
                    self.lb_unlock() # release the lock
                    return True
            # No opportunisitic nodes or no nodes with spare capacity
            print "Not enough space in the last node."
            if (len(self.bids) != 0):
                # launch the expensive bid
                new_id = self.launch_opp(self.bids[0])
                del self.bids[0]
                new_opp = self.pool[new_id]
                capacity = new_opp.capacity
                numEntries = new_opp.numEntries
                new_opp.addEntries(hotKV, self)  # TODO: Check condition here
                hot_core.dupAdded(new_opp.index, hot)
                print "Move part2: Moving %d KV entries from Node %d to Node %d" % (len(hot), hot_core_node_id, new_id)
                self.lb_unlock() # release the lock
                return True
            else:
                print "Move part2 failed: cannot launch new nodes."
                self.lb_unlock() # release the lock
                return False
            self.lb_unlock() # release the lock

    def __init__(self, numcore, duration, budget = 0.0):
        self.numcore = numcore
        self.duration = duration
        self.budget = float(budget)
        self.low_thr = LOW_THRESHOLD
        self.high_thr = HIGH_THRESHOLD
        self.rebalance_lock = False

        # Bidding for core node
        self.bidcore = float(predict.pull_prediction(self.duration)) # float
        
        if (self.numcore * self.bidcore > self.budget):
            print "Error: not enough budget."
            sys.exit(-1)

        # available bids (decreasing order)
        price_list = spot.get_current_spot_price()
        prices = [s for (s, a) in price_list]
        market = float(min(prices)) # float

        opp_budget = self.budget - self.numcore * self.bidcore
        self.bids = max_nodes_bid(self.bidcore, market, opp_budget)
 
        self.pool = dict()  # dictionary of nodes
        # Start with a series of core nodes
        num_launched = self.launch_all_cores()
        print "Launched : " + str(num_launched) + " nodes"
