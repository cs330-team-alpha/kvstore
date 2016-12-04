import os
import sys
sys.path.insert(0, os.path.abspath('../predictor'))

import collections
import predict
import spot
from kv_clients import MemcachedClient
import binascii
import spot_instance

CAPACITY = 100  # max number of entries each node can store
LOW_THRESHOLD = 20
HIGH_THRESHOLD = 80


class Node(object):
    nodeCount = 0

    def __init__(self, capacity, addr, port):
        self.index = Node.nodeCount
        Node.nodeCount += 1
        self.capacity = capacity
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
        return [(k, self.memcache.get(k)) for k in self.getHotKeys(num)]

    def getColdKeys(self, num):
        return self.counter.most_common()[:-num - 1:-1]

    def getColdKV(self, num):
        return [(k, self.memcache.get(k)) for k in self.getColdKeys(num)]


# Core node information stored in LB - inherits from Node
# Note: A = CoreNode(...); isinstance(A) == CoreNode
class CoreNode(Node):
    def __init__(self, capacity, addr, port):
        super(CoreNode, self).__init__(capacity, addr, port)
        # dont need to store bid, b/c all core has same bidcore
        # TODO: for each k-v entry in core nodes,
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
        super().__init__(capacity, addr, port)
        self.bid = bid
        self.numEntries = 0  # for computing spare space we have in the opp node

    # Assuming new entries will fit this opp node
    # new = [(k1, v1), ...]
    def addEntries(self, new):
        numAdd = len(new)
        self.numEntries += numAdd
        for (k, v) in new:
            self.memcache.insert(k, v)

        # Update the dupLocations in cores
        partitions = dict()
        for (k, _) in new:
            dest_id = self.cmemcache_hash(k)
            if dest_id in partitions:
                partitions[dest_id] = partitions[dest_id].append(k)
            else:
                partitions[dest_id] = [k]
        for core_id in partitions:
            self.pool[core_id].dupAdded(self.index, partitions[core_id])

        return numAdd

    # for write-invalidation
    def invalidateEntries(self, old):
        numInv = len(old)
        self.numEntries -= numInv
        for k in old:
            self.counter[k] = 0  # set invlaid entry as (dead) cold

        # Update the dupLocations in cores
        partitions = dict()
        for (k, _) in old:
            dest_id = self.cmemcache_hash(k)
            if dest_id in partitions:
                partitions[dest_id] = partitions[dest_id].append(k)
            else:
                partitions[dest_id] = [k]
        for core_id in partitions:
            self.pool[core_id].dupRemoved(self.index, partitions[core_id])

        return numInv

    # old = [k1, k2, ...]
    def removeEntries(self, old):
        numDel = len(old)
        self.numEntries -= numDel
        for k in old:
            self.memcache.delete(k)

        # Update the dupLocations in cores
        partitions = dict()
        for (k, _) in old:
            dest_id = self.cmemcache_hash(k)
            if dest_id in partitions:
                partitions[dest_id] = partitions[dest_id].append(k)
            else:
                partitions[dest_id] = [k]
        for core_id in partitions:
            self.pool[core_id].dupRemoved(self.index, partitions[core_id])

        return numDel

    def replaceEntries(self, old, new):
        self.removeEntries(old)
        self.addEntries(new)

# Naive bidding strategy: mid


def next_bid_mid(bids, market):
    return ((bids[-1] - market) / 2.0) + market

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

    def del_low(self):
        old_bid = self.bids.pop()
        return old_bid

    def del_any(self, index):
        # TODO: Check valid
        old_bid = self.bids.pop(index)
        return old_bid


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

            addr, port = spot_instance.launch_local_node(i)

            # END SUHAIL LOCAL LAUNCH TESTING

            new_node = CoreNode(CAPACITY, addr, port)
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

        new_node = OppNode(CAPACITY, addr, port, bid)
        idx = new_node.index
        self.pool.update({idx: new_node})
        return idx  # return the index of new opp node

    def terminate_opp(self, index):
        del self.pool[index]
        return index

    def cmemcache_hash(self, key):
        # Use memcache-style hash to locate key in core nodes
        return ((((binascii.crc32(key) & 0xffffffff) >> 16) & 0x7fff) or 1) % self.numcore

    def get_node_id(self, key):
        core_id = self.cmemcache_hash(key)
        if self.rebalance_lock:
            return core_id
        else:
            # Round-Robin
            if key not in self.pool[core_id].dupLocations:
                return core_id
            else:
                (rr_counter, dups) = self.pool[core_id].dupLocations[key]
                total_copies = 1 + len(dups)  # 1 for core
                rr_counter = (rr_counter + 1) % total_copies
                if rr_counter == 0:
                    return core_id
                else:
                    return dups[rr_counter - 1]

    def get_memcached_client(self, index):
        return self.pool[index].memcache

    # Use trigger ot allow Proxy initiate re-distribution of duplicates
    # Returns the opp node id we are rebalancing, or -1 if no opp node
    def rebalance(self, hot_core_node_id):
        self.rebalance_lock = True
        cold_opp_id = -1
        for idx in xrange(len(self.pool)):
            node = self.pool[idx]
            if (isinstance(node) == OppNode and node.freq < self.low_thr):
                # this node is under-utilized
                cold_opp = node
                cold_opp_id = idx

        if cold_opp_id >= 0:
            hot_core = self.pool[hot_core_node_id]

            num_elem = len(hot_core.counter.elements())
            # Note 10% of non-zero elem
            hotKV = hot_core.getHotKV(num_elem / 10)
            # Note 10% of non-zero elem
            hot = hot_core.getHotKeys(num_elem / 10)
            cold = cold_opp.getColdKeys(num_elem / 10)

            cold_opp.replaceEntries(cold, hotKV)
            partitions = dict()
            for k in cold:
                dest_id = self.cmemcache_hash(k)
                if dest_id in partitions:
                    partitions[dest_id] = partitions[dest_id].append(k)
                else:
                    partitions[dest_id] = [k]
            for core_id in partitions:
                self.pool[core_id].dupRemoved(cold_opp_id, partitions[core_id])

            hot_core.dupAdded(cold_opp_id, hot)
        self.rebalance_lock = False
        return cold_opp_id

    # Use trigger or allow Proxy initiate launch/terminate opp nodes
    def rescale(self, hot_core_node_id):
        # See rebalancing can fix the problem already
        rebalanced_opp = self.rebalance(self, hot_core_node_id)
        if rebalanced_opp == -1:
            # rebalance was not successful
            hot_core = self.pool[hot_core_node_id]
            num_elem = len(hot_core.counter.elements())
            # Note 10% of non-zero elem
            hotKV = hot_core.getHotKV(num_elem / 10)
            hot = hot_core.getHotKeys(num_elem / 10)
            last_id = max(self.pool.keys())
            last_opp = self.pool[last_id]
            capacity = last_opp.capacity
            numEntries = last_opp.numEntries
            if ((capacity - numEntries) >= len(hot)):
                last_opp.addEntries(hotKV)
            else:
                last_opp.addEntries(hotKV[:(capacity - numEntries)])
                if (len(self.bids) != 0):
                    # launch the expensive bid
                    new_id = self.launch_opp(self.bids[0])
                    del self.bids[0]
                    new_opp = self.pool[new_id]
                    new_opp.addEntries(hotKV[(capacity - numEntries):-1])
                    hot_core.dupAdded(new_opp.index, hot)
                else:
                    print "Error: cannot launch new nodes."

    def __init__(self, numcore, duration):
        # data.corenodes = [ ] # change to dict? map index to node
        # data.oppnodes = [ ]
        self.numcore = numcore
        self.duration = duration
        self.low_thr = LOW_THRESHOLD
        self.high_thr = HIGH_THRESHOLD
        self.rebalance_lock = False

        # Bidding for core node
        self.bidcore = predict.pull_prediction(self.duration)

        # TODO: create bids
        self.bids = []  # available bids (decreasing order)

        self.pool = dict()  # dictionary of nodes
        # Start with a series of core nodes
        num_launched = self.launch_all_cores()
        print "Launched : " + str(num_launched) + " nodes"
