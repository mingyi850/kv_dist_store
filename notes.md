# Distributed Systems Final Project
## Comparison and Analysis of Weak Consistency in Key Value Stores
## Summary
Our project aims to implement such a weakly consistent datastore using concepts from Dynamo DB (Which also underpin Cassandra and Scylla DB) such as gossip protocols, anti-entropy and vector clocks to exchange information on members and operations.
## Contents
1. Implementation of Key-Value Store
    1. Implementation of Basic Key-Value Store
        - KV Store to be implemented as a cluster of nodes, each serving as a single replica
        - Symmetry - every node in Dynamo has the same set of responsibility as its peers (leaderless(?))
        - Decentralization - Gossip
        - Interface: get(key) and put(key, context, object)
            1. get(key)locates where the object is located and returns a single object if present
                - Returns object with latest vector clock
            2. If conflicts occur, it returns a list of objects with a separate 'context' object
                - If multiple vector clocks are concurrent, it returns a list of all the objects and their vector clocks
            3. put(key, context, object) determines where replicas should be written to based on key and writes replicas to disk.
                - Put should send command to replicate to other nodes once received
                - It then stores data in it's own store
                - Then returns success to client
            4. context encodes system metadata about the object (opaque to caller), includes information such as version.
                - context is stored with object, so system can verify validity of context supplied in put request (?)
                - context contains vector clock information (which denotes it's version)
                - client needs to reconcile data with concurrent vector clocks and send new request to server


        
    2. Gossip Protocol for membership and failure detection
        - 
    3. Anti-entropy mechanism using merkle trees
        - Conflict resolution occurrs on reads

    4. Partitioning (Using consistent hashing)
    - Each key is hashed using MD5 hashing to create 128 bit identifier.
    - Nodes are assigned a random position in a ring
    - Each node has a number of 'tokens' such that is occupies multiple spaces in the ring
    - key hash is mapped to a position in the ring, and we traverse the ring clockwise to find the next node. Key goes into that node.
    - #### We can simplify this by using a single node per server, and using a hash modulo 360 with 360 positions on the ring.

    5. Replication
        - Each pieces of data is replicated at N hosts. 
        - Each key has a main host (coordinator node) based on hash position
        - Coordinator node will replicate keys at N - 1 successor nodes in the ring. (Find the next 2 nodes in the ring and replicate to them)

    6. Versioning
        - 
    7. Sloppy Quorum and hinted Handoff


### Observer
1. Observers put() and get() operations
2. put() -> value, node, context (vector clock), latency (timestamp of sent vs timestamp of response)
3. get() -> value, context (vector clock), latency (timestamp of sent vs timestamp of response)
4. Measuing staleness -> *find some measure of staleness between vector clocks


### Fuzzers:
1. See if we can vary network latency 
    - create a function which sendProbable(mesage, host, delayParams)
2. KV store nodes might just drop messages (by number of messages or by timer)

### Network Conditions 
- Increased latency 
- Server crashes
- Dropped messages 

### Testing and Benchmarking
1. Access patterns 
- multiple test cases 
    - Some will distribute the values of the keys 
        - Increased latency 
        - Server crashes
        - Dropped messages 
    - Some will cluster the values of the keys 
        - Increased latency 
        - Server crashes
        - Dropped messages 
    - Measure staleness and latency in these 2 main cases

### Design doc
    - API from client to server 
    - What the servers will send to observers

### Split 
- Mingyi: Dynamo DB (dist kv store)
- Sheng Siang: Observer and Test cases

    
