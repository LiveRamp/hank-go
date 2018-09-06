## Overview

The Hank Go Client handles connecting to Hank in a reliable manner - there should be no visible effect if Rings or Servers go down. This is a reimplementation of the [Hank Java client](https://github.com/LiveRamp/hank/tree/master/hank-client/src/main/java/com/liveramp/hank/client) and is comparable featurewise. 

In addition, since Hank uses rendevous hashing, the Client also handles figuring out which servers queries are routed to.

The Go Client has several options that govern retries, timeouts, number of connections and caching. 

1) NumConnectionsPerHost: Number of separate connections to a single host. The greater the number, the greater the number of simultaneous queries per host.
2) MinConnectionsPerPartition: Minimum number of connections to minimum number of live replicas of each record. Guarentees client can serve all queries.
3) TryLockTimeoutMs: Time to wait for a connection to a host before giving up. Connections might not be available if a specific server is experiencing high loads.
4) EstablishConnectionTimeoutMs: Time before a connection attempt fails. Governs how long client takes to start up.
5) EstablishConnectionRetries: Number of times to try to connect to a server. Governs how long client takes to start up.
6) QueryTimeoutMs: Time before a query fails. Governs how client latency.     
7) ResponseCacheNumItems: Size of client cache.       
8) ResponseCacheExpiryTime: Time till cache item is expired.

A quick example of how to use the client can be seen [here](https://github.com/LiveRamp/hank-go/blob/update_readme/main/simple_query_script.go).

## Connecting to Hank

The client connects to Hank as such:
1) Connect to Zookeeper. Retrieve a list of Rings and servers for each ring, including how data is partitioned across servers.
2) Attempt to establish *NumConnectionsPerHost* connections to *all* servers in *all* rings. 
3) Make sure *MinConnectionsPerPartition* connections to partitions are established. Hank data is partitioned across servers. This gives the client the minimum set of servers needed to serve all queries.
4) If all above conditions are fulfilled, succeed. Otherwise fail loudly.

**Note: Various connection options will affect how long the client takes to start up. For example, a high connection time out and/or retry number will significantly extend the time to success/failure.**

## Querying from Hank 

Queries proceed as follows:
1) As part of rendevous hashing, the client hashes the key being queried with. Using this hash, the client determines which Hank server contains the associated data.
2) If caching is enabled, the client first looks to its cache, returning any found value not expired.
3) If no value is found, or caching is not enabled. The client attempts to grab a connection to any server containing the value associated with the key. To minimize costs, the client prefers servers in the same AZ. There can be several queries executing simultaneously and a connection might not be available if there is high load on a particular server. 
4) If no connection is available, the query fails. Otherwise the client attempts to query the selected server, respecting time out settings.

**Note: Various query options will affect query latency. The client is asynchronous and threadsafe - it is okay for multiple go routines to be using the same client at the same time.**
