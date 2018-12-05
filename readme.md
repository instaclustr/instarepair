# Introduction

Repairs a Cassandra cluster using read repairs. Supports the same options as
nodetool repair where possible.

# Usage

ic-repair <options> [<keyspace> <tables>...]

Option                             | Description
------                             | -----------
-u,--username <username>           | Cassandra username to connect with
-pw,--password <password>          | Cassandra password to connect with
-h,--host <host>                   | Host to connect to. Defaults to localhost
-p,--port <port>                   | Port to connect to. Defaults to 9042
-ssl                               | Enable connecting with SSL. Uses JSSE.
-f,--file <filename>               | File to load/save repair state.
-fresh                             | Forces a fresh repair (ie. no resuming of previous repair)
-report                            | Print report of repair state and exit.
-nosys,--exclude-system            | Exclude repair of system keyspaces
-s,--steps <steps>                 | Steps per token range. Defaults to 1.
-pr,--partitioner-range            | Perform partitioner range repair.
-t,--threads threads>              | Maximum number parallel repairs. Defaults to number of available processors.
-r,--retry <max\_retry>            | Maximum number of retries when there are unavailable nodes.
-d,--retry-delay <delay\_ms>       | Base delay between retries when nodes are unavailable.

# SSL

The -ssl flag enables connecting with SSL using JSSE. You can config SSL settings
via the [JSSE system properties](http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#Customization).

Example of connecting with SSL:

```
ic-repair -Djavax.net.ssl.trustStore=/path/to/client.truststore -Djavax.net.ssl.trustStorePassword=password123 -ssl
```

# Throttling

Read repairs can be quite intensive on the cluster therefore you will want
to adjust the maximum number of parallel read repairs with the -t flag. The
optimal setting may vary between tables since the data model and compaction
strategy have a large impact on the performance of read repairs. Therefore you
may want to run repair for each table separately with different maximum
request parameters.

# Unavailable nodes

If a node becomes unavailable the repair application will wait up to max\_retry
times for the node to become available. It will wait delay\_ms milliseconds and
increase this exponentially on each subsequent retry. Once the maximum number
of retry attempts is reached the repair will be suspended.

# Background

The problem with standard repairs occur when there is large amounts of
inconsistency as any differences in the merkle tree requries streaming replicas
from all nodes involved which can lead to:
* Running out of disk space due to sending multiple replicas
* Lots of sstables from streaming sstable sections for the inconsistent
  token range
* Compactions falling behind from all the sstables being streamed
* High read latency from an increase in sstables per read
* High number of sstables causes high CPU usage in sorting them into buckets

In our experience we have seen repairs lead to cluster outages. The aim of this
application is to avoid these issues by relying on read repairs which in
comparison just send a mutation with the correct version of the row to nodes
without it. Additionally this application supports suspending and resuming the
repair. It can also handle nodes going down. This makes it more robust then
even tools such as Cassandra reaper.

Please see https://www.instaclustr.com/support/documentation/announcements/instaclustr-open-source-project-status/ for Instaclustr support status of this project.
