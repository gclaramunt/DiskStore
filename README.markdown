Simple Disk Store
=================

A disk based arbitrary byte arrays key value storage
(Yakvs = yet another key value storage )
Is optimized for fast writes (and as it's append only, it might work pretty well with SSDs).

Currently is just a toy implementation.

Roadmap
-------

Add buffered writes
Moar functional handling of resources
Use NIO
Better recovery
spread buckets into directories (will get over the max file name size limit)
Faster byte to hex conversion
Possibly add a bloom filter to the value store, to prevent full file scan if the value is not there
Benchmark with SSD



